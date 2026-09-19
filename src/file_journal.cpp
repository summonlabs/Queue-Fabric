// Queue Fabric - vendor-neutral queue lifecycle and occupancy governance runtime.
// Copyright 2026 Summon Software Labs.
// SPDX-License-Identifier: Apache-2.0
// The journal uses the C stdio surface deliberately: it is the only portable
// way to guarantee flush plus commit semantics on a file descriptor.
#ifdef _MSC_VER
#define _CRT_SECURE_NO_WARNINGS
#endif
#include <cstdio>
#include <filesystem>
#include <string>
#include <vector>

#include "qf/bytes.hpp"
#include "qf/hash.hpp"
#include "qf/journal.hpp"
#include "qf/version.hpp"

#ifdef _WIN32
#include <io.h>
#else
#include <unistd.h>
#endif

namespace qf {

namespace {

constexpr std::uint32_t kJournalMagic = 0x314A4651u;  // "QFJ1"
constexpr std::size_t kHeaderBytes = 40;
constexpr std::size_t kRecordPrefixBytes = 10;

bool sync_stream(std::FILE* file) noexcept {
  if (std::fflush(file) != 0) {
    return false;
  }
#ifdef _WIN32
  return _commit(_fileno(file)) == 0;
#else
  return ::fsync(::fileno(file)) == 0;
#endif
}

void put_u16_at(std::uint8_t* out, std::uint16_t value) noexcept {
  out[0] = static_cast<std::uint8_t>(value & 0xFFu);
  out[1] = static_cast<std::uint8_t>((value >> 8) & 0xFFu);
}

void put_u32_at(std::uint8_t* out, std::uint32_t value) noexcept {
  for (int i = 0; i < 4; ++i) {
    out[i] = static_cast<std::uint8_t>((value >> (8 * i)) & 0xFFu);
  }
}

void put_u64_at(std::uint8_t* out, std::uint64_t value) noexcept {
  for (int i = 0; i < 8; ++i) {
    out[i] = static_cast<std::uint8_t>((value >> (8 * i)) & 0xFFu);
  }
}

std::uint16_t get_u16_at(const std::uint8_t* in) noexcept {
  return static_cast<std::uint16_t>(static_cast<std::uint16_t>(in[0]) | (static_cast<std::uint16_t>(in[1]) << 8));
}

std::uint32_t get_u32_at(const std::uint8_t* in) noexcept {
  std::uint32_t value = 0;
  for (int i = 0; i < 4; ++i) {
    value |= static_cast<std::uint32_t>(in[i]) << (8 * i);
  }
  return value;
}

std::uint64_t get_u64_at(const std::uint8_t* in) noexcept {
  std::uint64_t value = 0;
  for (int i = 0; i < 8; ++i) {
    value |= static_cast<std::uint64_t>(in[i]) << (8 * i);
  }
  return value;
}

void encode_header(std::uint8_t* out, const BootId& boot, Epoch epoch) noexcept {
  put_u32_at(out, kJournalMagic);
  put_u16_at(out + 4, kDurableFormatVersion);
  put_u16_at(out + 6, 0);
  put_u64_at(out + 8, boot.hi);
  put_u64_at(out + 16, boot.lo);
  put_u64_at(out + 24, epoch.value());
  put_u32_at(out + 32, crc32c(out, 32));
  put_u32_at(out + 36, 0);
}

void encode_record(std::uint8_t* out, JournalRecordType type, std::span<const std::byte> payload) noexcept {
  put_u16_at(out, static_cast<std::uint16_t>(type));
  put_u32_at(out + 2, static_cast<std::uint32_t>(payload.size()));
  std::uint32_t crc = crc32c(out, 6);
  crc = crc32c_extend(crc, payload.data(), payload.size());
  put_u32_at(out + 6, crc);
}

}  // namespace

struct FileJournal::Impl {
  JournalConfig config{};
  std::FILE* file{nullptr};
  std::vector<JournalRecord> records{};
  RecoveryReport report{};
  BootId header_boot{};
  Epoch header_epoch{};
  std::uint64_t bytes_written{0};
  std::uint64_t valid_prefix_bytes{0};
  bool open{false};
};

FileJournal::FileJournal(JournalConfig config) : impl_(new Impl()) { impl_->config = std::move(config); }

FileJournal::~FileJournal() {
  close();
  delete impl_;
}

std::span<const JournalRecord> FileJournal::records() const noexcept { return impl_->records; }

const RecoveryReport& FileJournal::recovery() const noexcept { return impl_->report; }

std::string FileJournal::describe() const {
  std::string out = "file journal at ";
  out += impl_->config.path;
  out += " records=";
  out += std::to_string(impl_->records.size());
  out += " records_total=";
  out += std::to_string(impl_->report.records_total);
  return out;
}

Status FileJournal::open() {
  impl_->records.clear();
  impl_->report = RecoveryReport{};
  impl_->bytes_written = 0;
  impl_->valid_prefix_bytes = 0;
  impl_->open = false;

  if (impl_->config.path.empty()) {
    return Status{Code::invalid_argument, "journal path is empty"};
  }
  if (impl_->config.max_record_bytes > limits::kMaxJournalRecordBytes) {
    return Status{Code::out_of_range, "journal record bound exceeds the runtime maximum"};
  }

  std::error_code error;
  const bool exists = std::filesystem::exists(impl_->config.path, error);
  if (!exists) {
    const auto parent = std::filesystem::path(impl_->config.path).parent_path();
    if (!parent.empty()) {
      std::filesystem::create_directories(parent, error);
    }
  }

  impl_->file = std::fopen(impl_->config.path.c_str(), "rb+");
  if (impl_->file == nullptr) {
    impl_->file = std::fopen(impl_->config.path.c_str(), "wb+");
  }
  if (impl_->file == nullptr) {
    return Status{Code::internal, "cannot open journal file"};
  }
  impl_->open = true;
  impl_->report.journal_present = true;

  if (std::fseek(impl_->file, 0, SEEK_END) != 0) {
    return Status{Code::internal, "cannot seek journal file"};
  }
  const long size_raw = std::ftell(impl_->file);
  if (size_raw < 0) {
    return Status{Code::internal, "cannot size journal file"};
  }
  const std::uint64_t size = static_cast<std::uint64_t>(size_raw);
  if (size > impl_->config.max_file_bytes) {
    impl_->report.status = Status{Code::integrity_failure, "journal exceeds the configured maximum size"};
    impl_->report.note("journal file exceeds configured maximum; refusing to load");
    return impl_->report.status;
  }

  // Fresh file: write an empty header so the file is always self-describing.
  if (size == 0) {
    std::uint8_t header[kHeaderBytes];
    encode_header(header, BootId{}, Epoch{});
    if (std::fwrite(header, 1, kHeaderBytes, impl_->file) != kHeaderBytes) {
      return Status{Code::internal, "cannot write journal header"};
    }
    if (!sync_stream(impl_->file)) {
      return Status{Code::not_durable, "cannot flush journal header"};
    }
    impl_->bytes_written = kHeaderBytes;
    impl_->valid_prefix_bytes = kHeaderBytes;
    impl_->report.header_valid = true;
    impl_->report.format_version = kDurableFormatVersion;
    impl_->report.records_usable = 0;
    return Status{};
  }
  if (size < kHeaderBytes) {
    impl_->report.status = Status{Code::integrity_failure, "journal header is truncated"};
    impl_->report.note("journal shorter than a header; refusing to load");
    return impl_->report.status;
  }

  std::vector<std::uint8_t> raw(static_cast<std::size_t>(size));
  if (std::fseek(impl_->file, 0, SEEK_SET) != 0 || std::fread(raw.data(), 1, raw.size(), impl_->file) != raw.size()) {
    return Status{Code::internal, "cannot read journal file"};
  }

  if (get_u32_at(raw.data()) != kJournalMagic) {
    impl_->report.status = Status{Code::integrity_failure, "journal magic mismatch"};
    impl_->report.note("journal magic mismatch; refusing to load");
    return impl_->report.status;
  }
  const std::uint16_t version = get_u16_at(raw.data() + 4);
  impl_->report.format_version = version;
  if (version != kDurableFormatVersion) {
    impl_->report.status = Status{Code::unsupported, "unsupported durable format version"};
    impl_->report.note("durable format version is not supported by this build");
    return impl_->report.status;
  }
  if (get_u32_at(raw.data() + 32) != crc32c(raw.data(), 32)) {
    impl_->report.status = Status{Code::integrity_failure, "journal header checksum mismatch"};
    impl_->report.note("journal header checksum mismatch; refusing to load");
    return impl_->report.status;
  }
  impl_->header_boot = BootId{get_u64_at(raw.data() + 8), get_u64_at(raw.data() + 16)};
  impl_->header_epoch = Epoch::from_value(get_u64_at(raw.data() + 24));
  impl_->report.header_valid = true;
  impl_->report.previous_boot_known = impl_->header_boot.valid();
  impl_->report.previous_boot = impl_->header_boot;
  impl_->report.previous_epoch = impl_->header_epoch;

  std::size_t cursor = kHeaderBytes;
  std::size_t last_good = kHeaderBytes;
  bool stopped = false;
  while (cursor < raw.size()) {
    ++impl_->report.records_total;
    if (raw.size() - cursor < kRecordPrefixBytes) {
      impl_->report.records_truncated += 1;
      impl_->report.bytes_truncated += raw.size() - cursor;
      stopped = true;
      break;
    }
    const auto type_raw = get_u16_at(raw.data() + cursor);
    const std::uint32_t payload_len = get_u32_at(raw.data() + cursor + 2);
    const std::uint32_t stored_crc = get_u32_at(raw.data() + cursor + 6);
    if (payload_len > impl_->config.max_record_bytes || raw.size() - cursor < kRecordPrefixBytes + payload_len) {
      impl_->report.records_truncated += 1;
      impl_->report.bytes_truncated += raw.size() - cursor;
      stopped = true;
      break;
    }
    const std::uint32_t computed = crc32c_extend(crc32c(raw.data() + cursor, 6), raw.data() + cursor + kRecordPrefixBytes, payload_len);
    if (computed != stored_crc) {
      // Distinguish a torn tail from mid-file corruption: if a well-formed
      // record follows this position, the damage is inside the file.
      const std::size_t next = cursor + kRecordPrefixBytes + payload_len;
      bool looks_like_more_data = false;
      if (raw.size() - next >= kRecordPrefixBytes) {
        const std::uint32_t next_len = get_u32_at(raw.data() + next + 2);
        if (next_len <= impl_->config.max_record_bytes && raw.size() - next >= kRecordPrefixBytes + next_len) {
          const std::uint32_t next_crc = get_u32_at(raw.data() + next + 6);
          const std::uint32_t next_computed =
              crc32c_extend(crc32c(raw.data() + next, 6), raw.data() + next + kRecordPrefixBytes, next_len);
          looks_like_more_data = next_computed == next_crc;
        }
      }
      if (looks_like_more_data) {
        impl_->report.records_corrupt += 1;
        impl_->report.status = Status{Code::integrity_failure, "corrupt record followed by further valid records"};
        impl_->report.note("mid-file corruption detected; recovery refuses to skip records");
      } else {
        impl_->report.records_truncated += 1;
        impl_->report.bytes_truncated += raw.size() - cursor;
      }
      stopped = true;
      break;
    }
    JournalRecord record{};
    record.type = static_cast<JournalRecordType>(type_raw);
    record.payload.assign(reinterpret_cast<const std::byte*>(raw.data() + cursor + kRecordPrefixBytes),
                          reinterpret_cast<const std::byte*>(raw.data() + cursor + kRecordPrefixBytes + payload_len));
    impl_->records.push_back(std::move(record));
    cursor += kRecordPrefixBytes + payload_len;
    last_good = cursor;
  }
  (void)stopped;

  impl_->report.records_usable = impl_->records.size();
  impl_->valid_prefix_bytes = last_good;
  impl_->bytes_written = size;
  if (impl_->report.records_truncated > 0 && impl_->report.status.ok()) {
    impl_->report.note("torn write detected at journal tail; valid prefix retained");
  }
  return impl_->report.status;
}

Status FileJournal::append(JournalRecordType type, std::span<const std::byte> payload) {
  if (!impl_->open || impl_->file == nullptr) {
    return Status{Code::internal, "journal is not open"};
  }
  if (payload.size() > impl_->config.max_record_bytes) {
    return Status{Code::capacity_exceeded, "record exceeds configured maximum size"};
  }
  const std::uint64_t projected = impl_->bytes_written + kRecordPrefixBytes + payload.size();
  if (projected > impl_->config.max_file_bytes) {
    return Status{Code::capacity_exceeded, "journal file would exceed configured maximum size"};
  }
  std::uint8_t prefix[kRecordPrefixBytes];
  encode_record(prefix, type, payload);
  if (std::fseek(impl_->file, static_cast<long>(impl_->bytes_written), SEEK_SET) != 0) {
    return Status{Code::internal, "cannot seek journal for append"};
  }
  if (std::fwrite(prefix, 1, kRecordPrefixBytes, impl_->file) != kRecordPrefixBytes) {
    return Status{Code::internal, "cannot append journal record prefix"};
  }
  if (!payload.empty() && std::fwrite(payload.data(), 1, payload.size(), impl_->file) != payload.size()) {
    return Status{Code::internal, "cannot append journal record payload"};
  }
  impl_->bytes_written = projected;
  impl_->valid_prefix_bytes = projected;
  JournalRecord record{};
  record.type = type;
  record.payload.assign(payload.begin(), payload.end());
  impl_->records.push_back(std::move(record));
  return Status{};
}

Status FileJournal::sync() {
  if (!impl_->open || impl_->file == nullptr) {
    return Status{Code::internal, "journal is not open"};
  }
  if (!impl_->config.sync_on_commit) {
    return std::fflush(impl_->file) == 0 ? Status{} : Status{Code::not_durable, "flush failed"};
  }
  return sync_stream(impl_->file) ? Status{} : Status{Code::not_durable, "fsync failed"};
}

Status FileJournal::write_header(const BootId& boot, Epoch epoch) {
  if (!impl_->open || impl_->file == nullptr) {
    return Status{Code::internal, "journal is not open"};
  }
  std::uint8_t header[kHeaderBytes];
  encode_header(header, boot, epoch);
  if (std::fseek(impl_->file, 0, SEEK_SET) != 0) {
    return Status{Code::internal, "cannot seek journal header"};
  }
  if (std::fwrite(header, 1, kHeaderBytes, impl_->file) != kHeaderBytes) {
    return Status{Code::internal, "cannot write journal header"};
  }
  if (std::fseek(impl_->file, static_cast<long>(impl_->bytes_written), SEEK_SET) != 0) {
    return Status{Code::internal, "cannot restore journal position"};
  }
  impl_->header_boot = boot;
  impl_->header_epoch = epoch;
  return Status{};
}

Status FileJournal::truncate_to_valid_prefix() {
  if (!impl_->open || impl_->file == nullptr) {
    return Status{Code::internal, "journal is not open"};
  }
  if (impl_->valid_prefix_bytes == impl_->bytes_written) {
    return Status{};
  }
  if (std::fflush(impl_->file) != 0) {
    return Status{Code::internal, "cannot flush before truncation"};
  }
#ifdef _WIN32
  if (_chsize_s(_fileno(impl_->file), static_cast<long long>(impl_->valid_prefix_bytes)) != 0) {
    return Status{Code::internal, "cannot truncate journal tail"};
  }
#else
  if (::ftruncate(::fileno(impl_->file), static_cast<off_t>(impl_->valid_prefix_bytes)) != 0) {
    return Status{Code::internal, "cannot truncate journal tail"};
  }
#endif
  impl_->bytes_written = impl_->valid_prefix_bytes;
  if (!sync_stream(impl_->file)) {
    return Status{Code::not_durable, "cannot flush truncated journal"};
  }
  return Status{};
}

Status FileJournal::rewrite(std::span<const JournalRecord> records) {
  if (!impl_->open) {
    return Status{Code::internal, "journal is not open"};
  }
  const std::string temp_path = impl_->config.path + ".tmp";
  std::FILE* temp = std::fopen(temp_path.c_str(), "wb+");
  if (temp == nullptr) {
    return Status{Code::internal, "cannot create compaction target"};
  }
  std::uint8_t header[kHeaderBytes];
  encode_header(header, impl_->header_boot, impl_->header_epoch);
  bool ok = std::fwrite(header, 1, kHeaderBytes, temp) == kHeaderBytes;
  std::uint64_t written = kHeaderBytes;
  for (const auto& record : records) {
    if (!ok) {
      break;
    }
    if (record.payload.size() > impl_->config.max_record_bytes) {
      ok = false;
      break;
    }
    std::uint8_t prefix[kRecordPrefixBytes];
    encode_record(prefix, record.type, record.payload);
    ok = std::fwrite(prefix, 1, kRecordPrefixBytes, temp) == kRecordPrefixBytes &&
         (record.payload.empty() || std::fwrite(record.payload.data(), 1, record.payload.size(), temp) == record.payload.size());
    written += kRecordPrefixBytes + record.payload.size();
  }
  if (ok && written > impl_->config.max_file_bytes) {
    ok = false;
  }
  if (ok) {
    ok = sync_stream(temp);
  }
  std::fclose(temp);
  if (!ok) {
    std::error_code remove_error;
    std::filesystem::remove(temp_path, remove_error);
    return Status{Code::not_durable, "compaction failed; original journal retained"};
  }

  // Close the live handle, swap the file atomically, reopen.
  std::fclose(impl_->file);
  impl_->file = nullptr;
  std::error_code rename_error;
  std::filesystem::rename(temp_path, impl_->config.path, rename_error);
  if (rename_error) {
    impl_->file = std::fopen(impl_->config.path.c_str(), "rb+");
    return Status{Code::internal, "atomic journal replacement failed"};
  }
  impl_->file = std::fopen(impl_->config.path.c_str(), "rb+");
  if (impl_->file == nullptr) {
    impl_->open = false;
    return Status{Code::internal, "cannot reopen compacted journal"};
  }
  impl_->records.assign(records.begin(), records.end());
  impl_->bytes_written = written;
  impl_->valid_prefix_bytes = written;
  impl_->report.records_usable = records.size();
  impl_->report.records_truncated = 0;
  impl_->report.records_corrupt = 0;
  impl_->report.bytes_truncated = 0;
  if (std::fseek(impl_->file, static_cast<long>(written), SEEK_SET) != 0) {
    return Status{Code::internal, "cannot position compacted journal"};
  }
  return Status{};
}

void FileJournal::close() noexcept {
  if (impl_ != nullptr && impl_->file != nullptr) {
    std::fflush(impl_->file);
    std::fclose(impl_->file);
    impl_->file = nullptr;
  }
  if (impl_ != nullptr) {
    impl_->open = false;
  }
}

}  // namespace qf
