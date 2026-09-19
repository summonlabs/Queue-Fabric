// Queue Fabric - vendor-neutral queue lifecycle and occupancy governance runtime.
// Copyright 2026 Summon Software Labs.
// SPDX-License-Identifier: Apache-2.0
#pragma once

#include <cstddef>
#include <cstdint>
#include <span>
#include <string>
#include <vector>

#include "qf/bytes.hpp"
#include "qf/ids.hpp"
#include "qf/limits.hpp"
#include "qf/status.hpp"

namespace qf {

/// Record types written to durable storage. The set is versioned; recovery
/// refuses formats it does not understand rather than guessing.
enum class JournalRecordType : std::uint16_t {
  resource_state = 1,  ///< Durable resource/class/pool/authority snapshot.
  queue_state = 2,     ///< Durable per-queue authoritative state.
  begin_attempt = 3,   ///< A mutation attempt started; commit not yet durable.
  commit_mutation = 4, ///< The mutation effect committed and is durable.
  epoch_state = 5,     ///< Coordinator epoch / boot / incarnation record.
  noop = 6,
};

const char* to_string(JournalRecordType value) noexcept;

/// One durable record: type plus opaque bounded payload.
struct JournalRecord {
  JournalRecordType type{JournalRecordType::noop};
  Bytes payload{};

  friend bool operator==(const JournalRecord& a, const JournalRecord& b) noexcept {
    return a.type == b.type && a.payload == b.payload;
  }
};

/// Outcome of opening and scanning durable state.
struct RecoveryReport {
  bool journal_present{false};
  bool header_valid{false};
  std::uint16_t format_version{0};

  std::size_t records_total{0};
  std::size_t records_usable{0};
  std::size_t records_truncated{0};   ///< Torn tail: dropped, reported, never trusted.
  std::size_t records_corrupt{0};     ///< Integrity failure inside the file.
  std::size_t bytes_truncated{0};
  std::size_t orphaned_begins{0};     ///< Attempts with no durable commit.

  std::size_t queues_restored{0};
  std::size_t queues_revalidated{0};
  std::size_t mutations_replayed{0};
  std::size_t mutations_rejected{0};

  bool previous_boot_known{false};
  BootId previous_boot{};
  BootId current_boot{};
  Epoch previous_epoch{};
  Epoch current_epoch{};

  Status status{};
  std::vector<std::string> notes{};

  void note(std::string text);
};

/// Durable record store.
///
/// Implementations must guarantee that a record acknowledged by append+sync is
/// durable, and that a partially written record is detected and reported on the
/// next open rather than silently accepted.
class IJournal {
 public:
  IJournal() = default;
  IJournal(const IJournal&) = delete;
  IJournal& operator=(const IJournal&) = delete;
  virtual ~IJournal();

  /// Opens (or creates) the store and scans existing content.
  virtual Status open() = 0;

  /// Durable records in file order that passed validation.
  [[nodiscard]] virtual std::span<const JournalRecord> records() const noexcept = 0;

  [[nodiscard]] virtual const RecoveryReport& recovery() const noexcept = 0;

  /// Appends one record. Not durable until sync() returns ok.
  virtual Status append(JournalRecordType type, std::span<const std::byte> payload) = 0;

  /// Flushes everything appended so far to stable storage.
  virtual Status sync() = 0;

  /// Atomically replaces the store contents with the given records (compaction).
  virtual Status rewrite(std::span<const JournalRecord> records) = 0;

  /// Persists the durable epoch/boot marker. Stores that do not carry a header
  /// accept the call as a no-op.
  virtual Status write_header(const BootId& boot, Epoch epoch) {
    (void)boot;
    (void)epoch;
    return Status{};
  }

  /// Drops a torn tail at the last valid record boundary. Stores without a
  /// physical file accept the call as a no-op.
  virtual Status truncate_to_valid_prefix() { return Status{}; }

  virtual void close() noexcept = 0;

  [[nodiscard]] virtual std::string describe() const = 0;
};

/// Configuration for the file-backed journal.
struct JournalConfig {
  std::string path{};                 ///< Journal file path.
  bool sync_on_commit{true};          ///< fsync before a durable commit is acknowledged.
  std::uint64_t max_file_bytes{limits::kMaxJournalFileBytes};
  std::size_t max_record_bytes{limits::kMaxJournalRecordBytes};
};

/// File-backed crash-safe journal.
///
/// Layout: a fixed header (magic, format version, boot id, epoch, sequence)
/// followed by length-prefixed records, each carrying a CRC-32C over type and
/// payload. A short or corrupt trailing record is treated as a torn write and
/// dropped with a report; corruption followed by further valid records is an
/// integrity failure and is surfaced as such.
class FileJournal final : public IJournal {
 public:
  explicit FileJournal(JournalConfig config);
  ~FileJournal() override;

  Status open() override;
  [[nodiscard]] std::span<const JournalRecord> records() const noexcept override;
  [[nodiscard]] const RecoveryReport& recovery() const noexcept override;
  Status append(JournalRecordType type, std::span<const std::byte> payload) override;
  Status sync() override;
  Status rewrite(std::span<const JournalRecord> records) override;
  void close() noexcept override;
  [[nodiscard]] std::string describe() const override;

  /// Persists the header (boot/epoch) after recovery advances the epoch.
  Status write_header(const BootId& boot, Epoch epoch);

  /// Drops the tail of the file at the last valid record boundary. Used after a
  /// torn write is detected so that subsequent appends are contiguous.
  Status truncate_to_valid_prefix();

 private:
  struct Impl;
  Impl* impl_;
};

/// In-memory journal used by tests that do not exercise durability. It is
/// explicitly not durable: describe() says so, and it is never used for
/// durability claims.
class MemoryJournal final : public IJournal {
 public:
  MemoryJournal() = default;
  ~MemoryJournal() override;

  Status open() override;
  [[nodiscard]] std::span<const JournalRecord> records() const noexcept override;
  [[nodiscard]] const RecoveryReport& recovery() const noexcept override;
  Status append(JournalRecordType type, std::span<const std::byte> payload) override;
  Status sync() override;
  Status rewrite(std::span<const JournalRecord> records) override;
  void close() noexcept override;
  [[nodiscard]] std::string describe() const override;

  void set_sync_failure(bool fail) noexcept { sync_failure_ = fail; }

 private:
  std::vector<JournalRecord> records_{};
  RecoveryReport report_{};
  bool sync_failure_{false};
  bool open_{false};
};

}  // namespace qf
