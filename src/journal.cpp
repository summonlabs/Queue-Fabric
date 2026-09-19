// Queue Fabric - vendor-neutral queue lifecycle and occupancy governance runtime.
// Copyright 2026 Summon Software Labs.
// SPDX-License-Identifier: Apache-2.0
#include "qf/journal.hpp"

namespace qf {

const char* to_string(JournalRecordType value) noexcept {
  switch (value) {
    case JournalRecordType::resource_state: return "resource_state";
    case JournalRecordType::queue_state: return "queue_state";
    case JournalRecordType::begin_attempt: return "begin_attempt";
    case JournalRecordType::commit_mutation: return "commit_mutation";
    case JournalRecordType::epoch_state: return "epoch_state";
    case JournalRecordType::noop: return "noop";
  }
  return "unknown";
}

IJournal::~IJournal() = default;

void RecoveryReport::note(std::string text) {
  constexpr std::size_t kMaxNotes = 24;
  if (notes.size() >= kMaxNotes) {
    return;
  }
  if (text.size() > limits::kMaxMessageChars) {
    text.resize(limits::kMaxMessageChars);
  }
  notes.push_back(std::move(text));
}

MemoryJournal::~MemoryJournal() = default;

Status MemoryJournal::open() {
  open_ = true;
  records_.clear();
  report_ = RecoveryReport{};
  report_.journal_present = false;
  report_.header_valid = true;
  report_.format_version = 1;
  report_.status = Status{};
  report_.note("in-memory journal: not durable");
  return Status{};
}

std::span<const JournalRecord> MemoryJournal::records() const noexcept { return records_; }

const RecoveryReport& MemoryJournal::recovery() const noexcept { return report_; }

Status MemoryJournal::append(JournalRecordType type, std::span<const std::byte> payload) {
  if (!open_) {
    return Status{Code::internal, "journal is not open"};
  }
  if (payload.size() > limits::kMaxJournalRecordBytes) {
    return Status{Code::capacity_exceeded, "record exceeds maximum size"};
  }
  JournalRecord record{};
  record.type = type;
  record.payload.assign(payload.begin(), payload.end());
  records_.push_back(std::move(record));
  return Status{};
}

Status MemoryJournal::sync() {
  if (sync_failure_) {
    return Status{Code::not_durable, "injected sync failure"};
  }
  return Status{};
}

Status MemoryJournal::rewrite(std::span<const JournalRecord> records) {
  records_.assign(records.begin(), records.end());
  return Status{};
}

void MemoryJournal::close() noexcept { open_ = false; }

std::string MemoryJournal::describe() const {
  return "memory journal (not durable, records=" + std::to_string(records_.size()) + ")";
}

}  // namespace qf
