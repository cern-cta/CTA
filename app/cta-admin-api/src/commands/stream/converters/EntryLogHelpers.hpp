/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

namespace cta::admin_api::commands::stream {

/// @brief Fills a protobuf entry log message (username, host, time) from an EntryLog
template<class LogProto, class EntryLog>
void fillEntryLog(LogProto* out, const EntryLog& log) {
  out->set_username(log.username);
  out->set_host(log.host);
  out->set_time(log.time);
}

/// @brief Fills the creation_log and last_modification_log fields of a protobuf item
template<class ItemProto, class CreationLog, class ModificationLog>
void fillEntryLogs(ItemProto* item, const CreationLog& creationLog, const ModificationLog& lastModificationLog) {
  fillEntryLog(item->mutable_creation_log(), creationLog);
  fillEntryLog(item->mutable_last_modification_log(), lastModificationLog);
}

}  // namespace cta::admin_api::commands::stream
