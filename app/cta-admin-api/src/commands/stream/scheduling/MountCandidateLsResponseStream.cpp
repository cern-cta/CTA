/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "commands/stream/scheduling/MountCandidateLsResponseStream.hpp"

#include "protobuf/admin/MountTypeSerDeser.hpp"

#include <optional>
#include <stdexcept>

namespace cta::admin_api::commands::stream {

namespace {

template<typename T>
T valueOrDefault(const std::optional<T>& value) {
  return value.value_or(T {});
}

}  // namespace

MountCandidateLsResponseStream::MountCandidateLsResponseStream(cta::Scheduler& scheduler,
                                                               cta::mountdecision::MountDecisionDB& mountDecisionDb,
                                                               const std::string& instanceName,
                                                               cta::log::LogContext& lc,
                                                               cta::admin::HeaderType headerType)
    : CtaAdminResponseStream(headerType, instanceName),
      m_schedulerBackendName(scheduler.getSchedulerBackendName()),
      m_mountCandidateRecords(mountDecisionDb.listMountCandidates()) {
  static_cast<void>(lc);
}

bool MountCandidateLsResponseStream::isDone() {
  return m_mountCandidateRecordsIdx >= m_mountCandidateRecords.size();
}

cta::xrd::Data MountCandidateLsResponseStream::next() {
  if (isDone()) {
    throw std::runtime_error("Stream is exhausted");
  }

  cta::xrd::Data data;
  const auto& record = m_mountCandidateRecords[m_mountCandidateRecordsIdx++];
  const auto& candidate = record.candidate;

  auto mclsItem = data.mutable_mcls_item();

  mclsItem->set_candidate_id(record.candidateId);
  mclsItem->set_mount_type(cta::admin::MountTypeToProtobuf(candidate.mountType));
  mclsItem->set_logical_library(candidate.logicalLibrary);
  mclsItem->set_tapepool(candidate.tapePool);
  mclsItem->set_vo(candidate.vo);
  mclsItem->set_vid(valueOrDefault(candidate.vid));
  mclsItem->set_priority(candidate.priority);
  mclsItem->set_min_request_age(candidate.minRequestAge);
  mclsItem->set_files_queued(candidate.filesQueued);
  mclsItem->set_bytes_queued(candidate.bytesQueued);
  mclsItem->set_oldest_job_start_time(candidate.oldestJobStartTime);
  mclsItem->set_candidate_score(candidate.candidateScore);
  mclsItem->set_state(candidate.state);
  mclsItem->set_state_reason(valueOrDefault(candidate.stateReason));
  mclsItem->set_reserved_by_host(valueOrDefault(record.reservedByHost));
  mclsItem->set_reserved_by_drive(valueOrDefault(record.reservedByDrive));
  mclsItem->set_reserved_time(valueOrDefault(record.reservedTime));
  mclsItem->set_reservation_heartbeat_time(valueOrDefault(record.reservationHeartbeatTime));
  mclsItem->set_created_by(valueOrDefault(candidate.createdBy));
  mclsItem->set_creation_time(record.creationTime);
  mclsItem->set_last_update_time(record.lastUpdateTime);
  mclsItem->set_instance_name(m_instanceName);
  mclsItem->set_scheduler_backend_name(m_schedulerBackendName);
  mclsItem->set_override_candidate_score(valueOrDefault(candidate.overrideCandidateScore));

  return data;
}

}  // namespace cta::admin_api::commands::stream
