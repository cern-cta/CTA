/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "MountCandidateLsResponseStream.hpp"

#include "common/dataStructures/MountTypeSerDeser.hpp"
#include "common/exception/UserError.hpp"

namespace cta::frontend {

namespace {

template<typename T>
T valueOrDefault(const std::optional<T>& value) {
  return value.value_or(T {});
}

}  // namespace

MountCandidateLsResponseStream::MountCandidateLsResponseStream(cta::catalogue::Catalogue& catalogue,
                                                               cta::Scheduler& scheduler,
                                                               cta::mountdecision::MountDecisionDB& mountDecisionDb,
                                                               const std::string& instanceName,
                                                               cta::log::LogContext& lc)
    : CtaAdminResponseStream(catalogue, scheduler, instanceName),
      m_schedulerBackendName(scheduler.getSchedulerBackendName()) {
  static_cast<void>(lc);
#ifdef CTA_PGSCHED
  m_mountCandidateRecords = mountDecisionDb.listMountCandidates();
#else
  static_cast<void>(mountDecisionDb);
  throw cta::exception::UserError(
    "The mountcandidate command requires a PostgreSQL scheduler database exposing a connection provider.");
#endif
}

bool MountCandidateLsResponseStream::isDone() {
  return m_mountCandidateRecordsIdx >= m_mountCandidateRecords.size();
}

cta::xrd::Data MountCandidateLsResponseStream::next() {
  if (isDone()) {
    throw std::runtime_error("Stream is exhausted");
  }

  const auto& record = m_mountCandidateRecords[m_mountCandidateRecordsIdx++];
  const auto& candidate = record.candidate;

  cta::xrd::Data data;
  auto mclsItem = data.mutable_mcls_item();

  mclsItem->set_candidate_id(record.candidateId);
  mclsItem->set_mount_type(cta::admin::MountTypeToProtobuf(candidate.mountType));
  mclsItem->set_logical_library(candidate.logicalLibrary);
  mclsItem->set_tapepool(candidate.tapePool);
  mclsItem->set_vo(candidate.vo);
  mclsItem->set_vid(valueOrDefault(candidate.vid));
  mclsItem->set_activity(valueOrDefault(candidate.activity));
  mclsItem->set_priority(candidate.priority);
  mclsItem->set_min_request_age(candidate.minRequestAge);
  mclsItem->set_files_queued(candidate.filesQueued);
  mclsItem->set_bytes_queued(candidate.bytesQueued);
  mclsItem->set_oldest_job_start_time(candidate.oldestJobStartTime);
  mclsItem->set_youngest_job_start_time(candidate.youngestJobStartTime);
  mclsItem->set_ratio_of_mount_quota_used(candidate.ratioOfMountQuotaUsed);
  mclsItem->set_candidate_score(candidate.candidateScore);
  mclsItem->set_media_type(candidate.mediaType);
  mclsItem->set_label_format(candidate.labelFormat);
  mclsItem->set_vendor(candidate.vendor);
  mclsItem->set_capacity_in_bytes(candidate.capacityInBytes);
  mclsItem->set_last_fseq(valueOrDefault(candidate.lastFSeq));
  mclsItem->set_encryption_key_name(valueOrDefault(candidate.encryptionKeyName));
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

}  // namespace cta::frontend
