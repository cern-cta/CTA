/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "MountSlotLsResponseStream.hpp"

#include "common/dataStructures/MountTypeSerDeser.hpp"
#include "common/exception/UserError.hpp"

#ifdef CTA_PGSCHED
#include "scheduler/rdbms/ConnProvider.hpp"
#endif

namespace cta::frontend {

namespace {

template<typename T>
T valueOrDefault(const std::optional<T>& value) {
  return value.value_or(T {});
}

}  // namespace

MountSlotLsResponseStream::MountSlotLsResponseStream(cta::catalogue::Catalogue& catalogue,
                                                     cta::Scheduler& scheduler,
                                                     cta::SchedulerDatabase& schedulerDb,
                                                     const std::string& instanceName,
                                                     cta::log::LogContext& lc)
    : CtaAdminResponseStream(catalogue, scheduler, instanceName),
      m_schedulerBackendName(scheduler.getSchedulerBackendName()) {
  static_cast<void>(lc);
#ifdef CTA_PGSCHED
  auto* connProvider = dynamic_cast<cta::ConnProvider*>(&schedulerDb);
  if (connProvider == nullptr) {
    throw cta::exception::UserError(
      "The mountslot command requires a PostgreSQL scheduler database exposing a connection provider.");
  }

  cta::mountdecision::MountDecisionDB mountDecisionDb(*connProvider);
  m_mountSlots = mountDecisionDb.listMountSlots();
#else
  static_cast<void>(schedulerDb);
  throw cta::exception::UserError(
    "The mountslot command requires a PostgreSQL scheduler database exposing a connection provider.");
#endif
}

bool MountSlotLsResponseStream::isDone() {
  return m_mountSlotsIdx >= m_mountSlots.size();
}

cta::xrd::Data MountSlotLsResponseStream::next() {
  if (isDone()) {
    throw std::runtime_error("Stream is exhausted");
  }

  const auto& slot = m_mountSlots[m_mountSlotsIdx++];
  const auto& candidate = slot.candidate;

  cta::xrd::Data data;
  auto mslsItem = data.mutable_msls_item();

  mslsItem->set_candidate_id(slot.candidateId);
  mslsItem->set_mount_type(cta::admin::MountTypeToProtobuf(candidate.mountType));
  mslsItem->set_logical_library(candidate.logicalLibrary);
  mslsItem->set_tapepool(candidate.tapePool);
  mslsItem->set_vo(candidate.vo);
  mslsItem->set_vid(valueOrDefault(candidate.vid));
  mslsItem->set_activity(valueOrDefault(candidate.activity));
  mslsItem->set_priority(candidate.priority);
  mslsItem->set_min_request_age(candidate.minRequestAge);
  mslsItem->set_files_queued(candidate.filesQueued);
  mslsItem->set_bytes_queued(candidate.bytesQueued);
  mslsItem->set_oldest_job_start_time(candidate.oldestJobStartTime);
  mslsItem->set_youngest_job_start_time(candidate.youngestJobStartTime);
  mslsItem->set_ratio_of_mount_quota_used(candidate.ratioOfMountQuotaUsed);
  mslsItem->set_candidate_rank(candidate.candidateRank);
  mslsItem->set_media_type(candidate.mediaType);
  mslsItem->set_label_format(candidate.labelFormat);
  mslsItem->set_vendor(candidate.vendor);
  mslsItem->set_capacity_in_bytes(candidate.capacityInBytes);
  mslsItem->set_last_fseq(valueOrDefault(candidate.lastFSeq));
  mslsItem->set_encryption_key_name(valueOrDefault(candidate.encryptionKeyName));
  mslsItem->set_state(candidate.state);
  mslsItem->set_state_reason(valueOrDefault(candidate.stateReason));
  mslsItem->set_reserved_by_host(valueOrDefault(slot.reservedByHost));
  mslsItem->set_reserved_by_drive(valueOrDefault(slot.reservedByDrive));
  mslsItem->set_reserved_time(valueOrDefault(slot.reservedTime));
  mslsItem->set_reservation_heartbeat_time(valueOrDefault(slot.reservationHeartbeatTime));
  mslsItem->set_created_by(valueOrDefault(candidate.createdBy));
  mslsItem->set_creation_time(slot.creationTime);
  mslsItem->set_last_update_time(slot.lastUpdateTime);
  mslsItem->set_instance_name(m_instanceName);
  mslsItem->set_scheduler_backend_name(m_schedulerBackendName);

  return data;
}

}  // namespace cta::frontend
