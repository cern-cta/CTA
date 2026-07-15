/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/dataStructures/MountType.hpp"
#include "scheduler/rdbms/ConnProvider.hpp"

#include <optional>
#include <string>
#include <vector>

namespace cta::mountdecision {

struct MountCandidate {
  cta::common::dataStructures::MountType mountType;
  std::string logicalLibrary;
  std::string tapePool;
  std::string vo;
  std::optional<std::string> vid;
  std::optional<std::string> activity;
  uint64_t priority = 0;
  uint64_t minRequestAge = 0;
  uint64_t filesQueued = 0;
  uint64_t bytesQueued = 0;
  uint64_t oldestJobStartTime = 0;
  uint64_t youngestJobStartTime = 0;
  double ratioOfMountQuotaUsed = 0.0;
  uint64_t candidateRank;
  std::string mediaType;
  uint64_t labelFormat = 0;
  std::string vendor;
  uint64_t capacityInBytes = 0;
  std::optional<uint64_t> lastFSeq;
  std::optional<std::string> encryptionKeyName;
  std::string state;
  std::optional<std::string> stateReason;
  std::optional<std::string> createdBy;
};

struct ReservedMountCandidate {
  uint64_t candidateId = 0;
  MountCandidate candidate;
};

/**
 * Relational backend for the Mount Decision DB.
 */
class MountDecisionDB {
public:
  explicit MountDecisionDB(ConnProvider& connectionProvider);

  void ping();

  void setValue(const std::string& key, const std::string& value);

  std::optional<std::string> getValue(const std::string& key);

  void incrementCounter(const std::string& key);

  bool tryAcquireRefreshLock(const std::string& workKey, const std::string& host, uint64_t leaseSeconds);

  void replaceMountCandidates(const std::vector<MountCandidate>& candidates, uint64_t reservationTimeoutSeconds);

  bool hasAvailableMountCandidate(const std::string& logicalLibrary);

  std::optional<ReservedMountCandidate>
  tryReserveNextMountCandidate(const std::string& logicalLibrary, const std::string& host, const std::string& drive);

  void releaseMountCandidate(uint64_t candidateId, const std::string& host, const std::string& drive);

  void heartbeatMountCandidate(uint64_t candidateId, const std::string& host, const std::string& drive);

private:
  ConnProvider& m_connectionProvider;
};

}  // namespace cta::mountdecision
