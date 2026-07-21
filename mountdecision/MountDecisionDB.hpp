/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/dataStructures/MountType.hpp"
#include "scheduler/rdbms/ConnProvider.hpp"

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace cta::mountdecision {

struct MountCandidate {
  std::string candidateKey;
  cta::common::dataStructures::MountType mountType;
  std::string logicalLibrary;
  std::string tapePool;
  std::string vo;
  std::optional<std::string> vid;
  uint64_t priority = 0;
  uint64_t minRequestAge = 0;
  uint64_t filesQueued = 0;
  uint64_t bytesQueued = 0;
  uint64_t oldestJobStartTime = 0;
  uint64_t candidateScore;
  std::optional<uint64_t> overrideCandidateScore;
  std::string state;
  std::optional<std::string> stateReason;
  std::optional<std::string> createdBy;
};

struct ReservedMountCandidate {
  uint64_t candidateId = 0;
  MountCandidate candidate;
};

struct MountCandidateRecord {
  uint64_t candidateId = 0;
  MountCandidate candidate;
  std::optional<std::string> reservedByHost;
  std::optional<std::string> reservedByDrive;
  std::optional<uint64_t> reservedTime;
  std::optional<uint64_t> reservationHeartbeatTime;
  uint64_t creationTime = 0;
  uint64_t lastUpdateTime = 0;
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

  std::vector<MountCandidateRecord> blockExpiredReservedMountCandidates(uint64_t reservationTimeoutSeconds);

  void replaceMountCandidates(const std::vector<MountCandidate>& candidates, uint64_t reservationTimeoutSeconds);

  bool hasAvailableMountCandidate(const std::string& logicalLibrary);

  std::vector<MountCandidateRecord> listMountCandidates();

  void setMountCandidateScoreOverride(const std::string& candidateKey, std::optional<uint64_t> overrideCandidateScore);

  std::optional<ReservedMountCandidate>
  tryReserveNextMountCandidate(const std::string& logicalLibrary, const std::string& host, const std::string& drive);

  void blockReservedMountCandidate(uint64_t candidateId,
                                   const std::string& host,
                                   const std::string& drive,
                                   const std::string& reason);

  void releaseMountCandidate(uint64_t candidateId, const std::string& host, const std::string& drive);

  void heartbeatMountCandidate(uint64_t candidateId, const std::string& host, const std::string& drive);

private:
  ConnProvider& m_connectionProvider;
};

}  // namespace cta::mountdecision
