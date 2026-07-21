/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "mountdecision/MountDecision.hpp"

#include "catalogue/Catalogue.hpp"
#include "catalogue/TapeForWriting.hpp"
#include "catalogue/TapeSearchCriteria.hpp"
#include "common/dataStructures/LogicalLibrary.hpp"
#include "common/dataStructures/PhysicalLibrary.hpp"
#include "common/dataStructures/Tape.hpp"
#include "common/dataStructures/VirtualOrganization.hpp"
#include "common/exception/Exception.hpp"
#include "common/semconv/Logging.hpp"
#include "common/utils/Timer.hpp"
#include "common/utils/utils.hpp"
#include "scheduler/ArchiveMount.hpp"
#include "scheduler/RetrieveMount.hpp"
#include "scheduler/Scheduler.hpp"
#include "scheduler/SchedulerDatabase.hpp"

#include <algorithm>
#include <chrono>
#include <exception>
#include <list>
#include <map>
#include <optional>
#include <set>
#include <sstream>
#include <utility>
#include <vector>

namespace cta::mountdecision {

namespace {

constexpr uint64_t c_minFilesToWarrantAMount = 5;
constexpr uint64_t c_minBytesToWarrantAMount = 2000000;
constexpr uint64_t c_refreshLockLeaseSeconds = 300;
constexpr uint64_t c_reservationTimeoutSeconds = 300;
const std::string c_refreshWorkKey = "mount-candidate-refresh";

using TapePoolMountPair = std::pair<std::string, common::dataStructures::MountType>;
using VirtualOrganizationMountPair = std::pair<std::string, common::dataStructures::MountType>;

struct MountCounts {
  uint32_t totalMounts = 0;

  struct AutoZeroUint32_t {
    uint32_t value = 0;
  };

  std::map<std::string, AutoZeroUint32_t, std::less<>> activityMounts;
};

using ExistingMountSummaryPerTapepool = std::map<TapePoolMountPair, MountCounts>;
using ExistingMountSummaryPerVo = std::map<VirtualOrganizationMountPair, MountCounts>;

struct CandidatePotential {
  SchedulerDatabase::PotentialMount mount;
  std::optional<std::string> blockedReason;
};

struct MountInfoForDecision {
  std::vector<CandidatePotential> potentialMounts;
  std::set<std::string, std::less<>> tapesInUse;
  std::vector<catalogue::TapeForWriting> tapesForWriting;
  ExistingMountSummaryPerTapepool existingMountsDistinctTypeSummaryPerTapepool;
  ExistingMountSummaryPerVo existingMountsBasicTypeSummaryPerVo;
  std::map<std::string, common::dataStructures::VirtualOrganization, std::less<>> voNameVoMap;
};

struct CandidateRow {
  // Intermediate row before the final global ordering. Archive potentials can
  // expand into several rows, one per writable tape that still fits the
  // simulated mount limits.
  CandidatePotential potential;
  std::string logicalLibrary;
  std::optional<catalogue::TapeForWriting> tape;
  std::optional<std::string> blockedReason;
};

std::optional<std::string> getMountBlockReason(const SchedulerDatabase::PotentialMount& mount,
                                               const ExistingMountSummaryPerTapepool& existingMountsPerTapepool,
                                               const ExistingMountSummaryPerVo& existingMountsPerVo,
                                               const common::dataStructures::VirtualOrganization& vo) {
  // This mirrors the "can this mount start now?" checks from the existing
  // scheduler path. The caller may pass real mount counts or simulated counts
  // that already include candidates emitted earlier in the same refresh.
  uint32_t existingMountsDistinctTypesForThisTapepool = 0;
  uint32_t existingMountsBasicTypeForThisVo = 0;
  const auto basicMountType = common::dataStructures::getMountBasicType(mount.type);
  try {
    existingMountsDistinctTypesForThisTapepool =
      existingMountsPerTapepool.at(TapePoolMountPair(mount.tapePool, mount.type)).totalMounts;
  } catch (std::out_of_range&) {}
  try {
    existingMountsBasicTypeForThisVo =
      existingMountsPerVo.at(VirtualOrganizationMountPair(vo.name, basicMountType)).totalMounts;
  } catch (std::out_of_range&) {}

  uint32_t effectiveExistingMountsForThisTapepool = 0;
  if (basicMountType == common::dataStructures::MountType::ArchiveAllTypes) {
    effectiveExistingMountsForThisTapepool = existingMountsDistinctTypesForThisTapepool;
  }

  bool mountPassesACriteria = false;
  uint64_t minBytesToWarrantAMount = c_minBytesToWarrantAMount;
  uint64_t minFilesToWarrantAMount = c_minFilesToWarrantAMount;
  if (mount.type == common::dataStructures::MountType::ArchiveForRepack) {
    minBytesToWarrantAMount *= 2;
    minFilesToWarrantAMount *= 2;
  }
  if (mount.bytesQueued / (1 + effectiveExistingMountsForThisTapepool) >= minBytesToWarrantAMount) {
    mountPassesACriteria = true;
  }
  if (mount.filesQueued / (1 + effectiveExistingMountsForThisTapepool) >= minFilesToWarrantAMount) {
    mountPassesACriteria = true;
  }
  if (!effectiveExistingMountsForThisTapepool
      && ((std::chrono::system_clock::to_time_t(std::chrono::system_clock::now()) - mount.oldestJobStartTime)
          > mount.minRequestAge)) {
    mountPassesACriteria = true;
  }

  uint64_t maxDrives = 0;
  if (basicMountType == common::dataStructures::MountType::Retrieve) {
    maxDrives = vo.readMaxDrives;
  } else if (basicMountType == common::dataStructures::MountType::ArchiveAllTypes) {
    maxDrives = vo.writeMaxDrives;
  }

  if (!mountPassesACriteria) {
    return "Mount criteria not met";
  }
  if (existingMountsBasicTypeForThisVo >= maxDrives) {
    return "VO mount drive limit reached";
  }
  if (mount.sleepingMount) {
    return "Sleeping mount";
  }
  return std::nullopt;
}

void incrementSimulatedMountCounts(const SchedulerDatabase::PotentialMount& mount,
                                   ExistingMountSummaryPerTapepool& existingMountsPerTapepool,
                                   ExistingMountSummaryPerVo& existingMountsPerVo) {
  // Once an archive candidate is emitted, later candidates in the same refresh
  // must see that slot as already consumed so VO and tape-pool limits are not
  // over-allocated.
  const auto basicMountType = common::dataStructures::getMountBasicType(mount.type);
  existingMountsPerTapepool[TapePoolMountPair(mount.tapePool, mount.type)].totalMounts++;
  existingMountsPerVo[VirtualOrganizationMountPair(mount.vo, basicMountType)].totalMounts++;
  if (mount.activity.has_value()) {
    existingMountsPerTapepool[TapePoolMountPair(mount.tapePool, mount.type)]
      .activityMounts[mount.activity.value()]
      .value++;
  }
}

std::optional<catalogue::TapeForWriting> takeNextTapeForWriting(std::vector<catalogue::TapeForWriting>& tapesForWriting,
                                                                const std::string& tapePool) {
  auto tapeIt = std::find_if(tapesForWriting.begin(), tapesForWriting.end(), [&tapePool](const auto& tape) {
    return tape.tapePool == tapePool;
  });
  if (tapeIt == tapesForWriting.end()) {
    return std::nullopt;
  }

  auto ret = *tapeIt;
  tapesForWriting.erase(tapeIt);
  return ret;
}

uint64_t calculateCandidateScore(const SchedulerDatabase::PotentialMount& mount) {
  constexpr uint64_t c_priorityFactor = 10;
  constexpr uint64_t c_archiveForUserBonus = 1;

  uint64_t score = mount.priority * c_priorityFactor;
  if (mount.type == common::dataStructures::MountType::ArchiveForUser) {
    score += c_archiveForUserBonus;
  }
  return score;
}

uint64_t effectiveCandidateScore(const MountCandidate& candidate) {
  return candidate.overrideCandidateScore.value_or(candidate.candidateScore);
}

std::string calculateRetrieveCandidateKey(const SchedulerDatabase::PotentialMount& mount) {
  return common::dataStructures::toString(mount.type) + "-" + mount.vid;
}

std::string calculateArchiveCandidateKey(const SchedulerDatabase::PotentialMount& mount,
                                         const std::optional<std::string>& vid) {
  return common::dataStructures::toString(mount.type) + "-" + mount.tapePool + "-" + vid.value_or("no-vid");
}

bool potentialMountOrderLess(const SchedulerDatabase::PotentialMount& lhs,
                             const SchedulerDatabase::PotentialMount& rhs);

bool candidateRefreshOrderLess(const CandidatePotential& lhs, const CandidatePotential& rhs) {
  return potentialMountOrderLess(lhs.mount, rhs.mount);
}

using PotentialMountAggregationKey = std::pair<common::dataStructures::MountType, std::string>;

PotentialMountAggregationKey getPotentialMountAggregationKey(const SchedulerDatabase::PotentialMount& mount) {
  const auto basicType = common::dataStructures::getMountBasicType(mount.type);
  if (basicType == common::dataStructures::MountType::Retrieve) {
    return {mount.type, mount.vid};
  }
  if (basicType == common::dataStructures::MountType::ArchiveAllTypes) {
    return {mount.type, mount.tapePool};
  }
  return {mount.type, ""};
}

void mergePotentialMount(SchedulerDatabase::PotentialMount& aggregate, const SchedulerDatabase::PotentialMount& mount) {
  aggregate.filesQueued += mount.filesQueued;
  aggregate.bytesQueued += mount.bytesQueued;
  aggregate.priority = std::max(aggregate.priority, mount.priority);
  aggregate.minRequestAge = std::min(aggregate.minRequestAge, mount.minRequestAge);
  aggregate.oldestJobStartTime = std::min(aggregate.oldestJobStartTime, mount.oldestJobStartTime);
  aggregate.youngestJobStartTime = std::max(aggregate.youngestJobStartTime, mount.youngestJobStartTime);
  for (const auto& [mountPolicy, count] : mount.mountPolicyCountMap) {
    aggregate.mountPolicyCountMap[mountPolicy] += count;
  }
  if (mount.sleepingMount) {
    if (!aggregate.sleepingMount) {
      aggregate.diskSystemName = mount.diskSystemName;
      aggregate.sleepStartTime = mount.sleepStartTime;
      aggregate.sleepTime = mount.sleepTime;
    }
    aggregate.sleepingMount = true;
  }
  // Aggregation changes the row granularity. Activity may no longer identify a
  // single queue, and policy name fields are recomputed later from the merged
  // mountPolicyCountMap.
  aggregate.activity.reset();
  aggregate.highestPriorityMountPolicyName.reset();
  aggregate.lowestRequestAgeMountPolicyName.reset();
  aggregate.mountPolicyNames.reset();
}

// TODO: Temporary fix until the PostgreSQL scheduler schema exposes mount
// candidates at the same granularity expected by MountDecision.
std::vector<SchedulerDatabase::PotentialMount>
aggregatePotentialMountsForMountCandidates(const std::vector<SchedulerDatabase::PotentialMount>& potentialMounts) {
  std::map<PotentialMountAggregationKey, SchedulerDatabase::PotentialMount> aggregates;
  std::vector<PotentialMountAggregationKey> keyOrder;
  for (const auto& mount : potentialMounts) {
    const auto key = getPotentialMountAggregationKey(mount);
    if (key.second.empty()) {
      continue;
    }
    auto [it, inserted] = aggregates.emplace(key, mount);
    if (inserted) {
      keyOrder.push_back(key);
      // Aggregation changes the row granularity. Activity may no longer identify a
      // single queue, and policy name fields are recomputed later from the merged
      // mountPolicyCountMap.
      it->second.activity.reset();
      it->second.highestPriorityMountPolicyName.reset();
      it->second.lowestRequestAgeMountPolicyName.reset();
      it->second.mountPolicyNames.reset();
    } else {
      mergePotentialMount(it->second, mount);
    }
  }

  std::vector<SchedulerDatabase::PotentialMount> ret;
  ret.reserve(keyOrder.size());
  for (const auto& key : keyOrder) {
    ret.push_back(std::move(aggregates.at(key)));
  }
  return ret;
}

std::optional<std::string> optionalNonEmpty(const std::string& value) {
  if (value.empty()) {
    return std::nullopt;
  }
  return value;
}

template<typename T>
T requireValue(const std::optional<T>& value, const std::string& fieldName) {
  if (!value.has_value()) {
    throw exception::Exception("In MountDecision::getNextMount(): reserved candidate is missing " + fieldName);
  }
  return value.value();
}

bool potentialMountOrderLess(const SchedulerDatabase::PotentialMount& lhs,
                             const SchedulerDatabase::PotentialMount& rhs) {
  const uint64_t lhsScore = calculateCandidateScore(lhs);
  const uint64_t rhsScore = calculateCandidateScore(rhs);
  if (lhsScore != rhsScore) {
    return lhsScore > rhsScore;
  }
  if (lhs.oldestJobStartTime != rhs.oldestJobStartTime) {
    return lhs.oldestJobStartTime < rhs.oldestJobStartTime;
  }
  if (lhs.filesQueued != rhs.filesQueued) {
    return lhs.filesQueued > rhs.filesQueued;
  }
  if (lhs.bytesQueued != rhs.bytesQueued) {
    return lhs.bytesQueued > rhs.bytesQueued;
  }
  return false;
}

std::optional<catalogue::TapeForWriting> getTapeForWritingForCandidate(const MountCandidate& candidate,
                                                                       const std::string& logicalLibraryName,
                                                                       catalogue::Catalogue& catalogue,
                                                                       std::optional<std::string>& blockedReason) {
  const auto candidateVid = requireValue(candidate.vid, "VID");
  const auto tapesForWriting = catalogue.Tape()->getTapesForWriting(logicalLibraryName);
  const auto tapeIt = std::find_if(tapesForWriting.begin(), tapesForWriting.end(), [&candidateVid](const auto& tape) {
    return tape.vid == candidateVid;
  });
  if (tapeIt == tapesForWriting.end()) {
    blockedReason = "Tape is no longer available for writing";
    return std::nullopt;
  }
  if (tapeIt->tapePool != candidate.tapePool) {
    blockedReason = "Tape no longer belongs to the candidate tapepool";
    return std::nullopt;
  }
  return *tapeIt;
}

std::optional<common::dataStructures::Tape> getTapeForRetrieveCandidate(const MountCandidate& candidate,
                                                                        const std::string& logicalLibraryName,
                                                                        catalogue::Catalogue& catalogue,
                                                                        std::optional<std::string>& blockedReason) {
  using Tape = common::dataStructures::Tape;

  const auto candidateVid = requireValue(candidate.vid, "VID");
  try {
    const auto tapes = catalogue.Tape()->getTapesByVid(candidateVid);
    const auto tapeIt = tapes.find(candidateVid);
    if (tapeIt == tapes.end()) {
      blockedReason = "Tape no longer exists";
      return std::nullopt;
    }

    const auto& tape = tapeIt->second;
    if (tape.logicalLibraryName != logicalLibraryName) {
      blockedReason = "Tape no longer belongs to the requested logical library";
      return std::nullopt;
    }
    if (tape.tapePoolName != candidate.tapePool) {
      blockedReason = "Tape no longer belongs to the candidate tapepool";
      return std::nullopt;
    }
    if (tape.state != Tape::ACTIVE && tape.state != Tape::REPACKING) {
      blockedReason = "Tape state is " + Tape::stateToString(tape.state);
      return std::nullopt;
    }
    return tape;
  } catch (const catalogue::TapeNotFound&) {
    blockedReason = "Tape no longer exists";
    return std::nullopt;
  }
}

SchedulerDatabase::PotentialMount makePotentialMountForCandidate(const MountCandidate& candidate) {
  SchedulerDatabase::PotentialMount mount;
  mount.type = candidate.mountType;
  mount.vid = candidate.vid.value_or("");
  mount.tapePool = candidate.tapePool;
  mount.vo = candidate.vo;
  mount.priority = candidate.priority;
  mount.minRequestAge = static_cast<time_t>(candidate.minRequestAge);
  mount.filesQueued = candidate.filesQueued;
  mount.bytesQueued = candidate.bytesQueued;
  mount.oldestJobStartTime = static_cast<time_t>(candidate.oldestJobStartTime);
  mount.youngestJobStartTime = static_cast<time_t>(candidate.oldestJobStartTime);
  mount.logicalLibrary = candidate.logicalLibrary;
  mount.ratioOfMountQuotaUsed = 0.0;
  mount.mountCount = 0;
  return mount;
}

SchedulerDatabase::PotentialMount makePotentialMountForRetrieveCandidate(const MountCandidate& candidate,
                                                                         const common::dataStructures::Tape& tape) {
  auto mount = makePotentialMountForCandidate(candidate);
  mount.vid = requireValue(candidate.vid, "VID");
  mount.mediaType = tape.mediaType;
  mount.vendor = tape.vendor;
  mount.capacityInBytes = tape.capacityInBytes;
  mount.labelFormat = tape.labelFormat;
  mount.encryptionKeyName = tape.encryptionKeyName;
  return mount;
}

SchedulerDatabase::PotentialMount makePotentialMountForArchiveCandidate(const MountCandidate& candidate,
                                                                        const catalogue::TapeForWriting& tape) {
  auto mount = makePotentialMountForCandidate(candidate);
  mount.vid = tape.vid;
  mount.mediaType = tape.mediaType;
  mount.vendor = tape.vendor;
  mount.capacityInBytes = tape.capacityInBytes;
  mount.labelFormat = tape.labelFormat;
  mount.encryptionKeyName = tape.encryptionKeyName;
  return mount;
}

std::vector<common::dataStructures::LogicalLibrary> getEnabledLogicalLibraries(catalogue::Catalogue& catalogue) {
  std::set<std::string, std::less<>> disabledPhysicalLibraries;
  for (const auto& physicalLibrary : catalogue.PhysicalLibrary()->getPhysicalLibraries()) {
    if (physicalLibrary.isDisabled) {
      disabledPhysicalLibraries.insert(physicalLibrary.name);
    }
  }

  std::vector<common::dataStructures::LogicalLibrary> ret;
  for (const auto& logicalLibrary : catalogue.LogicalLibrary()->getLogicalLibraries()) {
    if (logicalLibrary.isDisabled) {
      continue;
    }
    if (logicalLibrary.physicalLibraryName.has_value()
        && disabledPhysicalLibraries.contains(logicalLibrary.physicalLibraryName.value())) {
      continue;
    }
    ret.push_back(logicalLibrary);
  }
  return ret;
}

// Intentionally adapted from Scheduler::sortAndGetTapesForMountInfo() while
// the current taped getNextMount() path remains in place.
MountInfoForDecision
prepareMountCandidatesForLogicalLibrary(const std::vector<SchedulerDatabase::PotentialMount>& potentialMounts,
                                        const std::vector<SchedulerDatabase::ExistingMount>& existingMounts,
                                        const std::string& logicalLibraryName,
                                        Scheduler& scheduler,
                                        catalogue::Catalogue& catalogue,
                                        log::LogContext& lc) {
  utils::Timer timer;
  MountInfoForDecision ret;
  auto workingPotentialMounts = potentialMounts;
  auto workingExistingMounts = existingMounts;

  bool anyRetrieve = std::ranges::any_of(workingPotentialMounts, [](const SchedulerDatabase::PotentialMount& pm) {
    return pm.type == common::dataStructures::MountType::Retrieve;
  });

  std::set<std::string> repackingTapeVids;
  if (anyRetrieve) {
    std::map<std::string, common::dataStructures::Tape, std::less<>> eligibleTapeMap;

    {
      {
        catalogue::TapeSearchCriteria searchCriteria;
        searchCriteria.logicalLibrary = logicalLibraryName;
        searchCriteria.state = common::dataStructures::Tape::ACTIVE;
        auto eligibleTapesList = catalogue.Tape()->getTapes(searchCriteria);
        for (auto& t : eligibleTapesList) {
          eligibleTapeMap[t.vid] = t;
        }
      }
      {
        catalogue::TapeSearchCriteria searchCriteria;
        searchCriteria.logicalLibrary = std::nullopt;
        searchCriteria.state = common::dataStructures::Tape::REPACKING;
        auto repackingTapesList = catalogue.Tape()->getTapes(searchCriteria);
        for (auto& t : repackingTapesList) {
          if (t.logicalLibraryName == logicalLibraryName) {
            eligibleTapeMap[t.vid] = t;
          }

          repackingTapeVids.insert(t.vid);
        }
      }
    }

    auto& v = workingPotentialMounts;

    // Remove mounts from unavailable tapes (not in same logical library, disabled, etc.)
    std::erase_if(v, [&eligibleTapeMap](const SchedulerDatabase::PotentialMount& pm) {
      return (pm.type == common::dataStructures::MountType::Retrieve && !eligibleTapeMap.contains(pm.vid));
    });

    static_cast<void>(timer.secs(utils::Timer::resetCounter));

    // Enrich potential mount entry
    for (auto& m : workingPotentialMounts) {
      if (m.type == common::dataStructures::MountType::Retrieve) {
        const auto& tp = eligibleTapeMap.at(m.vid);

        m.logicalLibrary = tp.logicalLibraryName;
        m.tapePool = tp.tapePoolName;
        m.vendor = tp.vendor;
        m.mediaType = tp.mediaType;
        m.vo = tp.vo;
        m.capacityInBytes = tp.capacityInBytes;
        m.labelFormat = tp.labelFormat;
        m.encryptionKeyName = tp.encryptionKeyName;
      }
    }
  }

  {
    // TODO: This block may no longer be necessary when using the Postgres Scheduler DB.
    //       References:
    //         - https://gitlab.cern.ch/cta/CTA/-/work_items/1164
    std::set<std::string> vidsWithEmptyTapePoolString;
    std::erase_if(workingPotentialMounts,
                  [&vidsWithEmptyTapePoolString](const SchedulerDatabase::PotentialMount& mount) {
                    if (mount.tapePool.empty()) {
                      vidsWithEmptyTapePoolString.insert(mount.vid);
                      return true;
                    }
                    return false;
                  });
    for (const auto& vid : vidsWithEmptyTapePoolString) {
      log::ScopedParamContainer params(lc);
      params.add("tapeVid", vid);
      lc.log(log::ERR,
             "In MountDecision::prepareMountCandidatesForLogicalLibrary(): empty tape pool string found on potential "
             "mounts.");
    }
  }

  {
    // TODO: This block may no longer be necessary when using the Postgres Scheduler DB.
    //       References:
    //         - https://gitlab.cern.ch/cta/CTA/-/work_items/1164
    std::set<std::string> drivesWithEmptyTapePoolString;
    std::erase_if(workingExistingMounts,
                  [&drivesWithEmptyTapePoolString](const SchedulerDatabase::ExistingMount& mount) {
                    if (mount.tapePool.empty()) {
                      drivesWithEmptyTapePoolString.insert(mount.driveName);
                      return true;
                    }
                    return false;
                  });
    for (const auto& drive : drivesWithEmptyTapePoolString) {
      log::ScopedParamContainer params(lc);
      params.add("drive", drive);
      lc.log(log::ERR,
             "In MountDecision::prepareMountCandidatesForLogicalLibrary(): empty tape pool string found on existing "
             "mount.");
    }
  }

  {
    // Remove drives connected to different scheduler backend instances
    auto schedulerBackendName = scheduler.getSchedulerBackendName();
    std::list<std::string> ignoredDrives;
    std::erase_if(workingExistingMounts,
                  [&ignoredDrives, &schedulerBackendName](const SchedulerDatabase::ExistingMount& mount) {
                    if (!mount.schedulerBackendName.has_value()) {
                      ignoredDrives.emplace_back(mount.driveName);
                      return true;
                    }
                    return mount.schedulerBackendName != schedulerBackendName;
                  });
    for (const auto& ignoredDriveName : ignoredDrives) {
      log::ScopedParamContainer params(lc);
      params.add("ignoredDrive", ignoredDriveName);
      lc.log(log::ERR,
             "In MountDecision::prepareMountCandidatesForLogicalLibrary(): found a drive without SchedulerBackendName "
             "configuration. Ignoring drive.");
    }
  }

  std::set<std::string> tapepoolsPotentialOrExistingMounts;
  for (const auto& pm : workingPotentialMounts) {
    tapepoolsPotentialOrExistingMounts.insert(pm.tapePool);
  }
  for (const auto& em : workingExistingMounts) {
    tapepoolsPotentialOrExistingMounts.insert(em.tapePool);
  }

  std::map<std::string, std::string> tapepoolVoNameMap;
  std::map<std::string, common::dataStructures::VirtualOrganization, std::less<>> voNameVoMap;
  for (auto& tapepool : tapepoolsPotentialOrExistingMounts) {
    try {
      auto vo = catalogue.VO()->getCachedVirtualOrganizationOfTapepool(tapepool);
      tapepoolVoNameMap[tapepool] = vo.name;
      voNameVoMap[vo.name] = vo;
    } catch (cta::exception::Exception& ex) {
      // The VO of this tapepool does not exist.
      // Abort the scheduling as we need it to know the number of allocated drives the VO is allowed to use.
      ex.getMessage() << " Aborting mount decision. VO does not exist." << std::endl;
      throw ex;
    }
  }

  std::optional<common::dataStructures::VirtualOrganization> defaultRepackVo =
    catalogue.VO()->getDefaultVirtualOrganizationForRepack();
  if (defaultRepackVo.has_value()) {
    voNameVoMap[defaultRepackVo->name] = defaultRepackVo.value();
  }

  for (auto& em : workingExistingMounts) {
    bool isRepackingMount = false;
    if (em.type == common::dataStructures::MountType::ArchiveForRepack) {
      isRepackingMount = true;
    } else if (em.type == common::dataStructures::MountType::Retrieve) {
      isRepackingMount = repackingTapeVids.contains(em.vid);
    }
    if (isRepackingMount && !defaultRepackVo.has_value()) {
      log::ScopedParamContainer params(lc);
      params.add("tapeVid", em.vid)
        .add("mountType", common::dataStructures::toCamelCaseString(em.type))
        .add("drive", em.driveName);
      lc.log(log::ERR,
             "In MountDecision::prepareMountCandidatesForLogicalLibrary(): existing repack mount found while there is "
             "no "
             "default repack VO defined.");
      continue;
    }
    auto voName = isRepackingMount ? defaultRepackVo->name : tapepoolVoNameMap.at(em.tapePool);
    ret.existingMountsDistinctTypeSummaryPerTapepool[TapePoolMountPair(em.tapePool, em.type)].totalMounts++;
    ret
      .existingMountsBasicTypeSummaryPerVo
        [VirtualOrganizationMountPair(voName, common::dataStructures::getMountBasicType(em.type))]
      .totalMounts++;
    if (em.activity) {
      ret.existingMountsDistinctTypeSummaryPerTapepool[TapePoolMountPair(em.tapePool, em.type)]
        .activityMounts[em.activity.value()]
        .value++;
    }
    if (em.vid.size()) {
      ret.tapesInUse.insert(em.vid);
    }
  }

  ret.potentialMounts.reserve(workingPotentialMounts.size());
  for (auto& m : workingPotentialMounts) {
    CandidatePotential candidate;
    candidate.mount = std::move(m);

    bool isRepackingMount = false;
    if (candidate.mount.type == common::dataStructures::MountType::ArchiveForRepack) {
      isRepackingMount = true;
    } else if (candidate.mount.type == common::dataStructures::MountType::Retrieve) {
      isRepackingMount = repackingTapeVids.contains(candidate.mount.vid);
    }

    if (isRepackingMount) {
      if (defaultRepackVo.has_value()) {
        candidate.mount.vo = defaultRepackVo->name;
      } else {
        candidate.blockedReason = "No default repack VO defined";
      }
    } else {
      candidate.mount.vo = tapepoolVoNameMap.at(candidate.mount.tapePool);
    }

    ret.potentialMounts.push_back(std::move(candidate));
  }

  for (auto& candidate : ret.potentialMounts) {
    if (candidate.blockedReason.has_value()) {
      continue;
    }

    auto& m = candidate.mount;
    auto blockReason = getMountBlockReason(m,
                                           ret.existingMountsDistinctTypeSummaryPerTapepool,
                                           ret.existingMountsBasicTypeSummaryPerVo,
                                           voNameVoMap.at(m.vo));
    if (blockReason.has_value()) {
      candidate.blockedReason = blockReason;
    } else {
      m.ratioOfMountQuotaUsed = 0.0L;
    }
  }

  ret.voNameVoMap = std::move(voNameVoMap);

  const bool anyArchive = std::any_of(ret.potentialMounts.cbegin(), ret.potentialMounts.cend(), [](const auto& m) {
    return common::dataStructures::getMountBasicType(m.mount.type)
           == common::dataStructures::MountType::ArchiveAllTypes;
  });
  if (anyArchive) {
    ret.tapesForWriting = catalogue.Tape()->getTapesForWriting(logicalLibraryName);
  }

  auto t = ret.tapesForWriting.begin();
  while (t != ret.tapesForWriting.end()) {
    if (ret.tapesInUse.count(t->vid)) {
      t = ret.tapesForWriting.erase(t);
    } else {
      t++;
    }
  }

  return ret;
}

MountCandidate makeRetrieveCandidate(const CandidatePotential& potential,
                                     const std::optional<std::string>& blockedReason,
                                     const std::string& owner) {
  const auto& m = potential.mount;
  MountCandidate candidate;
  candidate.candidateKey = calculateRetrieveCandidateKey(m);
  candidate.mountType = m.type;
  candidate.logicalLibrary = m.logicalLibrary;
  candidate.tapePool = m.tapePool;
  candidate.vo = m.vo;
  candidate.vid = optionalNonEmpty(m.vid);
  candidate.priority = m.priority;
  candidate.minRequestAge = static_cast<uint64_t>(m.minRequestAge);
  candidate.filesQueued = m.filesQueued;
  candidate.bytesQueued = m.bytesQueued;
  candidate.oldestJobStartTime = static_cast<uint64_t>(m.oldestJobStartTime);
  candidate.candidateScore = calculateCandidateScore(m);
  candidate.state = blockedReason.has_value() ? "Blocked" : "Available";
  candidate.stateReason = blockedReason;
  candidate.createdBy = owner;
  return candidate;
}

MountCandidate makeArchiveCandidate(const CandidatePotential& potential,
                                    const catalogue::TapeForWriting* tape,
                                    const std::string& logicalLibrary,
                                    const std::optional<std::string>& blockedReason,
                                    const std::string& owner) {
  const auto& m = potential.mount;
  MountCandidate candidate;
  candidate.candidateKey =
    calculateArchiveCandidateKey(m, tape != nullptr ? optionalNonEmpty(tape->vid) : std::nullopt);
  candidate.mountType = m.type;
  candidate.logicalLibrary = logicalLibrary;
  candidate.tapePool = m.tapePool;
  candidate.vo = tape != nullptr ? tape->vo : m.vo;
  candidate.vid = tape != nullptr ? optionalNonEmpty(tape->vid) : std::nullopt;
  candidate.priority = m.priority;
  candidate.minRequestAge = static_cast<uint64_t>(m.minRequestAge);
  candidate.filesQueued = m.filesQueued;
  candidate.bytesQueued = m.bytesQueued;
  candidate.oldestJobStartTime = static_cast<uint64_t>(m.oldestJobStartTime);
  candidate.candidateScore = calculateCandidateScore(m);
  candidate.state = blockedReason.has_value() ? "Blocked" : "Available";
  candidate.stateReason = blockedReason;
  candidate.createdBy = owner;
  return candidate;
}

}  // namespace

MountDecision::MountDecision(ConnProvider& connectionProvider, SchedulerDatabase& schedulerDb, log::LogContext& lc)
    : m_schedulerDb(schedulerDb) {
  try {
    m_db = std::make_unique<MountDecisionDB>(connectionProvider);
  } catch (cta::exception::Exception& ex) {
    log::ScopedParamContainer params(lc);
    params.add(semconv::log::exceptionMessage, ex.getMessageValue());
    lc.log(log::ERR,
           "In MountDecision::MountDecision(): Failed to initialise Mount Decision DB. "
           "Continuing without Mount Decision DB.");
  } catch (std::exception& ex) {
    log::ScopedParamContainer params(lc);
    params.add(semconv::log::exceptionMessage, ex.what());
    lc.log(log::ERR,
           "In MountDecision::MountDecision(): Failed to initialise Mount Decision DB. "
           "Continuing without Mount Decision DB.");
  }
}

bool MountDecision::isAvailable() const {
  return static_cast<bool>(m_db);
}

bool MountDecision::incrementCounter(const std::string& key, log::LogContext& lc, std::string_view operation) {
  if (!m_db) {
    return false;
  }

  try {
    m_db->incrementCounter(key);
    return true;
  } catch (cta::exception::Exception& ex) {
    log::ScopedParamContainer params(lc);
    params.add("mountDecisionCounterKey", key)
      .add("mountDecisionOperation", std::string(operation))
      .add(semconv::log::exceptionMessage, ex.getMessageValue());
    lc.log(log::ERR, "In MountDecision::incrementCounter(): Failed to increment counter.");
  } catch (std::exception& ex) {
    log::ScopedParamContainer params(lc);
    params.add("mountDecisionCounterKey", key)
      .add("mountDecisionOperation", std::string(operation))
      .add(semconv::log::exceptionMessage, ex.what());
    lc.log(log::ERR, "In MountDecision::incrementCounter(): Failed to increment counter.");
  }
  return false;
}

bool MountDecision::refreshMountCandidates(const std::string& owner, Scheduler& scheduler, log::LogContext& lc) {
  if (!m_db) {
    return false;
  }

  const auto host = utils::getShortHostname();

  try {
    // One maintd keeps renewing this lease while it is alive. Another maintd
    // takes over only after the lease expires, so the routine interval remains
    // controlled by the selected maintd's configured cycle sleep.
    if (!m_db->tryAcquireRefreshLock(c_refreshWorkKey, host, c_refreshLockLeaseSeconds)) {
      lc.log(log::DEBUG, "In MountDecision::refreshMountCandidates(): Mount decision refresh lock is held elsewhere.");
      return false;
    }

    utils::Timer timer;
    auto mountInfo = m_schedulerDb.getMountInfoNoLock(SchedulerDatabase::PurposeGetMountInfo::GET_NEXT_MOUNT, lc);
    mountInfo->potentialMounts = aggregatePotentialMountsForMountCandidates(mountInfo->potentialMounts);
    scheduler.fillMountPolicyNamesForPotentialMounts(*mountInfo, lc);
    scheduler.getExistingAndNextMounts(*mountInfo, lc);

    std::vector<CandidateRow> candidateRows;

    auto& catalogue = scheduler.getCatalogue();
    for (const auto& logicalLibrary : getEnabledLogicalLibraries(catalogue)) {
      // The old getNextMount() logic evaluates one logical library at a time.
      // Keep that separation here, then merge all rows into a single ordered list for
      // taped processes to reserve from.
      // TODO: Physical resources are not shared between logical libraries.
      //       Therefore, an easy way to parallelize this algorithm is by splitting it by logical library.
      auto prepared = prepareMountCandidatesForLogicalLibrary(mountInfo->potentialMounts,
                                                              mountInfo->existingOrNextMounts,
                                                              logicalLibrary.name,
                                                              scheduler,
                                                              catalogue,
                                                              lc);
      auto simulatedExistingMountsPerTapepool = prepared.existingMountsDistinctTypeSummaryPerTapepool;
      auto simulatedExistingMountsPerVo = prepared.existingMountsBasicTypeSummaryPerVo;
      auto tapesForWriting = prepared.tapesForWriting;
      std::ranges::stable_sort(prepared.potentialMounts, candidateRefreshOrderLess);
      for (const auto& potential : prepared.potentialMounts) {
        std::optional<std::string> blockedReason = potential.blockedReason;
        const auto basicType = common::dataStructures::getMountBasicType(potential.mount.type);
        if (basicType == common::dataStructures::MountType::Retrieve) {
          if (!blockedReason.has_value() && prepared.tapesInUse.contains(potential.mount.vid)) {
            blockedReason = "Tape already in use";
          }
          candidateRows.push_back(CandidateRow {potential, logicalLibrary.name, std::nullopt, blockedReason});
        } else if (basicType == common::dataStructures::MountType::ArchiveAllTypes) {
          // Archive potentials do not name a VID.
          // We materialize as many concrete tape choices as the current simulated limits allow.
          // A blocked row is useful only if no available row was emitted for this potential mount,
          // otherwise taped already has something actionable.
          bool emittedAvailableCandidate = false;
          while (true) {
            auto slotBlockedReason = blockedReason;
            if (!slotBlockedReason.has_value()) {
              slotBlockedReason = getMountBlockReason(potential.mount,
                                                      simulatedExistingMountsPerTapepool,
                                                      simulatedExistingMountsPerVo,
                                                      prepared.voNameVoMap.at(potential.mount.vo));
            }

            if (slotBlockedReason.has_value()) {
              if (!emittedAvailableCandidate) {
                candidateRows.push_back(CandidateRow {potential, logicalLibrary.name, std::nullopt, slotBlockedReason});
              }
              break;
            }

            auto selectedTape = takeNextTapeForWriting(tapesForWriting, potential.mount.tapePool);
            if (!selectedTape.has_value()) {
              if (!emittedAvailableCandidate) {
                candidateRows.push_back(
                  CandidateRow {potential, logicalLibrary.name, std::nullopt, "No tape available for writing"});
              }
              break;
            }

            candidateRows.push_back(CandidateRow {potential, logicalLibrary.name, selectedTape, std::nullopt});
            emittedAvailableCandidate = true;
            incrementSimulatedMountCounts(potential.mount,
                                          simulatedExistingMountsPerTapepool,
                                          simulatedExistingMountsPerVo);
          }
        }
      }
    }

    auto expiredReservations = m_db->blockExpiredReservedMountCandidates(c_reservationTimeoutSeconds);
    if (!expiredReservations.empty()) {
      for (const auto& expiredReservation : expiredReservations) {
        log::ScopedParamContainer detailParams(lc);
        detailParams.add("mountDecisionCandidateId", expiredReservation.candidateId)
          .add("mountDecisionCandidateKey", expiredReservation.candidate.candidateKey)
          .add("tapeVid", expiredReservation.candidate.vid.value_or(""))
          .add("reservedByHost", expiredReservation.reservedByHost.value_or(""))
          .add("reservedByDrive", expiredReservation.reservedByDrive.value_or(""))
          .add("reservedTime", expiredReservation.reservedTime.value_or(0))
          .add("reservationHeartbeatTime", expiredReservation.reservationHeartbeatTime.value_or(0));
        lc.log(log::ERR,
               "In MountDecision::refreshMountCandidates(): Found expired mount candidate reservation. Mount candidate "
               "switched to blocked.");
      }
    }

    std::vector<MountCandidate> candidates;
    candidates.reserve(candidateRows.size());
    for (const auto& row : candidateRows) {
      auto blockedReason = row.blockedReason;
      const auto basicType = common::dataStructures::getMountBasicType(row.potential.mount.type);

      if (basicType == common::dataStructures::MountType::Retrieve) {
        auto candidate = makeRetrieveCandidate(row.potential, blockedReason, owner);
        candidates.push_back(std::move(candidate));
      } else if (basicType == common::dataStructures::MountType::ArchiveAllTypes) {
        auto candidate = makeArchiveCandidate(row.potential,
                                              row.tape.has_value() ? &row.tape.value() : nullptr,
                                              row.logicalLibrary,
                                              blockedReason,
                                              owner);
        candidates.push_back(std::move(candidate));
      }
    }

    m_db->replaceMountCandidates(candidates, c_reservationTimeoutSeconds);

    log::ScopedParamContainer params(lc);
    params.add("mountDecisionCandidates", candidates.size()).add("mountDecisionProcessingTime", timer.secs());
    lc.log(log::INFO, "In MountDecision::refreshMountCandidates(): Refreshed mount candidates.");
    return true;
  } catch (cta::exception::Exception& ex) {
    log::ScopedParamContainer params(lc);
    params.add(semconv::log::exceptionMessage, ex.getMessageValue());
    lc.log(log::ERR, "In MountDecision::refreshMountCandidates(): Failed to refresh mount candidates.");
  } catch (std::exception& ex) {
    log::ScopedParamContainer params(lc);
    params.add(semconv::log::exceptionMessage, ex.what());
    lc.log(log::ERR, "In MountDecision::refreshMountCandidates(): Failed to refresh mount candidates.");
  }

  return false;
}

std::optional<ReservedTapeMount> MountDecision::getNextMount(const std::string& logicalLibraryName,
                                                             const std::string& driveName,
                                                             Scheduler& scheduler,
                                                             log::LogContext& lc,
                                                             const uint64_t timeout_us) {
  if (!m_db) {
    return std::nullopt;
  }

  const auto host = utils::getShortHostname();

  // Check in advance that there is an available mount, before taking the scheduler lock.
  if (!m_db->hasAvailableMountCandidate(logicalLibraryName)) {
    lc.log(log::DEBUG, "In MountDecision::getNextMount(): No available mount candidate found.");
    return std::nullopt;
  }

  auto mountInfo = m_schedulerDb.getMountInfoLockOnly(logicalLibraryName, lc, timeout_us);

  while (true) {
    auto reservedCandidate = m_db->tryReserveNextMountCandidate(logicalLibraryName, host, driveName);
    if (!reservedCandidate.has_value()) {
      lc.log(log::DEBUG, "In MountDecision::getNextMount(): No available mount candidate found.");
      return std::nullopt;
    }

    try {
      const auto basicMountType = common::dataStructures::getMountBasicType(reservedCandidate->candidate.mountType);
      std::optional<std::string> blockedReason;
      std::optional<catalogue::TapeForWriting> tapeForWriting;
      std::optional<common::dataStructures::Tape> tape;
      std::optional<SchedulerDatabase::PotentialMount> potentialMount;
      if (basicMountType == common::dataStructures::MountType::ArchiveAllTypes) {
        if (!mountInfo->hasPendingArchiveJobsForMountDecision(reservedCandidate->candidate.tapePool,
                                                              reservedCandidate->candidate.mountType)) {
          blockedReason = "No pending archive jobs found for reserved candidate";
        }
        if (!blockedReason.has_value()) {
          tapeForWriting = getTapeForWritingForCandidate(reservedCandidate->candidate,
                                                         logicalLibraryName,
                                                         scheduler.getCatalogue(),
                                                         blockedReason);
          if (tapeForWriting.has_value()) {
            potentialMount =
              makePotentialMountForArchiveCandidate(reservedCandidate->candidate, tapeForWriting.value());
          }
        }
      } else if (reservedCandidate->candidate.mountType == common::dataStructures::MountType::Retrieve) {
        if (!mountInfo->hasPendingRetrieveJobsForMountDecision(requireValue(reservedCandidate->candidate.vid, "VID"),
                                                               reservedCandidate->candidate.vo)) {
          blockedReason = "No pending retrieve jobs found for reserved candidate";
        }
        if (!blockedReason.has_value()) {
          tape = getTapeForRetrieveCandidate(reservedCandidate->candidate,
                                             logicalLibraryName,
                                             scheduler.getCatalogue(),
                                             blockedReason);
          if (tape.has_value()) {
            potentialMount = makePotentialMountForRetrieveCandidate(reservedCandidate->candidate, tape.value());
          }
        }
      } else {
        throw exception::Exception("In MountDecision::getNextMount(): unexpected mount type.");
      }

      if (blockedReason.has_value()) {
        m_db->blockReservedMountCandidate(reservedCandidate->candidateId, host, driveName, blockedReason.value());

        log::ScopedParamContainer params(lc);
        params.add("mountDecisionCandidateId", reservedCandidate->candidateId)
          .add("tapeVid", reservedCandidate->candidate.vid.value_or(""))
          .add("mountType", common::dataStructures::toCamelCaseString(reservedCandidate->candidate.mountType))
          .add("mountDecisionCandidateScore", effectiveCandidateScore(reservedCandidate->candidate))
          .add("mountDecisionBlockedReason", blockedReason.value());
        lc.log(log::INFO, "In MountDecision::getNextMount(): Blocked stale reserved mount candidate.");
        continue;
      }
      if (!potentialMount.has_value()) {
        throw exception::Exception("In MountDecision::getNextMount(): failed to build potential mount from reserved "
                                   "candidate.");
      }

      ReservedTapeMount ret;
      ret.candidateId = reservedCandidate->candidateId;

      if (basicMountType == common::dataStructures::MountType::ArchiveAllTypes) {
        std::unique_ptr<ArchiveMount> archiveMount(new ArchiveMount(
          scheduler.getCatalogue(),
          mountInfo
            ->createArchiveMount(potentialMount.value(), tapeForWriting.value(), driveName, logicalLibraryName, host)));
        archiveMount->m_sessionRunning = true;
        ret.mount = std::move(archiveMount);
      } else if (reservedCandidate->candidate.mountType == common::dataStructures::MountType::Retrieve) {
        std::unique_ptr<RetrieveMount> retrieveMount(new RetrieveMount(
          scheduler.getCatalogue(),
          mountInfo->createRetrieveMount(potentialMount.value(), driveName, logicalLibraryName, host)));
        retrieveMount->m_sessionRunning = true;
        retrieveMount->m_diskRunning = true;
        retrieveMount->m_tapeRunning = true;
        ret.mount = std::move(retrieveMount);
      }

      log::ScopedParamContainer params(lc);
      params.add("mountDecisionCandidateId", reservedCandidate->candidateId)
        .add("tapeVid", ret.mount->getVid())
        .add("mountType", common::dataStructures::toCamelCaseString(ret.mount->getMountType()))
        .add("mountDecisionCandidateScore", effectiveCandidateScore(reservedCandidate->candidate));
      lc.log(log::INFO, "In MountDecision::getNextMount(): Reserved mount candidate.");
      return ret;
    } catch (...) {
      m_db->releaseMountCandidate(reservedCandidate->candidateId, host, driveName);
      throw;
    }
  }
}

void MountDecision::releaseMountCandidate(const uint64_t candidateId,
                                          const std::string& host,
                                          const std::string& drive,
                                          log::LogContext& lc) noexcept {
  if (!m_db) {
    return;
  }
  try {
    m_db->releaseMountCandidate(candidateId, host, drive);
  } catch (cta::exception::Exception& ex) {
    log::ScopedParamContainer params(lc);
    params.add("mountDecisionCandidateId", candidateId).add(semconv::log::exceptionMessage, ex.getMessageValue());
    lc.log(log::ERR, "In MountDecision::releaseMountCandidate(): Failed to release mount candidate.");
  } catch (std::exception& ex) {
    log::ScopedParamContainer params(lc);
    params.add("mountDecisionCandidateId", candidateId).add(semconv::log::exceptionMessage, ex.what());
    lc.log(log::ERR, "In MountDecision::releaseMountCandidate(): Failed to release mount candidate.");
  }
}

void MountDecision::heartbeatMountCandidate(const uint64_t candidateId,
                                            const std::string& host,
                                            const std::string& drive) noexcept {
  if (!m_db) {
    return;
  }
  try {
    m_db->heartbeatMountCandidate(candidateId, host, drive);
  } catch (...) {}
}

}  // namespace cta::mountdecision
