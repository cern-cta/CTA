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
#include "scheduler/Scheduler.hpp"
#include "scheduler/SchedulerDatabase.hpp"

#include <algorithm>
#include <chrono>
#include <exception>
#include <list>
#include <map>
#include <optional>
#include <set>
#include <sys/types.h>
#include <unistd.h>
#include <utility>
#include <vector>

namespace cta::mountdecision {

namespace {

constexpr uint64_t c_minFilesToWarrantAMount = 5;
constexpr uint64_t c_minBytesToWarrantAMount = 2000000;
constexpr uint64_t c_refreshLockLeaseSeconds = 300;
constexpr uint64_t c_reservationTimeoutSeconds = 300;
const std::string c_refreshWorkKey = "mount-candidate-refresh";

class RefreshLockGuard {
public:
  RefreshLockGuard(MountDecisionDB& db,
                   std::string workKey,
                   std::string host,
                   std::optional<uint64_t> pid,
                   const bool ownsLock)
      : m_db(db),
        m_workKey(std::move(workKey)),
        m_host(std::move(host)),
        m_pid(pid),
        m_ownsLock(ownsLock) {}

  RefreshLockGuard(const RefreshLockGuard&) = delete;
  RefreshLockGuard& operator=(const RefreshLockGuard&) = delete;

  ~RefreshLockGuard() {
    if (!m_ownsLock) {
      return;
    }
    try {
      m_db.releaseRefreshLock(m_workKey, m_host, m_pid);
    } catch (...) {}
  }

  bool ownsLock() const { return m_ownsLock; }

private:
  MountDecisionDB& m_db;
  std::string m_workKey;
  std::string m_host;
  std::optional<uint64_t> m_pid;
  bool m_ownsLock = false;
};

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
};

struct CandidateRow {
  CandidatePotential potential;
  std::string logicalLibrary;
  std::optional<catalogue::TapeForWriting> tape;
  std::optional<std::string> blockedReason;
};

bool candidateLess(const CandidatePotential& lhs, const CandidatePotential& rhs) {
  return lhs.mount < rhs.mount;
}

bool candidateRowLess(const CandidateRow& lhs, const CandidateRow& rhs) {
  if (lhs.potential.mount < rhs.potential.mount) {
    return true;
  }
  if (rhs.potential.mount < lhs.potential.mount) {
    return false;
  }
  if (lhs.logicalLibrary < rhs.logicalLibrary) {
    return false;
  }
  if (lhs.logicalLibrary > rhs.logicalLibrary) {
    return true;
  }
  const auto lhsVid = lhs.tape.has_value() ? lhs.tape->vid : lhs.potential.mount.vid;
  const auto rhsVid = rhs.tape.has_value() ? rhs.tape->vid : rhs.potential.mount.vid;
  return lhsVid > rhsVid;
}

std::optional<std::string> optionalNonEmpty(const std::string& value) {
  if (value.empty()) {
    return std::nullopt;
  }
  return value;
}

std::optional<uint64_t>
optionalLabelFormat(const std::optional<cta::common::dataStructures::Label::Format>& labelFormat) {
  if (!labelFormat.has_value()) {
    return std::nullopt;
  }
  return static_cast<uint64_t>(static_cast<uint8_t>(labelFormat.value()));
}

std::optional<uint64_t> optionalLabelFormat(cta::common::dataStructures::Label::Format labelFormat) {
  return static_cast<uint64_t>(static_cast<uint8_t>(labelFormat));
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
MountInfoForDecision sortAndGetTapesForMountInfo(const std::vector<SchedulerDatabase::PotentialMount>& potentialMounts,
                                                 const std::vector<SchedulerDatabase::ExistingMount>& existingMounts,
                                                 const std::string& logicalLibraryName,
                                                 Scheduler& scheduler,
                                                 catalogue::Catalogue& catalogue,
                                                 log::LogContext& lc) {
  utils::Timer timer;
  MountInfoForDecision ret;
  auto workingPotentialMounts = potentialMounts;
  auto workingExistingMounts = existingMounts;

  bool anyRetr;
  {
    auto& v = workingPotentialMounts;
    anyRetr = std::any_of(v.begin(), v.end(), [](const SchedulerDatabase::PotentialMount& pm) {
      return pm.type == common::dataStructures::MountType::Retrieve;
    });
  }

  std::set<std::string> repackingTapeVids;
  if (anyRetr) {
    std::map<std::string, common::dataStructures::Tape, std::less<>> eligibleTapeMap;
    {
      catalogue::TapeSearchCriteria searchCriteria;
      searchCriteria.logicalLibrary = logicalLibraryName;
      searchCriteria.state = common::dataStructures::Tape::ACTIVE;
      auto eligibleTapesList = catalogue.Tape()->getTapes(searchCriteria);
      for (auto& t : eligibleTapesList) {
        eligibleTapeMap[t.vid] = t;
      }
      searchCriteria.state = common::dataStructures::Tape::REPACKING;
      searchCriteria.logicalLibrary = std::nullopt;
      eligibleTapesList = catalogue.Tape()->getTapes(searchCriteria);
      for (auto& t : eligibleTapesList) {
        if (t.logicalLibraryName == logicalLibraryName) {
          eligibleTapeMap[t.vid] = t;
        }

        repackingTapeVids.insert(t.vid);
      }
    }

    auto& v = workingPotentialMounts;
    v.erase(std::remove_if(v.begin(),
                           v.end(),
                           [&eligibleTapeMap](const SchedulerDatabase::PotentialMount& pm) {
                             return (pm.type == common::dataStructures::MountType::Retrieve
                                     && !eligibleTapeMap.contains(pm.vid));
                           }),
            v.end());

    static_cast<void>(timer.secs(utils::Timer::resetCounter));
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
    std::set<std::string> vidsWithEmptyTapePoolString;
    workingPotentialMounts.erase(
      std::remove_if(workingPotentialMounts.begin(),
                     workingPotentialMounts.end(),
                     [&vidsWithEmptyTapePoolString](const SchedulerDatabase::PotentialMount& mount) {
                       if (mount.tapePool.empty()) {
                         vidsWithEmptyTapePoolString.insert(mount.vid);
                         return true;
                       }
                       return false;
                     }),
      workingPotentialMounts.end());
    for (const auto& vid : vidsWithEmptyTapePoolString) {
      log::ScopedParamContainer params(lc);
      params.add("tapeVid", vid);
      lc.log(log::ERR,
             "In MountDecision::sortAndGetTapesForMountInfo(): empty tape pool string found on potential mounts.");
    }
  }

  {
    std::set<std::string> drivesWithEmptyTapePoolString;
    workingExistingMounts.erase(
      std::remove_if(workingExistingMounts.begin(),
                     workingExistingMounts.end(),
                     [&drivesWithEmptyTapePoolString](const SchedulerDatabase::ExistingMount& mount) {
                       if (mount.tapePool.empty()) {
                         drivesWithEmptyTapePoolString.insert(mount.driveName);
                         return true;
                       }
                       return false;
                     }),
      workingExistingMounts.end());
    for (const auto& drive : drivesWithEmptyTapePoolString) {
      log::ScopedParamContainer params(lc);
      params.add("drive", drive);
      lc.log(log::ERR,
             "In MountDecision::sortAndGetTapesForMountInfo(): empty tape pool string found on existing mount.");
    }

    auto schedulerBackendName = scheduler.getSchedulerBackendName();
    std::list<std::string> ignoredDrives;
    workingExistingMounts.erase(
      std::remove_if(workingExistingMounts.begin(),
                     workingExistingMounts.end(),
                     [&ignoredDrives, &schedulerBackendName](const SchedulerDatabase::ExistingMount& mount) {
                       if (!mount.schedulerBackendName.has_value()) {
                         ignoredDrives.emplace_back(mount.driveName);
                         return true;
                       }
                       return mount.schedulerBackendName != schedulerBackendName;
                     }),
      workingExistingMounts.end());
    for (const auto& ignoredDriveName : ignoredDrives) {
      log::ScopedParamContainer params(lc);
      params.add("ignoredDrive", ignoredDriveName);
      lc.log(log::ERR,
             "In MountDecision::sortAndGetTapesForMountInfo(): found a drive without SchedulerBackendName "
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
  std::map<std::string, common::dataStructures::VirtualOrganization> voNameVoMap;
  for (auto& tapepool : tapepoolsPotentialOrExistingMounts) {
    try {
      auto vo = catalogue.VO()->getCachedVirtualOrganizationOfTapepool(tapepool);
      tapepoolVoNameMap[tapepool] = vo.name;
      voNameVoMap[vo.name] = vo;
    } catch (cta::exception::Exception& ex) {
      ex.getMessage() << " Aborting mount decision." << std::endl;
      throw ex;
    }
  }

  std::optional<common::dataStructures::VirtualOrganization> defaultRepackVo =
    catalogue.VO()->getDefaultVirtualOrganizationForRepack();
  if (defaultRepackVo.has_value()) {
    voNameVoMap[defaultRepackVo->name] = defaultRepackVo.value();
  }

  ExistingMountSummaryPerTapepool existingMountsDistinctTypeSummaryPerTapepool;
  ExistingMountSummaryPerVo existingMountsBasicTypeSummaryPerVo;
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
             "In MountDecision::sortAndGetTapesForMountInfo(): existing repack mount found while there is no "
             "default repack VO defined.");
      continue;
    }
    auto voName = isRepackingMount ? defaultRepackVo->name : tapepoolVoNameMap.at(em.tapePool);
    existingMountsDistinctTypeSummaryPerTapepool[TapePoolMountPair(em.tapePool, em.type)].totalMounts++;
    existingMountsBasicTypeSummaryPerVo
      [VirtualOrganizationMountPair(voName, common::dataStructures::getMountBasicType(em.type))]
        .totalMounts++;
    if (em.activity) {
      existingMountsDistinctTypeSummaryPerTapepool[TapePoolMountPair(em.tapePool, em.type)]
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
    uint32_t existingMountsDistinctTypesForThisTapepool = 0;
    uint32_t existingMountsBasicTypeForThisVo = 0;
    common::dataStructures::MountType basicTypeOfThisPotentialMount = common::dataStructures::getMountBasicType(m.type);
    common::dataStructures::VirtualOrganization voOfThisPotentialMount = voNameVoMap.at(m.vo);
    try {
      existingMountsDistinctTypesForThisTapepool =
        existingMountsDistinctTypeSummaryPerTapepool.at(TapePoolMountPair(m.tapePool, m.type)).totalMounts;
    } catch (std::out_of_range&) {}
    try {
      existingMountsBasicTypeForThisVo =
        existingMountsBasicTypeSummaryPerVo
          .at(VirtualOrganizationMountPair(voOfThisPotentialMount.name, basicTypeOfThisPotentialMount))
          .totalMounts;
    } catch (std::out_of_range&) {}

    uint32_t effectiveExistingMountsForThisTapepool = 0;
    if (basicTypeOfThisPotentialMount == common::dataStructures::MountType::ArchiveAllTypes) {
      effectiveExistingMountsForThisTapepool = existingMountsDistinctTypesForThisTapepool;
    }
    bool mountPassesACriteria = false;
    uint64_t minBytesToWarrantAMount = c_minBytesToWarrantAMount;
    uint64_t minFilesToWarrantAMount = c_minFilesToWarrantAMount;
    if (m.type == common::dataStructures::MountType::ArchiveForRepack) {
      minBytesToWarrantAMount *= 2;
      minFilesToWarrantAMount *= 2;
    }
    if (m.bytesQueued / (1 + effectiveExistingMountsForThisTapepool) >= minBytesToWarrantAMount) {
      mountPassesACriteria = true;
    }
    if (m.filesQueued / (1 + effectiveExistingMountsForThisTapepool) >= minFilesToWarrantAMount) {
      mountPassesACriteria = true;
    }
    if (!effectiveExistingMountsForThisTapepool
        && ((std::chrono::system_clock::to_time_t(std::chrono::system_clock::now()) - m.oldestJobStartTime)
            > m.minRequestAge)) {
      mountPassesACriteria = true;
    }

    uint64_t maxDrives = 0;
    if (basicTypeOfThisPotentialMount == common::dataStructures::MountType::Retrieve) {
      maxDrives = voOfThisPotentialMount.readMaxDrives;
    } else if (basicTypeOfThisPotentialMount == common::dataStructures::MountType::ArchiveAllTypes) {
      maxDrives = voOfThisPotentialMount.writeMaxDrives;
    }

    if (!mountPassesACriteria) {
      candidate.blockedReason = "Mount criteria not met";
    } else if (existingMountsBasicTypeForThisVo >= maxDrives) {
      candidate.blockedReason = "VO mount drive limit reached";
    } else if (m.sleepingMount) {
      candidate.blockedReason = "Sleeping mount";
    } else {
      m.ratioOfMountQuotaUsed = 0.0L;
    }
  }

  std::sort(ret.potentialMounts.begin(), ret.potentialMounts.end(), candidateLess);
  std::reverse(ret.potentialMounts.begin(), ret.potentialMounts.end());

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
                                     uint64_t rank,
                                     const std::optional<std::string>& blockedReason,
                                     const std::string& owner) {
  const auto& m = potential.mount;
  MountCandidate candidate;
  candidate.mountType = m.type;
  candidate.logicalLibrary = m.logicalLibrary;
  candidate.tapePool = m.tapePool;
  candidate.vo = optionalNonEmpty(m.vo);
  candidate.vid = optionalNonEmpty(m.vid);
  candidate.activity = m.activity;
  candidate.priority = m.priority;
  candidate.minRequestAge = static_cast<uint64_t>(m.minRequestAge);
  candidate.filesQueued = m.filesQueued;
  candidate.bytesQueued = m.bytesQueued;
  candidate.oldestJobStartTime = static_cast<uint64_t>(m.oldestJobStartTime);
  candidate.youngestJobStartTime = static_cast<uint64_t>(m.youngestJobStartTime);
  candidate.ratioOfMountQuotaUsed = m.ratioOfMountQuotaUsed;
  candidate.candidateRank = rank;
  candidate.mediaType = optionalNonEmpty(m.mediaType);
  candidate.labelFormat = optionalLabelFormat(m.labelFormat);
  candidate.vendor = optionalNonEmpty(m.vendor);
  candidate.capacityInBytes = m.capacityInBytes;
  candidate.encryptionKeyName = m.encryptionKeyName;
  candidate.state = blockedReason.has_value() ? "Blocked" : "Available";
  candidate.stateReason = blockedReason;
  candidate.createdBy = owner;
  return candidate;
}

MountCandidate makeArchiveCandidate(const CandidatePotential& potential,
                                    const catalogue::TapeForWriting* tape,
                                    const std::string& logicalLibrary,
                                    uint64_t rank,
                                    const std::optional<std::string>& blockedReason,
                                    const std::string& owner) {
  const auto& m = potential.mount;
  MountCandidate candidate;
  candidate.mountType = m.type;
  candidate.logicalLibrary = logicalLibrary;
  candidate.tapePool = m.tapePool;
  candidate.vo = tape != nullptr ? optionalNonEmpty(tape->vo) : optionalNonEmpty(m.vo);
  candidate.vid = tape != nullptr ? optionalNonEmpty(tape->vid) : std::nullopt;
  candidate.activity = m.activity;
  candidate.priority = m.priority;
  candidate.minRequestAge = static_cast<uint64_t>(m.minRequestAge);
  candidate.filesQueued = m.filesQueued;
  candidate.bytesQueued = m.bytesQueued;
  candidate.oldestJobStartTime = static_cast<uint64_t>(m.oldestJobStartTime);
  candidate.youngestJobStartTime = static_cast<uint64_t>(m.youngestJobStartTime);
  candidate.ratioOfMountQuotaUsed = m.ratioOfMountQuotaUsed;
  candidate.candidateRank = rank;
  candidate.mediaType = tape != nullptr ? optionalNonEmpty(tape->mediaType) : optionalNonEmpty(m.mediaType);
  candidate.labelFormat = tape != nullptr ? optionalLabelFormat(tape->labelFormat) : optionalLabelFormat(m.labelFormat);
  candidate.vendor = tape != nullptr ? optionalNonEmpty(tape->vendor) : optionalNonEmpty(m.vendor);
  candidate.capacityInBytes = tape != nullptr ? tape->capacityInBytes : m.capacityInBytes;
  candidate.encryptionKeyName = tape != nullptr ? tape->encryptionKeyName : m.encryptionKeyName;
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
  const auto pid = static_cast<uint64_t>(getpid());

  try {
    const bool lockAcquired = m_db->tryAcquireRefreshLock(c_refreshWorkKey, host, pid, c_refreshLockLeaseSeconds);
    RefreshLockGuard refreshLock(*m_db, c_refreshWorkKey, host, pid, lockAcquired);
    if (!refreshLock.ownsLock()) {
      lc.log(log::DEBUG, "In MountDecision::refreshMountCandidates(): Mount decision refresh lock is held elsewhere.");
      return false;
    }

    utils::Timer timer;
    auto mountInfo = m_schedulerDb.getMountInfoNoLock(SchedulerDatabase::PurposeGetMountInfo::GET_NEXT_MOUNT, lc);
    scheduler.fillMountPolicyNamesForPotentialMounts(*mountInfo, lc);
    scheduler.getExistingAndNextMounts(*mountInfo, lc);

    const auto basePotentialMounts = mountInfo->potentialMounts;
    const auto baseExistingMounts = mountInfo->existingOrNextMounts;
    std::vector<CandidateRow> candidateRows;

    auto& catalogue = scheduler.getCatalogue();
    for (const auto& logicalLibrary : getEnabledLogicalLibraries(catalogue)) {
      auto prepared = sortAndGetTapesForMountInfo(basePotentialMounts,
                                                  baseExistingMounts,
                                                  logicalLibrary.name,
                                                  scheduler,
                                                  catalogue,
                                                  lc);
      for (const auto& potential : prepared.potentialMounts) {
        std::optional<std::string> blockedReason = potential.blockedReason;
        const auto basicType = common::dataStructures::getMountBasicType(potential.mount.type);
        if (basicType == common::dataStructures::MountType::Retrieve) {
          if (!blockedReason.has_value() && prepared.tapesInUse.contains(potential.mount.vid)) {
            blockedReason = "Tape already in use";
          }
          candidateRows.push_back(CandidateRow {potential, logicalLibrary.name, std::nullopt, blockedReason});
        } else if (basicType == common::dataStructures::MountType::ArchiveAllTypes) {
          const catalogue::TapeForWriting* selectedTape = nullptr;
          for (const auto& tape : prepared.tapesForWriting) {
            if (tape.tapePool == potential.mount.tapePool) {
              selectedTape = &tape;
              break;
            }
          }

          if (!blockedReason.has_value() && selectedTape == nullptr) {
            blockedReason = "No tape available for writing";
          }
          candidateRows.push_back(CandidateRow {
            potential,
            logicalLibrary.name,
            selectedTape != nullptr ? std::optional<catalogue::TapeForWriting>(*selectedTape) : std::nullopt,
            blockedReason});
        }
      }
    }

    std::sort(candidateRows.begin(), candidateRows.end(), candidateRowLess);
    std::reverse(candidateRows.begin(), candidateRows.end());

    std::vector<MountCandidate> candidates;
    candidates.reserve(candidateRows.size());
    std::set<std::string, std::less<>> selectedVids;
    uint64_t candidateRank = 1;
    for (const auto& row : candidateRows) {
      auto blockedReason = row.blockedReason;
      const auto basicType = common::dataStructures::getMountBasicType(row.potential.mount.type);
      std::optional<std::string> candidateVid;
      if (basicType == common::dataStructures::MountType::Retrieve) {
        candidateVid = optionalNonEmpty(row.potential.mount.vid);
      } else if (row.tape.has_value()) {
        candidateVid = optionalNonEmpty(row.tape->vid);
      }

      if (!blockedReason.has_value() && candidateVid.has_value() && selectedVids.contains(candidateVid.value())) {
        blockedReason = "Tape already selected by higher-ranked candidate";
      }

      if (basicType == common::dataStructures::MountType::Retrieve) {
        auto candidate = makeRetrieveCandidate(row.potential, candidateRank++, blockedReason, owner);
        if (!blockedReason.has_value() && candidate.vid.has_value()) {
          selectedVids.insert(candidate.vid.value());
        }
        candidates.push_back(std::move(candidate));
      } else if (basicType == common::dataStructures::MountType::ArchiveAllTypes) {
        auto candidate = makeArchiveCandidate(row.potential,
                                              row.tape.has_value() ? &row.tape.value() : nullptr,
                                              row.logicalLibrary,
                                              candidateRank++,
                                              blockedReason,
                                              owner);
        if (!blockedReason.has_value() && candidate.vid.has_value()) {
          selectedVids.insert(candidate.vid.value());
        }
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

}  // namespace cta::mountdecision
