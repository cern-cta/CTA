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
  // Intermediate row before the final global ranking. Archive potentials can
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

uint64_t labelFormatValue(const std::optional<cta::common::dataStructures::Label::Format>& labelFormat) {
  return optionalLabelFormat(labelFormat)
    .value_or(static_cast<uint64_t>(static_cast<uint8_t>(cta::common::dataStructures::Label::Format::CTA)));
}

uint64_t labelFormatValue(cta::common::dataStructures::Label::Format labelFormat) {
  return static_cast<uint64_t>(static_cast<uint8_t>(labelFormat));
}

template<typename T>
T requireValue(const std::optional<T>& value, const std::string& fieldName) {
  if (!value.has_value()) {
    throw exception::Exception("In MountDecision::getNextMount(): reserved candidate is missing " + fieldName);
  }
  return value.value();
}

common::dataStructures::Label::Format labelFormatFromDbValue(const uint64_t value) {
  return static_cast<common::dataStructures::Label::Format>(static_cast<uint8_t>(value));
}

SchedulerDatabase::PotentialMount toPotentialMount(const MountCandidate& candidate) {
  SchedulerDatabase::PotentialMount ret;
  ret.type = candidate.mountType;
  ret.vid = requireValue(candidate.vid, "VID");
  ret.tapePool = candidate.tapePool;
  ret.vo = candidate.vo;
  ret.mediaType = candidate.mediaType;
  ret.vendor = candidate.vendor;
  ret.capacityInBytes = candidate.capacityInBytes;
  ret.labelFormat = labelFormatFromDbValue(candidate.labelFormat);
  ret.priority = candidate.priority;
  ret.minRequestAge = static_cast<time_t>(candidate.minRequestAge);
  ret.filesQueued = candidate.filesQueued;
  ret.bytesQueued = candidate.bytesQueued;
  ret.oldestJobStartTime = static_cast<time_t>(candidate.oldestJobStartTime);
  ret.youngestJobStartTime = static_cast<time_t>(candidate.youngestJobStartTime);
  ret.logicalLibrary = candidate.logicalLibrary;
  ret.ratioOfMountQuotaUsed = candidate.ratioOfMountQuotaUsed;
  ret.activity = candidate.activity;
  ret.encryptionKeyName = candidate.encryptionKeyName;
  return ret;
}

catalogue::TapeForWriting toTapeForWriting(const MountCandidate& candidate) {
  catalogue::TapeForWriting ret;
  ret.vid = requireValue(candidate.vid, "VID");
  ret.mediaType = candidate.mediaType;
  ret.vendor = candidate.vendor;
  ret.tapePool = candidate.tapePool;
  ret.vo = candidate.vo;
  ret.lastFSeq = requireValue(candidate.lastFSeq, "LAST_FSEQ");
  ret.capacityInBytes = candidate.capacityInBytes;
  ret.labelFormat = labelFormatFromDbValue(candidate.labelFormat);
  ret.encryptionKeyName = candidate.encryptionKeyName;
  return ret;
}

std::optional<std::string> validateArchiveCandidate(const MountCandidate& candidate,
                                                    const std::string& logicalLibraryName,
                                                    catalogue::Catalogue& catalogue) {
  const auto candidateVid = requireValue(candidate.vid, "VID");
  const auto tapesForWriting = catalogue.Tape()->getTapesForWriting(logicalLibraryName);
  const auto tapeIt = std::find_if(tapesForWriting.begin(), tapesForWriting.end(), [&candidateVid](const auto& tape) {
    return tape.vid == candidateVid;
  });
  if (tapeIt == tapesForWriting.end()) {
    return "Tape is no longer available for writing";
  }
  if (tapeIt->tapePool != candidate.tapePool) {
    return "Tape no longer belongs to the candidate tapepool";
  }
  return std::nullopt;
}

std::optional<std::string> validateRetrieveCandidate(const MountCandidate& candidate,
                                                     const std::string& logicalLibraryName,
                                                     catalogue::Catalogue& catalogue) {
  using Tape = common::dataStructures::Tape;

  const auto candidateVid = requireValue(candidate.vid, "VID");
  try {
    const auto tapes = catalogue.Tape()->getTapesByVid(candidateVid);
    const auto tapeIt = tapes.find(candidateVid);
    if (tapeIt == tapes.end()) {
      return "Tape no longer exists";
    }

    const auto& tape = tapeIt->second;
    if (tape.logicalLibraryName != logicalLibraryName) {
      return "Tape no longer belongs to the requested logical library";
    }
    if (tape.state != Tape::ACTIVE && tape.state != Tape::REPACKING) {
      return "Tape state is " + Tape::stateToString(tape.state);
    }
  } catch (const catalogue::TapeNotFound&) {
    return "Tape no longer exists";
  }
  return std::nullopt;
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
             "In MountDecision::sortAndGetTapesForMountInfo(): empty tape pool string found on potential mounts.");
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
             "In MountDecision::sortAndGetTapesForMountInfo(): empty tape pool string found on existing mount.");
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
             "In MountDecision::sortAndGetTapesForMountInfo(): existing repack mount found while there is no "
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
  candidate.vo = m.vo;
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
  candidate.mediaType = m.mediaType;
  candidate.labelFormat = labelFormatValue(m.labelFormat);
  candidate.vendor = m.vendor;
  candidate.capacityInBytes = m.capacityInBytes;
  candidate.lastFSeq = std::nullopt;
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
  candidate.vo = tape != nullptr ? tape->vo : m.vo;
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
  candidate.mediaType = tape != nullptr ? tape->mediaType : m.mediaType;
  candidate.labelFormat = tape != nullptr ? labelFormatValue(tape->labelFormat) : labelFormatValue(m.labelFormat);
  candidate.vendor = tape != nullptr ? tape->vendor : m.vendor;
  candidate.capacityInBytes = tape != nullptr ? tape->capacityInBytes : m.capacityInBytes;
  candidate.lastFSeq = tape != nullptr ? std::optional<uint64_t>(tape->lastFSeq) : std::nullopt;
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
    scheduler.fillMountPolicyNamesForPotentialMounts(*mountInfo, lc);
    scheduler.getExistingAndNextMounts(*mountInfo, lc);

    std::vector<CandidateRow> candidateRows;

    auto& catalogue = scheduler.getCatalogue();
    for (const auto& logicalLibrary : getEnabledLogicalLibraries(catalogue)) {
      // The old getNextMount() logic evaluates one logical library at a time.
      // Keep that separation here, then merge all rows into a single ranked list for
      // taped processes to reserve from.
      // TODO: Physical resources are not shared between logical libraries.
      //       Therefore, an easy way to parallelize this algorithm is by splitting it by logical library.
      auto prepared = sortAndGetTapesForMountInfo(mountInfo->potentialMounts,
                                                  mountInfo->existingOrNextMounts,
                                                  logicalLibrary.name,
                                                  scheduler,
                                                  catalogue,
                                                  lc);
      auto simulatedExistingMountsPerTapepool = prepared.existingMountsDistinctTypeSummaryPerTapepool;
      auto simulatedExistingMountsPerVo = prepared.existingMountsBasicTypeSummaryPerVo;
      auto tapesForWriting = prepared.tapesForWriting;
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
        // A single VID can appear through multiple potential rows, especially
        // when retrieve and archive opportunities compete. Keep the lower
        // ranked row visible for diagnostics, but prevent taped from reserving
        // the same tape twice.
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

std::optional<ReservedTapeMount> MountDecision::getNextMount(const std::string& logicalLibraryName,
                                                             const std::string& driveName,
                                                             Scheduler& scheduler,
                                                             log::LogContext& lc,
                                                             const uint64_t timeout_us) {
  if (!m_db) {
    return std::nullopt;
  }

  const auto host = utils::getShortHostname();

  // Check in advance that there is an available mount, before fetching the mount info data.
  if (!m_db->hasAvailableMountCandidate(logicalLibraryName)) {
    lc.log(log::DEBUG, "In MountDecision::getNextMount(): No available mount candidate found.");
    return std::nullopt;
  }

  auto mountInfo = m_schedulerDb.getMountInfo(std::optional<std::string_view>(logicalLibraryName), lc, timeout_us);

  while (true) {
    auto reservedCandidate = m_db->tryReserveNextMountCandidate(logicalLibraryName, host, driveName);
    if (!reservedCandidate.has_value()) {
      lc.log(log::DEBUG, "In MountDecision::getNextMount(): No available mount candidate found.");
      return std::nullopt;
    }

    try {
      auto potentialMount = toPotentialMount(reservedCandidate->candidate);

      const auto basicMountType = common::dataStructures::getMountBasicType(potentialMount.type);
      std::optional<std::string> blockedReason;
      if (basicMountType == common::dataStructures::MountType::ArchiveAllTypes) {
        blockedReason =
          validateArchiveCandidate(reservedCandidate->candidate, logicalLibraryName, scheduler.getCatalogue());
      } else if (potentialMount.type == common::dataStructures::MountType::Retrieve) {
        blockedReason =
          validateRetrieveCandidate(reservedCandidate->candidate, logicalLibraryName, scheduler.getCatalogue());
      } else {
        throw exception::Exception("In MountDecision::getNextMount(): unexpected mount type.");
      }

      if (blockedReason.has_value()) {
        m_db->blockReservedMountCandidate(reservedCandidate->candidateId, host, driveName, blockedReason.value());

        log::ScopedParamContainer params(lc);
        params.add("mountDecisionCandidateId", reservedCandidate->candidateId)
          .add("tapeVid", reservedCandidate->candidate.vid.value_or(""))
          .add("mountType", common::dataStructures::toCamelCaseString(reservedCandidate->candidate.mountType))
          .add("mountDecisionCandidateRank", reservedCandidate->candidate.candidateRank)
          .add("mountDecisionBlockedReason", blockedReason.value());
        lc.log(log::INFO, "In MountDecision::getNextMount(): Blocked stale reserved mount candidate.");
        continue;
      }

      ReservedTapeMount ret;
      ret.candidateId = reservedCandidate->candidateId;

      if (basicMountType == common::dataStructures::MountType::ArchiveAllTypes) {
        auto tape = toTapeForWriting(reservedCandidate->candidate);
        std::unique_ptr<ArchiveMount> archiveMount(
          new ArchiveMount(scheduler.getCatalogue(),
                           mountInfo->createArchiveMount(potentialMount, tape, driveName, logicalLibraryName, host)));
        archiveMount->m_sessionRunning = true;
        ret.mount = std::move(archiveMount);
      } else if (potentialMount.type == common::dataStructures::MountType::Retrieve) {
        std::unique_ptr<RetrieveMount> retrieveMount(
          new RetrieveMount(scheduler.getCatalogue(),
                            mountInfo->createRetrieveMount(potentialMount, driveName, logicalLibraryName, host)));
        retrieveMount->m_sessionRunning = true;
        retrieveMount->m_diskRunning = true;
        retrieveMount->m_tapeRunning = true;
        ret.mount = std::move(retrieveMount);
      }

      log::ScopedParamContainer params(lc);
      params.add("mountDecisionCandidateId", reservedCandidate->candidateId)
        .add("tapeVid", ret.mount->getVid())
        .add("mountType", common::dataStructures::toCamelCaseString(ret.mount->getMountType()))
        .add("mountDecisionCandidateRank", reservedCandidate->candidate.candidateRank);
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
