/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/dataStructures/ArchiveRoute.hpp"
#include "common/log/LogContext.hpp"
#include "disk/DiskSystem.hpp"
#include "scheduler/rdbms/RelationalDB.hpp"
#include "scheduler/rdbms/postgres/RepackRequestTracker.hpp"
#include "scheduler/rdbms/postgres/Transaction.hpp"

#include <cstdint>
#include <list>

namespace cta::schedulerdb {

class RepackRequest : public SchedulerDatabase::RepackRequest {
  friend class cta::RelationalDB;

public:
  RepackRequest(rdbms::ConnPool& connPool, catalogue::Catalogue& catalogue, log::LogContext& lc)
      : m_connPool(connPool),
        m_catalogue(catalogue),
        m_lc(lc) {}

  RepackRequest(rdbms::ConnPool& connPool,
                catalogue::Catalogue& catalogue,
                log::LogContext& lc,
                const schedulerdb::postgres::RepackRequestTrackingRow& row)
      : m_connPool(connPool),
        m_catalogue(catalogue),
        m_lc(lc) {
    *this = row;
  }
  friend class RelationalDB;

  RepackRequest& operator=(const schedulerdb::postgres::RepackRequestTrackingRow& row);
  uint64_t getLastExpandedFSeq() override;
  void setLastExpandedFSeq(uint64_t fseq) override;

  uint64_t addSubrequestsAndUpdateStats(const std::list<Subrequest>& repackSubrequests,
                                        const cta::common::dataStructures::ArchiveRoute::FullMap& archiveRoutesMap,
                                        uint64_t maxFSeqLowBound,
                                        const uint64_t maxAddedFSeq,
                                        const TotalStatsFiles& totalStatsFiles,
                                        const disk::DiskSystemList& diskSystemList,
                                        log::LogContext& lc) override;

  cta::SchedulerDatabase::RepackRequest::TotalStatsFiles getTotalStatsFile();
  void expandDone() override;
  void fail() override;
  void requeueInToExpandQueue(log::LogContext& lc) override;
  common::dataStructures::RepackInfo::Status getCurrentStatus() const;
  void setExpandStartedAndChangeStatus() override;
  void fillLastExpandedFSeqAndTotalStatsFile(uint64_t& fSeq, TotalStatsFiles& totalStatsFiles) override;

  void setVid(const std::string& vid);
  void setType(common::dataStructures::RepackInfo::Type repackType);
  void setBufferURL(const std::string& bufferURL);
  void setMountPolicy(const common::dataStructures::MountPolicy& mp);
  void setNoRecall(const bool noRecall);
  void setCreationLog(const common::dataStructures::EntryLog& creationLog);
  void setMaxFilesToSelect(const uint64_t maxFilesToSelect);

  std::string getIdStr() const { return "??"; }

  void setTotalStats(const cta::SchedulerDatabase::RepackRequest::TotalStatsFiles& totalStatsFiles);

  void reportRetrieveCreationFailures(const uint64_t failedToRetrieveFiles,
                                      const uint64_t failedToRetrieveBytes,
                                      const uint64_t failedArchiveReq);

  void insert();

  void assignJobStatusToRepackInfoStatus(const RepackJobStatus& dbStatus);

  RepackJobStatus mapRepackInfoStatusToJobStatus(const common::dataStructures::RepackInfo::Status& infoStatus);

  common::dataStructures::MountPolicy m_mountPolicy;
  bool m_noRecall = false;
  common::dataStructures::EntryLog m_creationLog;
  bool m_addCopies = true;
  bool m_isMove = true;
  bool m_isComplete = false;
  uint64_t m_maxFilesToSelect = 0;
  uint64_t m_failedToCreateArchiveReq = 0;

  struct StatsValues {
    uint64_t files = 0;
    uint64_t bytes = 0;
  };

  struct SubrequestPointer {
    uint64_t fSeq = 0;
    bool isRetrieveAccounted = false;
    std::set<uint32_t> archiveCopyNbsSet;
    bool isSubreqDeleted = false;
  };

  std::list<SubrequestPointer> m_subreqp;
  // References to external objects
  //rdbms::ConnPool      &m_connPool;
  rdbms::ConnPool& m_connPool;
  catalogue::Catalogue& m_catalogue;
  log::LogContext m_lc;
};

}  // namespace cta::schedulerdb
