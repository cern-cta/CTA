/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#pragma once

#include "scheduler/rdbms/RelationalDB.hpp"
#include "scheduler/rdbms/postgres/Transaction.hpp"
#include "scheduler/rdbms/postgres/RepackJobQueue.hpp"
#include "common/dataStructures/ArchiveRoute.hpp"
#include "common/log/LogContext.hpp"
#include "disk/DiskSystem.hpp"

#include <list>
#include <cstdint>

namespace cta::schedulerdb {

class RepackRequest : public SchedulerDatabase::RepackRequest {
  friend class cta::RelationalDB;

public:
  RepackRequest(rdbms::Conn& conn, catalogue::Catalogue& catalogue, log::LogContext& lc)
      : m_conn(conn),
        m_catalogue(catalogue),
        m_lc(lc) {}

  RepackRequest(rdbms::Conn& conn,
                catalogue::Catalogue& catalogue,
                log::LogContext& lc,
                const schedulerdb::postgres::RepackJobQueueRow& row)
      : m_conn(conn),
        m_catalogue(catalogue),
        m_lc(lc) {
    *this = row;
  }

  RepackRequest& operator=(const schedulerdb::postgres::RepackJobQueueRow& row);

  uint64_t getLastExpandedFSeq() override;
  void setLastExpandedFSeq(uint64_t fseq) override;

  uint64_t addSubrequestsAndUpdateStats(std::list<Subrequest>& repackSubrequests,
                                        cta::common::dataStructures::ArchiveRoute::FullMap& archiveRoutesMap,
                                        uint64_t maxFSeqLowBound,
                                        const uint64_t maxAddedFSeq,
                                        const TotalStatsFiles& totalStatsFiles,
                                        disk::DiskSystemList diskSystemList,
                                        log::LogContext& lc) override;

  void expandDone() override;
  void fail() override;
  void requeueInToExpandQueue(log::LogContext& lc) override;
  void setExpandStartedAndChangeStatus() override;
  void fillLastExpandedFSeqAndTotalStatsFile(uint64_t& fSeq, TotalStatsFiles& totalStatsFiles) override;

  void setVid(const std::string& vid);
  void setType(common::dataStructures::RepackInfo::Type repackType);
  void setBufferURL(const std::string& bufferURL);
  void setMountPolicy(const common::dataStructures::MountPolicy& mp);
  void setNoRecall(const bool noRecall);
  void setCreationLog(const common::dataStructures::EntryLog& creationLog);

  std::string getIdStr() const { return "??"; }

  void setTotalStats(const cta::SchedulerDatabase::RepackRequest::TotalStatsFiles& totalStatsFiles);
  [[noreturn]] void
  reportRetrieveCreationFailures([[maybe_unused]] const std::list<Subrequest>& notCreatedSubrequests) const;

  void commit();
  void insert();
  void update() const;

  common::dataStructures::MountPolicy m_mountPolicy;
  bool m_noRecall = false;
  common::dataStructures::EntryLog m_creationLog;
  bool m_addCopies = true;
  bool m_isMove = true;

  struct StatsValues {
    uint64_t files = 0;
    uint64_t bytes = 0;
  };

  struct SubrequestPointer {
    std::string address;
    uint64_t fSeq = 0;
    bool isRetrieveAccounted = false;
    std::set<uint32_t> archiveCopyNbAccounted;
    bool isSubreqDeleted = false;
  };

  std::list<SubrequestPointer> m_subreqp;
  std::unique_ptr<schedulerdb::Transaction> m_txn;

  // References to external objects
  //rdbms::ConnPool      &m_connPool;
  rdbms::Conn& m_conn;
  catalogue::Catalogue& m_catalogue;
  log::LogContext& m_lc;
};

}  // namespace cta::schedulerdb
