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

#include "PostgresSchedDB.hpp"
#include "common/dataStructures/ArchiveRoute.hpp"
#include "common/log/LogContext.hpp"
#include "disk/DiskSystem.hpp"

#include <list>
#include <cstdint>

namespace cta {

class PostgresSchedDB::RepackRequest : public SchedulerDatabase::RepackRequest {
 public:

   RepackRequest();

   void setLastExpandedFSeq(uint64_t fseq) override;

   uint64_t addSubrequestsAndUpdateStats(std::list<Subrequest>& repackSubrequests,
      cta::common::dataStructures::ArchiveRoute::FullMap & archiveRoutesMap, uint64_t maxFSeqLowBound,
      const uint64_t maxAddedFSeq, const TotalStatsFiles &totalStatsFiles, disk::DiskSystemList diskSystemList,
      log::LogContext & lc) override;

   void expandDone() override;

   void fail() override;

   void requeueInToExpandQueue(log::LogContext &lc) override;

   void setExpandStartedAndChangeStatus() override;

   void fillLastExpandedFSeqAndTotalStatsFile(uint64_t &fSeq, TotalStatsFiles &totalStatsFiles) override;

};

} //namespace cta
