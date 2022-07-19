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

#include "RepackRequest.hpp"
#include "common/log/LogContext.hpp"

namespace cta {

PostgresSchedDB::RepackRequest::RepackRequest()
{
   throw cta::exception::Exception("Not implemented");
}

void PostgresSchedDB::RepackRequest::setLastExpandedFSeq(uint64_t fseq)
{
   throw cta::exception::Exception("Not implemented");
}

uint64_t PostgresSchedDB::RepackRequest::addSubrequestsAndUpdateStats(std::list<Subrequest>& repackSubrequests,
      cta::common::dataStructures::ArchiveRoute::FullMap & archiveRoutesMap, uint64_t maxFSeqLowBound,
      const uint64_t maxAddedFSeq, const TotalStatsFiles &totalStatsFiles, disk::DiskSystemList diskSystemList,
      log::LogContext & lc)
{
   throw cta::exception::Exception("Not implemented");
}

void PostgresSchedDB::RepackRequest::expandDone()
{
   throw cta::exception::Exception("Not implemented");
}

void PostgresSchedDB::RepackRequest::fail()
{
   throw cta::exception::Exception("Not implemented");
}

void PostgresSchedDB::RepackRequest::requeueInToExpandQueue(log::LogContext &lc)
{
   throw cta::exception::Exception("Not implemented");
}

void PostgresSchedDB::RepackRequest::setExpandStartedAndChangeStatus()
{
   throw cta::exception::Exception("Not implemented");
}

void PostgresSchedDB::RepackRequest::fillLastExpandedFSeqAndTotalStatsFile(uint64_t &fSeq, TotalStatsFiles &totalStatsFiles)
{
   throw cta::exception::Exception("Not implemented");
}

} //namespace cta
