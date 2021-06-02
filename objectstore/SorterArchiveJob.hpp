/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2021 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#ifndef SORTERARCHIVEJOB_HPP
#define SORTERARCHIVEJOB_HPP
#include "ArchiveRequest.hpp"

namespace cta { namespace objectstore {
/**
 * This structure holds the necessary data to queue a job taken from the ArchiveRequest that needs to be queued.
 */
struct SorterArchiveJob{
  std::shared_ptr<ArchiveRequest> archiveRequest;
  ArchiveRequest::JobDump jobDump;
  common::dataStructures::ArchiveFile archiveFile;
  AgentReferenceInterface * previousOwner;
  common::dataStructures::MountPolicy mountPolicy;
  cta::objectstore::JobQueueType jobQueueType;
};
  
/**
 * This structure holds the datas the user have to 
 * give to insert an ArchiveRequest without any fetch needed on the Request
 */
struct SorterArchiveRequest{
  std::list<SorterArchiveJob> archiveJobs;
};


}} 
#endif /* SORTERARCHIVEJOB_HPP */

