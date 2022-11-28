/*
* @project      The CERN Tape Archive (CTA)
* @copyright    Copyright © 2021-2022 CERN
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

#include "objectstore/Sorter.hpp"
#include "objectstore/BackendVFS.hpp"


/**
 * Plan => Cleanup runner keeps track of queues that need to be emptied
 * If a queue is signaled for cleanup, the cleanup runner should take ownership of it, and move all the requests
 * to other queues.
 * If there is no other queue available, the request should be aborted and reported back to the user.
 */

namespace unitTests {

void fillRetrieveRequestsForCleanupRunner(
        typename cta::objectstore::ContainerAlgorithms<cta::objectstore::RetrieveQueue, cta::objectstore::RetrieveQueueToTransfer>::InsertedElement::list &requests,
        uint32_t requestNr,
        std::list<std::unique_ptr<cta::objectstore::RetrieveRequest> > &requestPtrs, //List to avoid memory leak on ArchiveQueueAlgorithms test
        std::set<std::string> & tapeNames, // List of tapes that will contain a replica
        std::string & activeCopyTape,
        cta::objectstore::BackendVFS &be,
        cta::objectstore::AgentReference &agentRef, uint64_t startFseq = 0);

}