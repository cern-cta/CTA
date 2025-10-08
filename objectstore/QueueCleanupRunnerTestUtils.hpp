/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
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
