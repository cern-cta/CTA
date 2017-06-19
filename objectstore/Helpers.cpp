/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "Helpers.hpp"
#include "Backend.hpp"
#include "ArchiveQueue.hpp"
#include "AgentReference.hpp"
#include "RootEntry.hpp"

namespace cta { namespace objectstore {

//------------------------------------------------------------------------------
// OStoreDB::getLockedAndFetchedArchiveQueue()
//------------------------------------------------------------------------------
void Helpers::getLockedAndFetchedArchiveQueue(ArchiveQueue& archiveQueue,
  ScopedExclusiveLock& archiveQueueLock, AgentReference & agentReference,
  const std::string& tapePool) {
  // TODO: if necessary, we could use a singleton caching object here to accelerate
  // lookups.
  // Getting a locked AQ is the name of the game.
  // Try and find an existing one first, create if needed
  Backend & be = archiveQueue.m_objectStore;
  for (size_t i=0; i<5; i++) {
    {
      RootEntry re (be);
      ScopedSharedLock rel(re);
      re.fetch();
      try {
        archiveQueue.setAddress(re.getArchiveQueueAddress(tapePool));
      } catch (cta::exception::Exception & ex) {
        rel.release();
        ScopedExclusiveLock rexl(re);
        re.fetch();
        archiveQueue.setAddress(re.addOrGetArchiveQueueAndCommit(tapePool, agentReference));
      }
    }
    try {
      archiveQueueLock.lock(archiveQueue);
      archiveQueue.fetch();
      return;
    } catch (cta::exception::Exception & ex) {
      // We have a (rare) opportunity for a race condition, where we identify the
      // queue and it gets deleted before we manage to lock it.
      // The locking of fetching will fail in this case.
      // We hence allow ourselves to retry a couple times.
      continue;
    }
  }
  throw cta::exception::Exception(std::string(
      "In OStoreDB::getLockedArchiveQueue(): failed to find or create and lock archive queue after 5 retries for tapepool: ")
      + tapePool);
}

}} // namespace cta::objectstore.