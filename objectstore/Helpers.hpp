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

#pragma once

#include <string>

/**
 * A collection of helper functions for commonly used multi-object operations
 */

namespace cta { namespace objectstore {

class ArchiveQueue;
class ScopedExclusiveLock;
class AgentReference;

/**
 * A class with static functions allowing multi-object operations
 */
class Helpers {
public:
  /**
   * Find or create an archive queue, and return it locked and fetched to the caller
   * (ArchiveQueue and ScopedExclusiveLock objects are provided empty)
   * @param archiveQueue the ArchiveQueue object, empty
   * @param archiveQueueLock the lock, not initialized
   * @param tapePool the name of the needed tape pool
   */
  static void getLockedAndFetchedArchiveQueue(ArchiveQueue & archiveQueue, 
    ScopedExclusiveLock & archiveQueueLock, AgentReference & agentReference, 
    const std::string & tapePool);
  
};

}} // namespace cta::objectstore