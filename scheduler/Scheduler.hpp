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

#include <list>
#include <map>
#include <string>

namespace cta {

// Forward declarations for opaque references.
class ArchivalJob;
class ArchiveToDirRequest;
class ArchiveToFileRequest;
class RetrievalJob;
class RetrieveToDirRequest;
class RetrieveToFileRequest;
class SecurityIdentity;
class Tape;
class TapePool;
class NameServer;

/**
 * Abstract class defining the interface to a tape resource scheduler.
 */
class Scheduler {
public:

  /**
   * Destructor.
   */
  virtual ~Scheduler() throw() = 0;

  /**
   * Queues the specified request.
   *
   * @param rqst The request to be queued.
   */
  virtual void queue(const ArchiveToDirRequest &rqst) = 0;

  /**
   * Queues the specified request.
   *
   * @param rqst The request to be queued.
   */
  virtual void queue(const ArchiveToFileRequest &rqst) = 0;

  /**
   * Returns all of the existing archival jobs grouped by tape pool and then
   * sorted by creation time in ascending order (oldest first).
   *
   * @param requester The identity of the user requesting the list.
   * @return All of the existing archival jobs grouped by tape pool and then
   * sorted by creation time in ascending order (oldest first).
   */
  virtual std::map<TapePool, std::list<ArchivalJob> > getArchivalJobs(
    const SecurityIdentity &requester) const = 0;

  /**
   * Returns the list of archival jobs associated with the specified tape pool
   * sorted by creation time in ascending order (oldest first).
   *
   * @param requester The identity of the user requesting the list.
   * @param tapePoolName The name of the tape pool.
   * @return The list of archival jobs associated with the specified tape pool
   * sorted by creation time in ascending order (oldest first).
   */
  virtual std::list<ArchivalJob> getArchivalJobs(
    const SecurityIdentity &requester,
    const std::string &tapePoolName) const = 0;

  /**
   * Deletes the specified archival job.
   *
   * @param requester The identity of the user requesting the deletion of the
   * tape.
   * @param dstPath The absolute path of the destination file within the
   * archive namespace.
   */
  virtual void deleteArchivalJob(
    const SecurityIdentity &requester,
    const std::string &dstPath) = 0;

  /**
   * Queues the specified request.
   *
   * @param rqst The request to be queued.
   */
  virtual void queue(RetrieveToDirRequest &rqst) = 0;

  /**
   * Queues the specified request.
   *
   * @param rqst The request to be queued.
   */
  virtual void queue(RetrieveToFileRequest &rqst) = 0;

  /**
   * Returns all of the existing retrieval jobs grouped by tape and then
   * sorted by creation time in ascending order (oldest first).
   *
   * @param requester The identity of the user requesting the list.
   * @return All of the existing retrieval jobs grouped by tape and then
   * sorted by creation time in ascending order (oldest first).
   */
  virtual std::map<Tape, std::list<RetrievalJob> > getRetrievalJobs(
    const SecurityIdentity &requester) const = 0;

  /**
   * Returns the list of retrieval jobs associated with the specified tape
   * sorted by creation time in ascending order (oldest first).
   *
   * @param requester The identity of the user requesting the list.
   * @param vid The volume identifier of the tape.
   * @return The list of retrieval jobs associated with the specified tape
   * sorted by creation time in ascending order (oldest first).
   */
  virtual std::list<RetrievalJob> getRetrievalJobs(
    const SecurityIdentity &requester,
    const std::string &vid) const = 0;
  
  /**
   * Deletes the specified retrieval job.
   *
   * @param requester The identity of the user requesting the deletion of the
   * tape.
   * @param dstUrl The URL of the destination file or directory.
   */
  virtual void deleteRetrievalJob(
    const SecurityIdentity &requester,
    const std::string &dstUrl) = 0;

}; // class Scheduler

} // namespace cta
