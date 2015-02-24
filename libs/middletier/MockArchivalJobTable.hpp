#pragma once

#include "ArchivalJob.hpp"
#include "TapePool.hpp"
#include "SecurityIdentity.hpp"

#include <list>
#include <map>
#include <string>

namespace cta {

/**
 * Mock database-table of archival jobs.
 */
class MockArchivalJobTable {
public:

  /**
   * Creates an archival job.
   *
   * @param requester The identity of the user requesting the creation of the
   * job.
   * @param srcUrl The URL of the source file.
   * @param dstPath The full path of the destination file within the archive.
   */
  void createArchivalJob(
    const SecurityIdentity &requester,
    const std::string &srcUrl,
    const std::string &dstPath);

  /**
   * Throws an exception if the specified archival job already exists.
   *
   * @param dstPath The full path of the destination file within the archive.
   */
  void checkArchivalJobDoesNotAlreadyExist(const std::string &dstPath) const;

  /**
   * Deletes the specified archival job.
   *
   * @param requester The identity of the user requesting the deletion of the
   * job.
   * @param dstPath The full path of the destination file within the archive.
   */
  void deleteArchivalJob(
    const SecurityIdentity &requester,
    const std::string &dstPath);

  /**
   * Returns all of the existing archival jobs grouped by tape pool and then
   * sorted by creation time in ascending order (oldest first).
   *
   * @param requester The identity of the user requesting the list.
   * @return All of the existing archival jobs grouped by tape pool and then
   * sorted by creation time in ascending order (oldest first).
   */
  const std::map<TapePool, std::list<ArchivalJob> > getArchivalJobs(
    const SecurityIdentity &requester) const;

  /**
   * Returns the list of archival jobs associated with the specified tape pool
   * sorted by creation time in ascending order (oldest first).
   *
   * @param requester The identity of the user requesting the list.
   * @param tapePoolName The name of the tape pool.
   * @return The list of archival jobs associated with the specified tape pool
   * sorted by creation time in ascending order (oldest first).
   */
  std::list<ArchivalJob> getArchivalJobs(
    const SecurityIdentity &requester,
    const std::string &tapePoolName) const;

private:

  /**
   * All of the existing archival jobs grouped by tape pool and then
   * sorted by creation time in ascending order (oldest first).
   */
  std::map<TapePool, std::map<time_t, ArchivalJob> > m_jobs;

}; // class MockArchivalJobTable

} // namespace cta
