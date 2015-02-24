#pragma once

#include "RetrievalJob.hpp"
#include "Tape.hpp"
#include "SecurityIdentity.hpp"

#include <list>
#include <map>
#include <string>

namespace cta {

/**
 * Mock database-table of retrieval jobs.
 */
class MockRetrievalJobTable {
public:

  /**
   * Creates a retrival job.
   *
   * @param requester The identity of the user requesting the creation of the
   * job.
   * @param srcPath The full path of the source file within the archive.
   * @param dstUrl The URL of the destination file.
   */
  void createRetrievalJob(
    const SecurityIdentity &requester,
    const std::string &srcPath,
    const std::string &dstUrl);

  /**
   * Throws an exception if the specified retrieval job already exists.
   *
   * @param dstUrl The URL of the destination file.
   */
  void checkRetrievalJobDoesNotAlreadyExist(const std::string &dstUrl) const;

  /**
   * Deletes the specified retrieval job.
   *
   * @param requester The identity of the user requesting the deletion of the
   * job.
   * @param dstPath The full path of the destination file within the archive.
   */
  void deleteRetrievalJob(
    const SecurityIdentity &requester,
    const std::string &dstPath);

  /**
   * Returns all of the existing retrieval jobs grouped by tape and then
   * sorted by creation time in ascending order (oldest first).
   *
   * @param requester The identity of the user requesting the list.
   * @return All of the existing retrieval jobs grouped by tape and then
   * sorted by creation time in ascending order (oldest first).
   */
  const std::map<Tape, std::list<RetrievalJob> > getRetrievalJobs(
    const SecurityIdentity &requester) const;

  /**
   * Returns the list of retrieval jobs associated with the specified tape
   * sorted by creation time in ascending order (oldest first).
   *
   * @param requester The identity of the user requesting the list.
   * @param vid The volume identifier of the tape.
   * @return The list of retrieval jobs associated with the specified tape
   * sorted by creation time in ascending order (oldest first).
   */
  std::list<RetrievalJob> getRetrievalJobs(
    const SecurityIdentity &requester,
    const std::string &vid) const;

private:

  /**
   * All of the existing retrieval jobs grouped by tape and then
   * sorted by creation time in ascending order (oldest first).
   */
  std::map<Tape, std::map<time_t, RetrievalJob> > m_jobs;

}; // class MockRetrievalJobTable

} // namespace cta
