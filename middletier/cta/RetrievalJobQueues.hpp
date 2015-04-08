#pragma once

#include "cta/RetrievalJob.hpp"
#include "cta/RetrievalMount.hpp"
#include "cta/Exception.hpp"

#include <list>
#include <stdint.h>
#include <string>

namespace cta {

/**
 * Abstract class representing the retrieval job queues.
 */
class RetrievalJobQueues {
public:

  /**
   * Destructor.
   */
  virtual ~RetrievalJobQueues() throw() = 0;

  /**
   * Queues the specified retrieval job with the specified priority.
   *
   * @param priority The priority of the job to be queued.
   * @param job The job to be queued.
   */
  virtual void queueJob(const uint32_t priority, const RetrievalJob &job) = 0;

  /**
   * Deletes the retrieval job associated with the specified destination disk
   * file.
   *
   * @param dstUrl The URL of the destination disk file.
   */
  virtual void deleteJob(const std::string &dstUrl) = 0;

  /**
   * Returns the current list of retrieval jobs for the specified tape.
   *
   * @param vid The volume-indentifier name of the tape.
   */
  virtual std::list<RetrievalJob> getJobs(const std::string &vid) = 0;

  /**
   * Notifies this object that the specified file transfer has succeeded.
   *
   * @param copyNb The copy number of the source tape file.
   * @param dstUrl The URL of the destination disk file.
   */
  virtual void fileTransferSucceeded(const uint32_t copyNb,
    const std::string &dstUrl) = 0;

  /**
   * Notifies this object that the specified file transfer has failed.
   *
   * @param copyNb The copy number of the source tape file.
   * @param dstUrl The URL of the destination disk file.
   * @param reason The reason the file transfer failed.
   */
  virtual void fileTransferFailed(const uint32_t copyNb,
    const std::string &dstUrl, const Exception &reason) = 0;

  /**
   * Returns the next retrieval mount for the specified tape or NULL if there
   * currenly isn't one.
   *
   * @param vid The volume-indentifier name of the tape.
   * @return The next retrieval mount or NULL if there isn't one.  Please note
   * that it is the responsibility of the caller to deallocate any returned
   * RetrievalMount object.
   */
  virtual RetrievalMount *getNextMount(const std::string &vid) = 0;

  /**
   * Returns the next batch of retrieval jobs for the specified on-going tape
   * mount.
   *
   * @param mountId The identifier of the mount.
   */
  virtual std::list<RetrievalJob> getNextJobsForMount(const std::string &mountId)
    = 0;

  /**
   * Requests this object to execute any time dependent and asynchronous logic.
   */
  virtual void tick() = 0;

}; // class RetrievalJobQueues

} // namespace cta
