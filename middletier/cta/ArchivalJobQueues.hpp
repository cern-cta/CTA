#pragma once

#include "cta/ArchivalJob.hpp"
#include "cta/ArchivalMount.hpp"
#include "cta/Exception.hpp"

#include <list>
#include <stdint.h>
#include <string>

namespace cta {

/**
 * Abstract class representing the archival job queues.
 */
class ArchivalJobQueues {
public:

  /**
   * Destructor.
   */
  virtual ~ArchivalJobQueues() throw() = 0;

  /**
   * Queues the specified archival job with the specified priority.
   *
   * @param priority The priority of the job to be queued.
   * @param job The job to be queued.
   */
  virtual void queueJob(const uint32_t priority, const ArchivalJob &job) = 0;

  /**
   * Deletes the archival job associated with the specified destination entry
   * within the archive namespace.
   *
   * @param dstPath The full path of the destination entry within the archive
   * namespace.
   */
  virtual void deleteJob(const std::string &dstPath) = 0;

  /**
   * Returns the current list of archival jobs for the specified tape pool.
   *
   * @param tapePoolName The name of the tape pool.
   */
  virtual std::list<ArchivalJob> getJobs(const std::string &tapePoolName) = 0;

  /**
   * Notifies this object that the specified destination entry has been created
   * with the archive namespace.
   *
   * @param dstPath The full path of the destination entry within the archive
   * namespace.
   */
  virtual void fileCreatedInNamespace(const std::string &dstPath) = 0;

  /**
   * Notifies this object that the specified file transfer has succeeded.
   *
   * @param copyNb The copy number of the destination tape file.
   * @param dstPath The full path of the destination entry within the archive
   * namespace.
   */
  virtual void fileTransferSucceeded(const uint32_t copyNb,
    const std::string &dstPath) = 0;

  /**
   * Notifies this object that the specified file transfer has failed.
   *
   * @param copyNb The copy number of the destination tape file.
   * @param dstPath The full path of the destination entry within the archive
   * namespace.
   * @param reason The reason the file transfer failed.
   */
  virtual void fileTransferFailed(const uint32_t copyNb,
    const std::string &dstPath, const Exception &reason) = 0;

  /**
   * Notifies this object that the specified destination entry within the
   * archive namespace has been marked as archived.
   *
   * @param dstPath The full path of the destination entry within the archive
   * namespace.
   */
  virtual void fileArchivedInNamespace(const std::string &dstPath) = 0;

  /**
   * Returns the next archival mount for the specified tape pool or NULL if there
   * currenly isn't one.
   *
   * @param tapePoolName The name of the tape pool.
   * @return The next archival mount or NULL if there isn't one.  Please note
   * that it is the responsibility of the caller to deallocate any returned
   * ArchivalMount object.
   */
  virtual ArchivalMount *getNextMount(const std::string &tapePoolName) = 0;

  /**
   * Returns the next batch of archival jobs for the specified on-going tape
   * mount.
   *
   * @param mountId The identifier of the mount.
   */
  virtual std::list<ArchivalJob> getNextJobsForMount(const std::string &mountId)
    = 0;

  /**
   * Requests this object to execute any time dependent and asynchronous logic.
   */
  virtual void tick() = 0;

}; // class ArchivalJobQueues

} // namespace cta
