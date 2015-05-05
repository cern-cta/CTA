#pragma once

#include "cta/ArchivalRequest.hpp"
#include "cta/ArchiveToFileRequest.hpp"

#include <list>
#include <string>

namespace cta {

/**
 * Class representing a user request to archive one or more remote files to an
 * archive directory.
 */
class ArchiveToDirRquest: public ArchivalRequest {
public:

  /**
   * Constructor.
   */
  ArchiveToDirRquest();

  /**
   * Destructor.
   */
  ~ArchiveToDirRquest() throw();

  /**
   * Constructor.
   *
   * @param archiveDir The full path of the destination archive directory.
   * @param storageClassName The name of the storage class.
   * @param id The identifier of the request.
   * @param priority The priority of the request.
   * @param user The identity of the user who made the request.
   * @param creationTime Optionally the absolute time at which the user request
   * was created.  If no value is given then the current time is used.
   */
  ArchiveToDirRquest(
    const std::string &archiveDir,
    const std::string &storageClassName,
    const std::string &id,
    const uint64_t priority,
    const SecurityIdentity &user,
    const time_t creationTime = time(NULL));

  /**
   * Returns the full path of the destination archive directory.
   *
   * @return The full path of the destination archive directory.
   */
  const std::string &getArchiveDir() const throw();

  /**
   * Returns the list of the individual archive to individual file requests
   * that make up this archive to directory request.
   *
   * @return The list of the individual archive to file requests that make up
   * this archive to directory request.
   */
  const std::list<ArchiveToFileRequest> &getArchiveToFileRequests() const
    throw();

  /**
   * Returns the list of the individual archive to file requests that make up
   * this archive to directory request.
   *
   * @return The list of the individual archive to file requests that make up
   * this archive to directory request.
   */
  std::list<ArchiveToFileRequest> &getArchiveToFileRequests() throw();

private:

  /**
   * The full path of the destination archive directory.
   */
  std::string m_archiveDir;

  /**
   * The list of the individual archive to file requests that make up this
   * archive to directory request.
   */
  std::list<ArchiveToFileRequest> m_archiveToFileRequests;

}; // class ArchiveToDirRquest

} // namespace cta
