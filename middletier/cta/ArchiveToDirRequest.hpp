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
   */
  ArchiveToDirRquest(const std::string &archiveDir);

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
