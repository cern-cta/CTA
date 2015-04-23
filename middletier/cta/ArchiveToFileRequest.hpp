#pragma once

#include "cta/ArchivalRequest.hpp"

#include <stdint.h>
#include <string>

namespace cta {

/**
 * Class representing a user request to archive to a single remote file to a
 * single destination archive file.
 */
class ArchiveToFileRequest: public ArchivalRequest {
public:

  /**
   * Constructor.
   */
  ArchiveToFileRequest();

  /**
   * Destructor.
   */
  ~ArchiveToFileRequest() throw();

  /**
   * Constructor.
   *
   * @param remoteFile The URL of the source remote file to be archived.
   * @param archiveFile The full path of the destination archive file.
   * @param nbCopies The number of archive copies to be created.
   */
  ArchiveToFileRequest(const std::string &remoteFile,
    const std::string &archiveFile, const uint32_t nbCopies);

  /**
   * Returns the URL of the source remote file to be archived.
   *
   * @return The URL of the source remote file to be archived.
   */
  const std::string &getRemoteFile() const throw();

  /**
   * Returns the full path of the destination archive file.
   *
   * @return The full path of the destination archive file.
   */
  const std::string &getArchiveFile() const throw();

  /**
   * Returns the number of archive copies to be created.
   *
   * @return The number of archive copies to be created.
   */
  uint32_t getNbCopies() const throw();

private:

  /**
   * The URL of the destination remote file to be archived.
   */
  std::string m_remoteFile;

  /**
   * The full path of the source archive file.
   */
  std::string m_archiveFile;

  /**
   * The number of archive copies to be created.
   */
  uint32_t m_nbCopies;

}; // class ArchiveToFileRequest

} // namespace cta
