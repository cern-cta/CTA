#pragma once

#include "cta/RetrievalRequest.hpp"

#include <string>

namespace cta {

/**
 * Class representing a user request to retrieve a single archived file to a
 * single remote file.
 */
class RetrieveToFileRequest: public RetrievalRequest {
public:

  /**
   * Constructor.
   */
  RetrieveToFileRequest();

  /**
   * Destructor.
   */
  ~RetrieveToFileRequest() throw();

  /**
   * Constructor.
   *
   * @param archiveFile The full path of the source archive file.
   * @param remoteFile The URL of the destination remote file.
   */
  RetrieveToFileRequest(const std::string &archiveFile,
    const std::string &remoteFile);

  /**
   * Returns the full path of the source archive file.
   *
   * @return The full path of the source archive file.
   */
  const std::string &getArchiveFile() const throw();

  /**
   * Returns the URL of the destination remote file.
   *
   * @return The URL of the destination remote file.
   */
  const std::string &getRemoteFile() const throw();

private:

  /**
   * The full path of the source archive file.
   */
  std::string m_archiveFile;

  /**
   * The URL of the destination remote file.
   */
  std::string m_remoteFile;

}; // class RetrieveToFileRequest

} // namespace cta
