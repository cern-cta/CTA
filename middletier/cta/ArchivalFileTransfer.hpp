#pragma once

#include <string>

namespace cta {

/**
 * Class representing the transfer of a single copy of a remote file to tape.
 */
class ArchivalFileTransfer {
public:

  /**
   * Constructor.
   */
  ArchivalFileTransfer();

  /**
   * Destructor.
   */
  ~ArchivalFileTransfer() throw();

  /**
   * Constructor.
   *
   * @param remoteFile The URL of the remote source file.
   * @param vid The volume identifier of the destination tape.
   */
  ArchivalFileTransfer(const std::string &remoteFile, const std::string &vid);

  /**
   * Returns the URL of the remote source file.
   *
   * @return the URL of the remote source file.
   */
  const std::string &getRemoteFile() const throw();

  /**
   * Returns the volume identifier of the destination tape.
   *
   * @return the volume identifier of the destination tape.
   */
  const std::string &getVid() const throw();

private:

  /**
   * The URL of the remote source file.
   */
  std::string m_remoteFile;

  /**
   * The volume identifier of the destination tape.
   */
  std::string m_vid;

}; // class ArchivalFileTransfer

} // namespace cta
