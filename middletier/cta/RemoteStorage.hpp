#pragma once

#include <string>

namespace cta {

/**
 * Abstract class specifying the interface to a remote storage system.
 */
class RemoteStorage {
public:

  /**
   * Destructor.
   */
  virtual ~RemoteStorage() throw() = 0;

  /**
   * Returns true if the specified remote file exists or false if not.
   *
   * @param remoteFile The URL of the remote file.
   * @return true if the specified remote file exists or false if not.
   */
  virtual bool fileExists(const std::string &remoteFile) = 0;

  /**
   * Renames the specified remote file to the specified new name.
   *
   * @oldRemoteFile The current URL of teh remote file.
   * @newRemoteFile The new URL of teh remote file.
   */
  virtual void rename(const std::string &remoteFile,
    const std::string &newRemoteFile) = 0;

}; // class RemoteStorage

} // namespace cta
