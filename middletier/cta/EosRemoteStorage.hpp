#pragma once

#include "cta/RemoteStorage.hpp"

namespace cta {

/**
 * Proxy class for a remote EOS storage system.
 */
class EosRemoteStorage: public RemoteStorage {
public:

  /**
   * Destructor.
   */
  ~EosRemoteStorage() throw();

  /**
   * Returns true if the specified remote file exists or false if not.
   *
   * @param remoteFile The URL of the remote file.
   * @return true if the specified remote file exists or false if not.
   */
  bool fileExists(const std::string &remoteFile);

  /**
   * Renames the specified remote file to the specified new name.
   *
   * @oldRemoteFile The current URL of teh remote file.
   * @newRemoteFile The new URL of teh remote file.
   */
  void rename(const std::string &remoteFile,
    const std::string &newRemoteFile);

}; // class EosRemoteStorage

} // namespace cta
