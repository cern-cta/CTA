#pragma once

#include "cta/FileTransfer.hpp"

#include <stdint.h>
#include <string>

namespace cta {

/**
 * Class representing the transfer of a single copy of a remote file to tape.
 */
class ArchivalFileTransfer: public FileTransfer {
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
   * @param tapePoolName The name of the destination tape pool.
   * @param id The identifier of the file transfer.
   * @param userRequestId The identifier of the associated user request.
   * @param copyNb The copy number.
   * @param remoteFile The URL of the remote source file.
   */
  ArchivalFileTransfer(
    const std::string &tapePoolName,
    const std::string &id,
    const std::string &userRequestId,
    const uint32_t copyNb,
    const std::string &remoteFile);

  /**
   * Returns the name of the destination tape pool.
   *
   * @return the name of the destination tape pool.
   */
  const std::string &getTapePoolName() const throw();

private:

  /**
   * The name of the destination tape pool.
   */
  std::string m_tapePoolName;

}; // class ArchivalFileTransfer

} // namespace cta
