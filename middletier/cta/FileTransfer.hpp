#pragma once

#include <stdint.h>
#include <string>

namespace cta {

/**
 * Abstract class representing the transfer of a single copy of file.
 */
class FileTransfer {
public:

  /**
   * Constructor.
   */
  FileTransfer();

  /**
   * Destructor.
   */
  virtual ~FileTransfer() throw() = 0;

  /**
   * Constructor.
   *
   * @param id The identifier of the file transfer.
   * @param userRequestId The identifier of the associated user request.
   * @param copyNb The copy number.
   * @param remoteFile The URL of the remote file that depending on the
   * direction of the data transfer may be either the source or the
   * destination of the file transfer.
   */
  FileTransfer(
    const std::string &id,
    const std::string &userRequestId,
    const uint32_t copyNb,
    const std::string &remoteFile);

  /**
   * Returns the identifier of the file transfer.
   *
   * @return The identifier of the file transfer.
   */
  const std::string &getId() const throw();

  /**
   * Returns the identifier of the associated user request.
   *
   * @return The identifier of the associated user request.
   */
  const std::string &getUserRequestId() const throw();

  /**
   * Returns the copy number.
   *
   * @return The copy number.
   */
  uint32_t getCopyNb() const throw();

  /**
   * Returns the URL of the remote file that depending on the direction of the
   * data transfer may be either the source or the destination of the file
   * transfer.
   *
   * @return The URL of the remote file that depending on the direction of the
   * data transfer may be either the source or the destination of the file
   * transfer.
   */
  const std::string &getRemoteFile() const throw();

private:

  /**
   * The identifier of the file transfer.
   */
  std::string m_id;

  /**
   * The identifier of the associated user request.
   */
  std::string m_userRequestId;

  /**
   * The copy number.
   */
  uint32_t m_copyNb;

  /**
   * The URL of the remote file that depending on the direction of the data
   * transfer may be either the source or the destination of the file transfer.
   */
  std::string m_remoteFile;

}; // class FileTransfer

} // namespace cta
