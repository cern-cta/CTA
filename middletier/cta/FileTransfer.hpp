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
   * @prama copyNb The copy number
   */
  FileTransfer(const std::string &id, const std::string &userRequestId,
    const uint32_t copyNb);

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

}; // class FileTransfer

} // namespace cta
