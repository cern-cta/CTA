#pragma once

#include "cta/FileTransfer.hpp"
#include "cta/TapeFileLocation.hpp"

#include <string>

namespace cta {

/**
 * Class representing the transfer of a single copy of a tape file to a remote
 * file.
 */
class RetrievalFileTransfer: public FileTransfer {
public:

  /**
   * Constructor.
   */
  RetrievalFileTransfer();

  /**
   * Destructor.
   */
  ~RetrievalFileTransfer() throw();

  /**
   * Constructor.
   *
   * @param tapeFile The location of the source tape file.
   * @param id The identifier of the file transfer.
   * @param userRequestId The identifier of the associated user request.
   * @param copyNb The copy number.
   * @param remoteFile The URL of the destination source file.
   */
  RetrievalFileTransfer(
    const TapeFileLocation &tapeFile,
    const std::string &id,
    const std::string &userRequestId,
    const uint32_t copyNb,
    const std::string &remoteFile);

  /**
   * Returns the location of the source tape file.
   *
   * @return The location of the source tape file.
   */
  const TapeFileLocation &getTapeFile() const throw();

private:

  /**
   * The location of the source tape file.
   */
  const TapeFileLocation m_tapeFile;

}; // class RetrievalFileTransfer

} // namespace cta
