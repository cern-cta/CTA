#pragma once

#include "cta/TapeFileLocation.hpp"

#include <string>

namespace cta {

/**
 * Class representing the transfer of a single copy of a tape file to a remote
 * file.
 */
class RetrievalFileTransfer {
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
   * @param tapeFileLocation The location of the source tape file.
   * @param remoteFile The URL of the destination source file.
   */
  RetrievalFileTransfer(const TapeFileLocation &tapeFile,
    const std::string &remoteFile);

  /**
   * Returns the location of the source tape file.
   *
   * @return The location of the source tape file.
   */
  const TapeFileLocation &getTapeFile() const throw();

  /**
   * Returns the URL of the remote source file.
   *
   * @return the URL of the remote source file.
   */
  const std::string &getRemoteFile() const throw();

private:

  /**
   * The location of the source tape file.
   */
  const TapeFileLocation m_tapeFile;

  /**
   * The URL of the remote destination file.
   */
  std::string m_remoteFile;

}; // class RetrievalFileTransfer

} // namespace cta
