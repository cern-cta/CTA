#pragma once

#include "UserIdentity.hpp"

#include <string>
#include <time.h>

namespace cta {

/**
 * Class representing the job of retrieving a file from tape.
 */
class FileRetrievalJob {
public:

  /**
   * Constructor.
   */
  FileRetrievalJob();

  /**
   * Constructor.
   *
   * @param srcPath The full path of the source file within the archive.
   * @param dstUrl The URL of the destination file.
   * @param creator The identity of the user that created the job.
   */
  FileRetrievalJob(
    const std::string &srcPath,
    const std::string &dstUrl,
    const UserIdentity &creator);

  /**
   * Returns the full path of the source file within the archive.
   *
   * @return The full path of the source file within the archive.
   */
  const std::string &getSrcPath() const throw();

  /**
   * Returns the URL of the destination file.
   *
   * @return The URL of the destination file.
   */
  const std::string &getDstUrl() const throw();

  /**
   * Returns the time when the job was created.
   *
   * @return The time when the job was created.
   */
  time_t getCreationTime() const throw();

  /**
   * Returns the identity of the user that created the job.
   *
   * @return The identity of the user that created the job.
   */
  const UserIdentity &getCreator() const throw();

private:

  /**
   * The full path of the source file within the archive.
   */
  std::string m_srcPath;

  /**
   * The URL of the destination file.
   */
  std::string m_dstUrl;

  /**
   * The time when the job was created.
   */
  time_t m_creationTime;

  /**
   * The identity of the user that created the job.
   */
  UserIdentity m_creator;

}; // class FileRetrieval

} // namespace cta
