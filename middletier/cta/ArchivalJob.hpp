#pragma once

#include "cta/ArchivalJobState.hpp"
#include "cta/UserIdentity.hpp"

#include <string>
#include <time.h>

namespace cta {

/**
 * Class representing the job of archiving a file to tape.
 */
class ArchivalJob {
public:

  /**
   * Constructor.
   */
  ArchivalJob();

  /**
   * Constructor.
   *
   * @param state The state of the archival job.
   * @param srcUrl The URL of the source file.
   * @param dstPath The full path of the destination file within the archive.
   * @param creator The identity of the user that created the job.
   */
  ArchivalJob(
    const ArchivalJobState::Enum state,
    const std::string &srcUrl,
    const std::string &dstPath,
    const UserIdentity &creator,
    const time_t creationTime);

  /**
   * Sets the state of the archival job.
   *
   * @param state The new state of the archival job.
   */
  void setState(const ArchivalJobState::Enum state);

  /**
   * Returns the state of the archival job.
   *
   * @return The state of the archival job.
   */
  ArchivalJobState::Enum getState() const throw();

  /**
   * Thread safe method that returns the string representation of the state of
   * the archival job.
   *
   * @return The string representation of the state of the archival job.
   */
  const char *getStateStr() const throw();

  /**
   * Returns the URL of the source file.
   *
   * @return The URL of the source file.
   */
  const std::string &getSrcUrl() const throw();

  /**
   * Returns the full path of the destination file within the archive.
   *
   * @return The full path of the destination file within the archive.
   */
  const std::string &getDstPath() const throw();

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
   * The state of the archival job.
   */
  ArchivalJobState::Enum m_state;

  /**
   * The URL of the source file.
   */
  std::string m_srcUrl;

  /**
   * The full path of the destination file within the archive.
   */
  std::string m_dstPath;

  /**
   * The time when the job was created.
   */
  time_t m_creationTime;

  /**
   * The identity of the user that created the job.
   */
  UserIdentity m_creator;

}; // class FileArchival

} // namespace cta
