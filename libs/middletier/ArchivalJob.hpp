#pragma once

#include "UserIdentity.hpp"

#include <stdint.h>
#include <string>

namespace cta {

/**
 * Class representing an archival job.
 */
class ArchivalJob {
public:

  /**
   * Enumeration of the possible states of an archival job.
   */
  enum JobState {
    JOBSTATE_NONE,
    JOBSTATE_PENDING,
    JOBSTATE_SUCCESS,
    JOBSTATE_ERROR};

  /**
   * Thread safe method that returns the string representation of the
   * enumeration value.
   *
   * @param enumValue The enumeration value.
   * @return The string representation.
   */
  static const char *JobStateToStr(const JobState enumValue) throw();

  /**
   * Constructor.
   */
  ArchivalJob();

  /**
   * Constructor.
   *
   * @param id The identification string of the archival job.
   * @param state The state of the archival job.
   * @param totalNbFileTransfers The total number of file transfers
   * represented by the archival job.
   * @param creator The identity of the user that created the archival job.
   * @param comment The comment describing the archival job.
   */
  ArchivalJob(
    const std::string &id,
    const JobState state,
    const uint32_t totalNbFileTransfers,
    const UserIdentity &creator,
    const std::string &comment);

  /**
   * Returns the identification string of the archival job.
   *
   * @return The identification string of the archival job.
   */
  const std::string &getId() const throw();

  /**
   * Returns the state of the archival job.
   *
   * @return The state of the archival job.
   */
  JobState getState() const throw();

  /**
   * Returns the total number of file transfers represented by the archival job.
   *
   * @return The total number of file transfers represented by the archival job.
   */
  uint32_t getTotalNbFileTransfers() const throw();

  /**
   * Returns the number of failed file transfers.
   *
   * @return The number of failed file transfers.
   */
  uint32_t getNbFailedFileTransfers() const throw();

  /**
   * Returns the time when the archival job was created.
   *
   * @return The time when the archival job was created.
   */
  time_t getCreationTime() const throw();

  /**
   * Returns the identity of the user that created the archival job.
   *
   * @return The identity of the user that created the archival job.
   */
  const UserIdentity &getCreator() const throw();

  /**
   * Returns the comment describing the archival job.
   *
   * @return The comment describing the archival job.
   */
  const std::string &getComment() const throw();

private:

  /**
   * The identification string of the archival job.
   */
  std::string m_id;

  /**
   * The state of the archival job.
   */
  JobState m_state;

  /**
   * The total number of file transfers repesented by this archival job.
   */
  uint32_t m_totalNbFileTransfers;

  /**
   * The number of failed file transfers.
   */
  uint32_t m_nbFailedFileTransfers;

  /**
   * The time when the archival job was created.
   */
  time_t m_creationTime;

  /**
   * The identity of the user that created the archival job.
   */
  UserIdentity m_creator;

  /**
   * Comment describing the archival job.
   */
  std::string m_comment;

}; // class ArchivalJob

} // namespace cta
