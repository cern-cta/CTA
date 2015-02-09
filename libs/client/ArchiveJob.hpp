#pragma once

#include "UserIdentity.hpp"

#include <stdint.h>
#include <string>

namespace cta {

/**
 * Class representing a  archive job.
 */
class ArchiveJob {
public:

  /**
   * Enumeration of the possible states of an archive job.
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
  ArchiveJob();

  /**
   * Constructor.
   *
   * @param id The identification string of the archive job.
   * @param state The state of the archive job.
   * @param totalNbFileTransfers The total number of file transfers
   * represented by the archive job.
   * @param creator The identity of the user that created the archive job.
   * @param comment The comment describing the archive job.
   */
  ArchiveJob(
    const std::string &id,
    const JobState state,
    const uint32_t totalNbFileTransfers,
    const UserIdentity &creator,
    const std::string &comment);

  /**
   * Returns the identification string of the archive job.
   *
   * @return The identification string of the archive job.
   */
  const std::string &getId() const throw();

  /**
   * Returns the state of the archive job.
   *
   * @return The state of the archive job.
   */
  JobState getState() const throw();

  /**
   * Returns the total number of file transfers represented by the archive job.
   *
   * @return The total number of file transfers represented by the archive job.
   */
  uint32_t getTotalNbFileTransfers() const throw();

  /**
   * Returns the number of failed file transfers.
   *
   * @return The number of failed file transfers.
   */
  uint32_t getNbFailedFileTransfers() const throw();

  /**
   * Returns the time when the archive job was created.
   *
   * @return The time when the archive job was created.
   */
  time_t getCreationTime() const throw();

  /**
   * Returns the identity of the user that created the archive job.
   *
   * @return The identity of the user that created the archive job.
   */
  const UserIdentity &getCreator() const throw();

  /**
   * Returns the comment describing the archive job.
   *
   * @return The comment describing the archive job.
   */
  const std::string &getComment() const throw();

private:

  /**
   * The identification string of the archive job.
   */
  std::string m_id;

  /**
   * The state of the archive job.
   */
  JobState m_state;

  /**
   * The total number of file transfers repesented by this archive job.
   */
  uint32_t m_totalNbFileTransfers;

  /**
   * The number of failed file transfers.
   */
  uint32_t m_nbFailedFileTransfers;

  /**
   * The time when the archive job was created.
   */
  time_t m_creationTime;

  /**
   * The identity of the user that created the archive job.
   */
  UserIdentity m_creator;

  /**
   * Comment describing the archive job.
   */
  std::string m_comment;

}; // class ArchiveJob

} // namespace cta
