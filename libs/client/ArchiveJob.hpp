#pragma once

#include "UserIdentity.hpp"

#include <string>

namespace cta {

/**
 * Class representing a  archive job.
 */
class ArchiveJob {
public:

  /**
   * Constructor.
   */
  ArchiveJob();

  /**
   * Constructor.
   *
   * @param id The identification string of the archive job.
   * @param creator The identity of the user that created the archive job.
   * @param comment The comment describing the archive job.
   */
  ArchiveJob(
    const std::string &id,
    const UserIdentity &creator,
    const std::string &comment);

  /**
   * Returns the identification string of the archive job.
   *
   * @return The identification string of the archive job.
   */
  const std::string &getId() const throw();

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
