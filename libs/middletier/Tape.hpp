#pragma once

#include "UserIdentity.hpp"

#include <string>

namespace cta {

/**
 * Class representing a tape.
 */
class Tape {
public:

  /**
   * Constructor.
   */
  Tape();

  /**
   * Constructor.
   *
   * @param vid The volume identifier of the tape.
   * @param logicalLibrary The name of the logical library to which the tape
   * belongs.
   * @param tapePoolName The name of the tape pool to which the tape belongs.
   * @param capacityInBytes The capacity of the tape.
   * @param creator The identity of the user that created the tape.
   * @param comment The comment describing the tape.
   */
  Tape(
    const std::string &vid,
    const std::string &logicalLibraryName,
    const std::string &tapePoolName,
    const uint64_t capacityInBytes,
    const uint64_t dataOnTapeInBytes,
    const UserIdentity &creator,
    const time_t creationTime,
    const std::string &comment);

  /**
   * Less than operator.
   *
   * @param rhs The right-hand side of the operator.
   */
  bool operator<(const Tape &rhs) const throw();

  /**
   * Returns the volume identifier of the tape.
   *
   * @return The volume identifier of the tape.
   */
  const std::string &getVid() const throw();

  /**
   * Returns the name of the logical library to which the tape belongs.
   *
   * @return The name of the logical library to which the tape belongs.
   */
  const std::string &getLogicalLibraryName() const throw();

  /**
   * Returns the name of the tape pool to which the tape belongs.
   *
   * @return The name of the tape pool to which the tape belongs.
   */
  const std::string &getTapePoolName() const throw();

  /**
   * Returns the capacity of the tape.
   *
   * @return The capacity of the tape.
   */
  uint64_t getCapacityInBytes() const throw();

  /**
   * Returns the amount of data on the tape.
   *
   * @return The amount of data on the tape.
   */
  uint64_t getDataOnTapeInBytes() const throw();

  /**
   * Returns the time when the tape was created.
   *
   * @return The time when the tape was created.
   */
  time_t getCreationTime() const throw();

  /**
   * Returns the identity of the user that created the tape.
   *
   * @return The identity of the user that created the tape.
   */
  const UserIdentity &getCreator() const throw();

  /**
   * Returns the comment describing the tape.
   *
   * @return The comment describing the tape.
   */
  const std::string &getComment() const throw();

private:

  /**
   * The volume identifier of the tape.
   */
  std::string m_vid;

  /**
   * The name of the logical library to which the tape belongs.
   */
  std::string m_logicalLibraryName;

  /**
   * The name of the tape pool to which the tape belongs.
   */
  std::string m_tapePoolName;

  /**
   * The capacity of the tape.
   */
  uint64_t m_capacityInBytes;

  /**
   * The amount of data on the tape.
   */
  uint64_t m_dataOnTapeInBytes;

  /**
   * The time when the tape was created.
   */
  time_t m_creationTime;

  /**
   * The identity of the user that created the tape.
   */
  UserIdentity m_creator;

  /**
   * Comment describing the tape.
   */
  std::string m_comment;

}; // class Tape

} // namespace cta
