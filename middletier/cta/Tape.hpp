#pragma once

#include "cta/ConfigurationItem.hpp"
#include "cta/UserIdentity.hpp"

#include <stdint.h>
#include <string>

namespace cta {

/**
 * Class representing a tape.
 */
class Tape: public ConfigurationItem {
public:

  /**
   * Constructor.
   */
  Tape();

  /**
   * Destructor.
   */
  ~Tape() throw();

  /**
   * Constructor.
   *
   * @param vid The volume identifier of the tape.
   * @param logicalLibraryName The name of the logical library to which the tape
   * belongs.
   * @param tapePoolName The name of the tape pool to which the tape belongs.
   * @param capacityInBytes The capacity of the tape.
   * @param dataOnTapeInBytes The amount of data currently stored on the tape in
   * bytes.
   * @param creator The identity of the user that created this configuration
   * item.
   * @param comment The comment describing this configuration item.
   * @param creationTime Optionally the absolute time at which this
   * configuration item was created.  If no value is given then the current
   * time is used.
   */
  Tape(
    const std::string &vid,
    const std::string &logicalLibraryName,
    const std::string &tapePoolName,
    const uint64_t capacityInBytes,
    const uint64_t dataOnTapeInBytes,
    const UserIdentity &creator,
    const std::string &comment,
    const time_t creationTime = time(NULL));

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

}; // class Tape

} // namespace cta
