#pragma once

#include "SecurityIdentity.hpp"
#include "Tape.hpp"

#include <list>
#include <map>
#include <string>

namespace cta {

/**
 * Mock database-table of tapes.
 */
class MockTapeTable {
public:

  /**
   * Creates a tape.
   *
   * @param requester The identity of the user requesting the creation of the
   * tape.
   * @param vid The volume identifier of the tape.
   * @param logicalLibrary The name of the logical library to which the tape
   * belongs.
   * @param tapePoolName The name of the tape pool to which the tape belongs.
   * @param capacityInBytes The capacity of the tape.
   * @param comment The comment describing the logical library.
   */
  void createTape(
    const SecurityIdentity &requester,
    const std::string &vid,
    const std::string &logicalLibraryName,
    const std::string &tapePoolName,
    const uint64_t capacityInBytes,
    const std::string &comment);

  /**
   * Throws an exception if the specified tape already exists.
   *
   * @param vid The volume identifier of the tape.
   */
  void checkTapeDoesNotAlreadyExist(const std::string &vid) const;

  /**
   * Deletes the tape with the specified volume identifier.
   *
   * @param requester The identity of the user requesting the deletion of the
   * tape.
   * @param vid The volume identifier of the tape.
   */
  void deleteTape(
    const SecurityIdentity &requester,
    const std::string &vid);

  /**
   * Returns the current list of tapes in the lexicographical order of their
   * volume identifiers.
   *
   * @param requester The identity of the user requesting the list.
   * @return The current list of tapes in the lexicographical order of their
   * volume identifiers.
   */
  std::list<Tape> getTapes(
    const SecurityIdentity &requester) const;

private:

  /**
   * Mapping from volume identiifer to tape.
   */
  std::map<std::string, Tape> m_tapes;

}; // class MockTapeTable

} // namespace cta
