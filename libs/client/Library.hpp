#pragma once

#include "UserIdentity.hpp"

#include <string>

namespace cta {

/**
 * Class representing a library.
 */
class Library {
public:

  /**
   * Constructor.
   */
  Library();

  /**
   * Constructor.
   *
   * @param name The name of the library.
   * @param deviceGroupName The name of the device group to which the library
   * belongs.  An empty string means the library does not belong to any device
   * group.
   * @param creator The identity of the user that created the library.
   * @param comment The comment describing the library.
   */
  Library(
    const std::string &name,
    const std::string &deviceGroupName,
    const UserIdentity &creator,
    const std::string &comment);

  /**
   * Returns the name of the library.
   *
   * @return The name of the library.
   */
  const std::string &getName() const throw();

  /**
   * Returns the name of the device group.
   *
   * @return The name of the device group.
   */
  const std::string &getDeviceGroupName() const throw();

  /**
   * Returns the time when the library was created.
   *
   * @return The time when the library was created.
   */
  time_t getCreationTime() const throw();

  /**
   * Returns the identity of the user that created the library.
   *
   * @return The identity of the user that created the library.
   */
  const UserIdentity &getCreator() const throw();

  /**
   * Returns the comment describing the library.
   *
   * @return The comment describing the library.
   */
  const std::string &getComment() const throw();

private:

  /**
   * The name of the library.
   */
  std::string m_name;

  /**
   * The name of the device group to which the library belongs.  An empty
   * string means the library does not belong to any device group.
   */
  std::string m_deviceGroupName;

  /**
   * The time when the library was created.
   */
  time_t m_creationTime;

  /**
   * The identity of the user that created the library.
   */
  UserIdentity m_creator;

  /**
   * Comment describing the library.
   */
  std::string m_comment;

}; // class Library

} // namespace cta
