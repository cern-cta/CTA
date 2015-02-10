#pragma once

#include "UserIdentity.hpp"

#include <string>

namespace cta {

/**
 * Class representing a device group.
 */
class DeviceGroup {
public:

  /**
   * Constructor.
   */
  DeviceGroup();

  /**
   * Constructor.
   *
   * @param name The name of the device group.
   * @param creator The identity of the user that created the device group.
   * @param comment The comment describing the device group.
   */
  DeviceGroup(
    const std::string &name,
    const UserIdentity &creator,
    const std::string &comment);

  /**
   * Returns the name of the device group.
   *
   * @return The name of the device group.
   */
  const std::string &getName() const throw();

  /**
   * Returns the time when the device group was created.
   *
   * @return The time when the device group was created.
   */
  time_t getCreationTime() const throw();

  /**
   * Returns the identity of the user that created the device group.
   *
   * @return The identity of the user that created the device group.
   */
  const UserIdentity &getCreator() const throw();

  /**
   * Returns the comment describing the device group.
   *
   * @return The comment describing the device group.
   */
  const std::string &getComment() const throw();

private:

  /**
   * The name of the device group.
   */
  std::string m_name;

  /**
   * The time when the device group was created.
   */
  time_t m_creationTime;

  /**
   * The identity of the user that created the device group.
   */
  UserIdentity m_creator;

  /**
   * Comment describing the device group.
   */
  std::string m_comment;

}; // class DeviceGroup

} // namespace cta
