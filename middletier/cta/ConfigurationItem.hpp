#pragma once

#include "cta/UserIdentity.hpp"

#include <string>
#include <time.h>

namespace cta {

/**
 * Abstract class representing a configuration item.
 */
class ConfigurationItem {
public:

  /**
   * Constructor.
   */
  ConfigurationItem();

  /**
   * Destructor.
   */
  virtual ~ConfigurationItem() throw() = 0;

  /**
   * Constructor.
   *
   * @param creator The identity of the user that created this configuration
   * item.
   * @param comment The comment made by the creator of this configuration
   * item.
   * @param creationTime Optionally the absolute time at which this
   * configuration item was created.  If no value is given then the current
   * time is used.
   */
  ConfigurationItem(const UserIdentity &creator, const std::string &comment,
    const time_t creationTime = time(NULL));

  /**
   * Returns the identity of the user that created this configuration item.
   *
   * @return The identity of the user that created this configuration item.
   */
  const UserIdentity &getCreator() const throw();

  /**
   * Returns the comment made by the creator of this configuration request.
   *
   * @return The comment made by the creator of this configuration request.
   */
  const std::string &getComment() const throw();

  /**
   * Returns the absolute time at which this configuration item was created.
   *
   * @return The absolute time at which this configuration item was created.
   */
  time_t getCreationTime() const throw();

private:

  /**
   * The identity of the user that created this configuration item.
   */
  UserIdentity m_creator;

  /**
   * The comment made by the creator of this configuration request.
   */
  std::string m_comment;

  /**
   * The absolute time at which this configuration item was created.
   */
  time_t m_creationTime;

}; // class ConfigurationItem

} // namespace cta
