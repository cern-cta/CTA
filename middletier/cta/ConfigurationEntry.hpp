#pragma once

#include "cta/UserIdentity.hpp"

#include <string>
#include <time.h>

namespace cta {

/**
 * Abstract class representing a configuration entry.
 */
class ConfigurationEntry {
public:

  /**
   * Constructor.
   */
  ConfigurationEntry();

  /**
   * Destructor.
   */
  virtual ~ConfigurationEntry() throw() = 0;

  /**
   * Constructor.
   *
   * @param creator The identity of the user that created the configuration
   * entry.
   * @param comment The comment made by the creator of the configuration
   * request.
   */
  ConfigurationEntry(const UserIdentity &creator, const std::string &comment);

  /**
   * Returns the absolute time at which the configuration entry was created.
   *
   * @return The absolute time at which the configuration entry was created.
   */
  time_t getCreationTime() const throw();

  /**
   * Returns the identity of the user that created the configuration entry.
   *
   * @return The identity of the user that created the configuration entry.
   */
  const UserIdentity &getCreator() const throw();

  /**
   * Returns the comment made by the creator of the configuration request.
   *
   * @return The comment made by the creator of the configuration request.
   */
  const std::string &getComment() const throw();

private:

  /**
   * The absolute time at which the configuration entry was created.
   */
  time_t m_creationTime;

  /**
   * The identity of the user that created the configuration entry.
   */
  UserIdentity m_creator;

  /**
   * The comment made by the creator of the configuration request.
   */
  std::string m_comment;

}; // class ConfigurationEntry

} // namespace cta
