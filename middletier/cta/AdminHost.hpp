#pragma once

#include "cta/ConfigurationItem.hpp"
#include "cta/UserIdentity.hpp"

#include <string>

namespace cta {

/**
 * Class representing a administration host.
 */
class AdminHost: public ConfigurationItem {
public:

  /**
   * Constructor.
   */
  AdminHost();

  /**
   * Destructor.
   */
  ~AdminHost() throw();

  /**
   * Constructor.
   *
   * @param name The network name of the administration host.
   * @param creator The identity of the user that created this configuration
   * item.
   * @param comment The comment made by the creator of this configuration
   * item.
   * @param creationTime Optionally the absolute time at which this
   * configuration item was created.  If no value is given then the current
   * time is used.
   */
  AdminHost(
    const std::string &name,
    const UserIdentity &creator,
    const std::string &comment,
    const time_t creationTime = time(NULL));

  /**
   * Returns the network name of the administration host.
   *
   * @return The network name of the administration host.
   */
  const std::string &getName() const throw();

private:

  /**
   * The network name of the administration host.
   */
  std::string m_name;

}; // class AdminHost

} // namespace cta
