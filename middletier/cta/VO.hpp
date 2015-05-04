#pragma once

#include "cta/ConfigurationItem.hpp"

#include <string>

namespace cta {

/**
 * Class representing a virtual organisation.
 */
class VO: public ConfigurationItem {
public:

  /**
   * Constructor.
   */
  VO();

  /**
   * Destructor.
   */
  ~VO() throw();

  /**
   * Constructor.
   *
   * @param name The name of the virtual organisation.
   * @param creator The identity of the user that created this configuration
   * item.
   * @param comment The comment describing this configuration item.
   * @param creationTime Optionally the absolute time at which this
   * configuration item was created.  If no value is given then the current
   * time is used.
   */
  VO(
    const std::string &name,
    const UserIdentity &creator,
    const std::string &comment,
    const time_t creationTime = time(NULL));

  /**
   * Returns the name of the tape pool.
   *
   * @return The name of the tape pool.
   */
  const std::string &getName() const throw();

private:

  /**
   * The name of the tape pool.
   */
  std::string m_name;

}; // class VO

} // namespace cta
