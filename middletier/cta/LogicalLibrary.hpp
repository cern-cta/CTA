#pragma once

#include "cta/ConfigurationItem.hpp"

#include <string>
#include <time.h>

namespace cta {

/**
 * Class representing a logical library.
 */
class LogicalLibrary: public ConfigurationItem {
public:

  /**
   * Constructor.
   */
  LogicalLibrary();

  /**
   * Destructor.
   */
  ~LogicalLibrary() throw();

  /**
   * Constructor.
   *
   * @param name The name of the logical library.
   * @param creator The identity of the user that created the logical library.
   * @param comment The comment describing the logical library.
   * @param creationTime Optionally the absolute time at which the
   * configuration item was created.  If no value is given then the current
   * time is used.
   */
  LogicalLibrary(
    const std::string &name,
    const UserIdentity &creator,
    const std::string &comment,
    const time_t creationTime = time(NULL));

  /**
   * Returns the name of the logical library.
   *
   * @return The name of the logical library.
   */
  const std::string &getName() const throw();

private:

  /**
   * The name of the logical library.
   */
  std::string m_name;

}; // class LogicalLibrary

} // namespace cta
