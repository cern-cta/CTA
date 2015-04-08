#pragma once

#include "cta/Exception.hpp"

namespace cta {

/**
 * Class representing a disk releated fault.
 */
class DiskException: public Exception {
public:

  /**
   * Constructor.
   */
  DiskException(const std::string &message);

  /**
   * Destructor.
   */
  ~DiskException() throw();

}; // class Exception

} // namespace cta
