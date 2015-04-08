#pragma once

#include "cta/Exception.hpp"

namespace cta {

/**
 * Class representing a tape releated fault.
 */
class TapeException: public Exception {
public:

  /**
   * Constructor.
   */
  TapeException(const std::string &message);

  /**
   * Destructor.
   */
  ~TapeException() throw();

}; // class Exception

} // namespace cta
