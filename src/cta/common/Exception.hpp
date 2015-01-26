#pragma once

#include <exception>
#include <string>

namespace cta {
namespace common {

/**
 * Class representing an exception.
 */
class Exception: public std::exception {
public:

  /**
   * Constructor.
   */
  Exception(const std::string &message);

  /**
   * Destructor.
   */
  ~Exception() throw();

  /**
   * Returns a description of what went wrong.
   */
  const char *what() const throw();

private:

  std::string m_message;

}; // class Exception

} // namespace common
} // namespace cta
