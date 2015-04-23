#pragma once

#include "cta/UserRequest.hpp"

#include <stdint.h>

namespace cta {

/**
 * Abstract class representing a user request to archive some data.
 */
class ArchivalRequest: public UserRequest {
public:

  /**
   * Constructor.
   */
  ArchivalRequest();

  /**
   * Destructor.
   */
  virtual ~ArchivalRequest() throw() = 0;

  /**
   * Constructor.
   *
   * @param tapePoolName The name of the destination tape pool.
   * @param priority The priority of the request.
   */
  ArchivalRequest(const std::string &tapePoolName, const uint64_t priority);

  /**
   * Returns the name of the destination tape pool.
   *
   * @return The name of the destination tape pool.
   */
  const std::string &getTapePoolName() const throw();

  /**
   * Returns the priority of the request.
   *
   * @return The priority of the request.
   */
  uint64_t getPriority() const throw();

private:

  /**
   * The name of the destination tape pool.
   */
  std::string m_tapePoolName;

  /**
   * The priority of the request.
   */
  uint64_t m_priority;

}; // class ArchivalRequest

} // namespace cta
