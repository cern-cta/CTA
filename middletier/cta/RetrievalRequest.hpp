#pragma once

#include "cta/UserRequest.hpp"

#include <stdint.h>

namespace cta {

/**
 * Abstract class representing a user request to retrieve some data.
 */
class RetrievalRequest: public UserRequest {
public:

  /**
   * Constructor.
   */
  RetrievalRequest();

  /**
   * Destructor.
   */
  virtual ~RetrievalRequest() throw();

  /**
   * Constructor.
   *
   * @param priority The priority of the request.
   */
  RetrievalRequest(const uint64_t priority);

  /**
   * Returns the priority of the request.
   *
   * @return The priority of the request.
   */
  uint64_t getPriority() const throw();

private:

  /**
   * The priority of the request.
   */
  uint64_t m_priority;

}; // class RetrievalRequest

} // namespace cta
