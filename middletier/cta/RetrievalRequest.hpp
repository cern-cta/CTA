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
   * @param id The identifier of the request.
   * @param user The identity of the user who made the request.
   * @param creationTime Optionally the absolute time at which the user request
   * was created.  If no value is given then the current time is used.
   */
  RetrievalRequest(const uint64_t priority, const std::string &id,
    const SecurityIdentity &user, const time_t creationTime = time(NULL));

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
