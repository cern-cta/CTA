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
  virtual ~RetrievalRequest() throw() = 0;

  /**
   * Constructor.
   *
   * @param id The identifier of the request.
   * @param priority The priority of the request.
   * @param user The identity of the user who made the request.
   * @param creationTime Optionally the absolute time at which the user request
   * was created.  If no value is given then the current time is used.
   */
  RetrievalRequest(
    const std::string &id,
    const uint64_t priority,
    const SecurityIdentity &user,
    const time_t creationTime = time(NULL));

}; // class RetrievalRequest

} // namespace cta
