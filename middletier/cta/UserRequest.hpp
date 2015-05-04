#pragma once

#include "cta/SecurityIdentity.hpp"

#include <string>
#include <time.h>

namespace cta {

/**
 * Abstract class representing a user request.
 */
class UserRequest {
public:

  /**
   * Constructor.
   */
  UserRequest();

  /**
   * Destructor.
   */
  virtual ~UserRequest() throw() = 0;

  /**
   * Constructor.
   *
   * @param id The identifier of the request.
   * @param user The identity of the user who made the request.
   * @param creationTime Optionally the absolute time at which the user request
   * was created.  If no value is given then the current time is used.
   */
  UserRequest(const std::string &id, const SecurityIdentity &user,
    const time_t creationTime = time(NULL));

  /**
   * Returns the identifier of the request.
   *
   * @return The identifier of the request.
   */
  const std::string &getId() const throw();

  /**
   * Returns the identity of the user who made the request.
   *
   * @return The identity of the user who made the request.
   */
  const SecurityIdentity &getUser() const throw();

  /**
   * Returns the time at which the user request was created.
   *
   * @return The time at which the user request was created.
   */
  time_t getCreationTime() const throw();

private:

  /**
   * The identifier of the request.
   */
  std::string m_id;

  /**
   * The identity of the user who made the request.
   */
  SecurityIdentity m_user;

  /**
   * The time at which the user request was created.
   */
  time_t m_creationTime;

}; // class UserRequest

} // namespace cta
