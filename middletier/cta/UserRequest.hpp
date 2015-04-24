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
  virtual ~UserRequest() throw();

  /**
   * Constructor.
   *
   * @param id The identifier of the request.
   * @param user The identity of the user who made the request.
   */
  UserRequest(const std::string &id, const SecurityIdentity &user);

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
