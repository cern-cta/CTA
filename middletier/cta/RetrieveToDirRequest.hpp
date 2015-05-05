#pragma once

#include "cta/RetrievalRequest.hpp"
#include "cta/RetrieveToFileRequest.hpp"

#include <list>
#include <string>

namespace cta {

/**
 * Class representing a user request to retrieve one or more archived files to a
 * remote directory.
 */
class RetrieveToDirRquest: public RetrievalRequest {
public:

  /**
   * Constructor.
   */
  RetrieveToDirRquest();

  /**
   * Destructor.
   */
  ~RetrieveToDirRquest() throw();

  /**
   * Constructor.
   *
   * @param remoteDir The URL of the destination remote directory.
   * @param id The identifier of the request.
   * @param priority The priority of the request.
   * @param user The identity of the user who made the request.
   * @param creationTime Optionally the absolute time at which the user request
   * was created.  If no value is given then the current time is used.
   */
  RetrieveToDirRquest(
    const std::string &remoteDir,
    const std::string &id,
    const uint64_t priority,
    const SecurityIdentity &user,
    const time_t creationTime = time(NULL));

  /**
   * Returns the URL of the destination remote directory.
   *
   * @return The URL of the destination remote directory.
   */
  const std::string &getRemoteDir() const throw();

  /**
   * Returns the list of the individual retrieve to file requests that make up
   * this retrieve to directory request.
   *
   * @return the list of the individual retrieve to file requests that make up
   * this retrieve to directory request.
   */
  const std::list<RetrieveToFileRequest> &getRetrieveToFileRequests() const
    throw();

  /**
   * Returns the list of the individual retrieve to file requests that make up
   * this retrieve to directory request.
   *
   * @return the list of the individual retrieve to file requests that make up
   * this retrieve to directory request.
   */
  std::list<RetrieveToFileRequest> &getRetrieveToFileRequests() throw();

private:

  /**
   * The URL of the destination remote directory.
   */
  std::string m_remoteDir;

  /**
   * The list of the individual retrieve to file requests that make up this
   * retrieve to directory request.
   */
  std::list<RetrieveToFileRequest> m_retrieveToFileRequests;

}; // class RetrieveToDirRquest

} // namespace cta
