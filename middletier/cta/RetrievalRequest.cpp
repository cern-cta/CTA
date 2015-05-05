#include "cta/RetrievalRequest.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::RetrievalRequest::RetrievalRequest() {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::RetrievalRequest::~RetrievalRequest() throw() {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::RetrievalRequest::RetrievalRequest(
  const std::string &id,
  const uint64_t priority,
  const SecurityIdentity &user,
  const time_t creationTime):
  UserRequest(id, priority, user, creationTime) {
}
