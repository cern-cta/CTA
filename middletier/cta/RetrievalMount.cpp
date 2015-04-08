#include "cta/RetrievalMount.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::RetrievalMount::RetrievalMount(const std::string &id,
  const std::string &vid):
  Mount(id, vid) {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::RetrievalMount::~RetrievalMount() throw() {
}
