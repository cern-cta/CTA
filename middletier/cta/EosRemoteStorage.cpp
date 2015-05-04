#include "cta/EosRemoteStorage.hpp"
#include "cta/Exception.hpp"

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::EosRemoteStorage::~EosRemoteStorage() throw() {
}

//------------------------------------------------------------------------------
// fileExists
//------------------------------------------------------------------------------
bool cta::EosRemoteStorage::fileExists(const std::string &remoteFile) {
  throw  Exception("EosRemoteStorage::fileExists() not implemented");
}

//------------------------------------------------------------------------------
// rename
//------------------------------------------------------------------------------
void cta::EosRemoteStorage::rename(const std::string &remoteFile,
  const std::string &newRemoteFile) {
  throw Exception("EosRemoteStorage::rename() not implemented");
}
