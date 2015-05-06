#include "XrdProDir.hpp"

#include <pwd.h>

//------------------------------------------------------------------------------
// checkClient
//------------------------------------------------------------------------------
int XrdProDir::checkClient(const XrdSecEntity *client, cta::SecurityIdentity &requester) {
  if(!client || !client->host || strncmp(client->host, "localhost", 9))
  {
    error.setErrInfo(ENOTSUP, "[ERROR] Operation only possible from localhost.");
    return SFS_ERROR;
  }
  struct passwd pwd;
  struct passwd *result;
  char *buf;
  long bufsize = sysconf(_SC_GETPW_R_SIZE_MAX);
  if (bufsize == -1)
  {
    bufsize = 16384;
  }
  buf = (char *)malloc((size_t)bufsize);
  if(buf == NULL)
  {
    free(buf);
    error.setErrInfo(ENOTSUP, "[ERROR] malloc of the buffer failed");
    return SFS_ERROR;
  }
  int rc = getpwnam_r(client->name, &pwd, buf, bufsize, &result);
  if(result == NULL)
  {
    if (rc == 0)
    {
      std::string response = "[ERROR] User ";
      response += client->name;
      response += " not found";
      free(buf);
      error.setErrInfo(ENOTSUP, response.c_str());
      return SFS_ERROR;
    }
    else
    {
      free(buf);
      error.setErrInfo(ENOTSUP, "[ERROR] getpwnam_r failed");
      return SFS_ERROR;
    }
  }
  std::cout << "Dir request received from client. Username: " << client->name << " uid: " << pwd.pw_uid << " gid: " << pwd.pw_gid << std::endl;
  requester.host = client->host;
  requester.user.setUid(pwd.pw_uid);
  requester.user.setGid(pwd.pw_gid);
  free(buf);
  return SFS_OK;
}

//------------------------------------------------------------------------------
// open
//------------------------------------------------------------------------------
int XrdProDir::open(const char *path, const XrdSecEntity *client, const char *opaque) {
  cta::SecurityIdentity requester;
  int checkResult = checkClient(client, requester);
  if(SFS_OK!=checkResult) {
    return checkResult;
  }
  m_itor = m_userApi.getDirContents(requester, path);
  return SFS_OK;
}

//------------------------------------------------------------------------------
// nextEntry
//------------------------------------------------------------------------------
const char * XrdProDir::nextEntry() {
  if(m_itor.hasMore()) {
    return m_itor.next().getName().c_str();
  }
  else {
    return NULL;
  }
}

//------------------------------------------------------------------------------
// close
//------------------------------------------------------------------------------
int XrdProDir::close() {
  return SFS_OK;
}

//------------------------------------------------------------------------------
// FName
//------------------------------------------------------------------------------
const char * XrdProDir::FName() {
  return NULL;
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
XrdProDir::XrdProDir(cta::SqliteMiddleTierUser &userApi, const char *user, int MonID): error(user, MonID), m_userApi(userApi) {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
XrdProDir::~XrdProDir() {
}