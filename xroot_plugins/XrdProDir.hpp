#pragma once

#include <iostream>

#include "XrdSec/XrdSecEntity.hh"
#include "XrdSfs/XrdSfsInterface.hh"

#include "libs/middletier/SqliteMiddleTierUser.hpp"

class XrdProDir : public XrdSfsDirectory
{
public:
  XrdOucErrInfo error;
  virtual int open(const char *path, const XrdSecEntity *client = 0, const char *opaque = 0);
  virtual const char *nextEntry();
  virtual int close();
  virtual const char *FName();
  XrdProDir(cta::SqliteMiddleTierUser &userApi, const char *user=0, int MonID=0);
  virtual ~XrdProDir();
protected:
  
  /**
   * Iterator holding contents of the directory
   */
  cta::DirectoryIterator m_itor;
  
  /**
   * Pointer to the user API object
   */
  cta::SqliteMiddleTierUser &m_userApi;
  
  /**
   * Checks whether client has correct permissions and fills the UserIdentity structure
   * 
   * @param req     parsed request
   * @param requester The structure to be filled
   * @return SFS_OK in case check is passed, SFS_ERROR otherwise
   */
  int checkClient(const XrdSecEntity *client, cta::SecurityIdentity &requester);
}; // class XrdProDir
