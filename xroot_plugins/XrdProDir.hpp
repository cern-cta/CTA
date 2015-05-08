/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include "cta/SqliteMiddleTierUser.hpp"
#include "XrdSec/XrdSecEntity.hh"
#include "XrdSfs/XrdSfsInterface.hh"

#include <iostream>

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
  cta::DirIterator m_itor;
  
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
