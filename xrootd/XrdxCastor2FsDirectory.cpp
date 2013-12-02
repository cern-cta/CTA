/*******************************************************************************
 *                      XrdxCastor2FsDirectory.cpp
 *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2013  CERN
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 *
 *
 * @author Andreas Peters <apeters@cern.ch>
 * @author Elvin Sindrilaru <esindril@cern.ch>
 *
 ******************************************************************************/

/*----------------------------------------------------------------------------*/
#include "XrdxCastor2FsDirectory.hpp"
#include "XrdxCastor2FsSecurity.hpp"
#include "XrdxCastor2Fs.hpp"
/*----------------------------------------------------------------------------*/


//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
XrdxCastor2FsDirectory::XrdxCastor2FsDirectory(const char* user, int MonID) :
  XrdSfsDirectory(user, MonID),
  dh(0),
  fname(0),
  d_pnt(0)
{
  // empty
}


//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
XrdxCastor2FsDirectory::~XrdxCastor2FsDirectory()
{
  if (dh) 
    close();

  if (fname)
  {
    free(fname);
    fname = 0;
  }
}


//------------------------------------------------------------------------------
// Open directory
//------------------------------------------------------------------------------
int
XrdxCastor2FsDirectory::open(const char*         dir_path,
                             const XrdSecEntity* client,
                             const char*         info)
{
  static const char* epname = "open";
  uid_t client_uid;
  gid_t client_gid;
  XrdOucEnv Open_Env(info);
  AUTHORIZE(client, &Open_Env, AOP_Readdir, "open directory", dir_path, error);
  std::string map_dir = gMgr->NsMapping(dir_path) ;
  xcastor_debug("open directory: %s", map_dir.c_str());

  if (map_dir == "")
    return XrdxCastor2Fs::Emsg(epname, error, ENOMEDIUM, "map filename", dir_path);

  gMgr->GetIdMapping(client, client_uid, client_gid);

  if (gMgr->mProc)
    gMgr->mStats.IncCmd();

  // Verify that this object is not already associated with an open directory
  if (dh) return XrdxCastor2Fs::Emsg(epname, error, EADDRINUSE,
                                     "open directory", map_dir.c_str());

  // Open the directory and get it's id
  if (!(dh = XrdxCastor2FsUFS::OpenDir(map_dir.c_str())))
    return  XrdxCastor2Fs::Emsg(epname, error, serrno, "open directory", map_dir.c_str());

  fname = strdup(map_dir.c_str());
  return SFS_OK;
}


//------------------------------------------------------------------------------
// Read next directory entry
//------------------------------------------------------------------------------
const char*
XrdxCastor2FsDirectory::nextEntry()
{
  static const char* epname = "nextEntry";

  if (!dh)
  {
    XrdxCastor2Fs::Emsg(epname, error, EBADF, "read directory", fname);
    return (const char*)0;
  }

  if (gMgr->mProc)
    gMgr->mStats.IncReadd();

  // Read the next directory entry
  if (!(d_pnt = XrdxCastor2FsUFS::ReadDir(dh)))
  {
    if (serrno != 0)
      XrdxCastor2Fs::Emsg(epname, error, serrno, "read directory", fname);
    
    return (const char*)0;
  }

  entry = d_pnt->d_name;
  xcastor_debug("dir next entry: %s", entry.c_str());
  return (const char*)(entry.c_str());
}


//------------------------------------------------------------------------------
// Close directory
//------------------------------------------------------------------------------
int
XrdxCastor2FsDirectory::close()
{
  static const char* epname = "closedir";

  if (gMgr->mProc)
    gMgr->mStats.IncCmd();
 
  // Release the handle
  if (dh && XrdxCastor2FsUFS::CloseDir(dh))
  {
    XrdxCastor2Fs::Emsg(epname, error, serrno ? serrno : EIO, "close directory", fname);
    return SFS_ERROR;
  }

  if (fname) 
  {
    free(fname);
    fname = 0;
  }
  
  dh = (DIR*)0;
  return SFS_OK;
}

