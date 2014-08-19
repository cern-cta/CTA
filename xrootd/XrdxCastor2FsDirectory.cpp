/*******************************************************************************
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
 * @author Castor Dev team, castor-dev@cern.ch
 *
 ******************************************************************************/

/*----------------------------------------------------------------------------*/
#include "XrdOfs/XrdOfsTrace.hh"
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
  d_pnt(0),
  ds_ptn(0),
  mAutoStat(0)
{ }


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
XrdxCastor2FsDirectory::open(const char* dir_path,
                             const XrdSecEntity* client,
                             const char* info)
{
  EPNAME("opendir");
  uid_t client_uid;
  gid_t client_gid;
  XrdOucEnv Open_Env(info);
  AUTHORIZE(client, &Open_Env, AOP_Readdir, "open directory", dir_path, error);
  std::string map_dir = gMgr->NsMapping(dir_path) ;
  xcastor_debug("open directory: %s", map_dir.c_str());

  if (map_dir == "")
    return gMgr->Emsg(epname, error, ENOMEDIUM, "map filename", dir_path);

  gMgr->GetIdMapping(client, client_uid, client_gid);

  if (gMgr->mProc)
    gMgr->mStats.IncCmd();

  // Verify that this object is not already associated with an open directory
  if (dh)
    return gMgr->Emsg(epname, error, EADDRINUSE, "open directory", map_dir.c_str());

  // Open the directory and get its id
  if (!(dh = XrdxCastor2FsUFS::OpenDir(map_dir.c_str())))
    return  gMgr->Emsg(epname, error, serrno, "open directory", map_dir.c_str());

  fname = strdup(map_dir.c_str());
  return SFS_OK;
}


//------------------------------------------------------------------------------
// Read next directory entry
//------------------------------------------------------------------------------
const char*
XrdxCastor2FsDirectory::nextEntry()
{
  EPNAME("readdir");

  if (!dh)
  {
    gMgr->Emsg(epname, error, EBADF, "read directory", fname);
    return static_cast<const char*>(0);
  }

  if (gMgr->mProc)
    gMgr->mStats.IncReadd();

  if (mAutoStat)
  {
    // Read the next directory entry with stat information
    if (!(ds_ptn = XrdxCastor2FsUFS::ReadDirStat(dh)))
    {
      if (serrno != 0)
        gMgr->Emsg(epname, error, serrno, "read stat directory", fname);

      return static_cast<const char*>(0);
    }

    memset(mAutoStat, sizeof(struct stat), 0);
    mAutoStat->st_dev     = 0xcaff;
    mAutoStat->st_ino     = ds_ptn->fileid;
    mAutoStat->st_mode    = ds_ptn->filemode;
    mAutoStat->st_nlink   = ds_ptn->nlink;
    mAutoStat->st_uid     = ds_ptn->uid;
    mAutoStat->st_gid     = ds_ptn->gid;
    mAutoStat->st_rdev    = 0;     /* device type (if inode device) */
    mAutoStat->st_size    = ds_ptn->filesize;
    mAutoStat->st_blksize = 4096;
    mAutoStat->st_blocks  = ds_ptn->filesize / 4096;
    mAutoStat->st_atime   = ds_ptn->atime;
    mAutoStat->st_mtime   = ds_ptn->mtime;
    mAutoStat->st_ctime   = ds_ptn->ctime;
    xcastor_debug("dir next stat entry: %s", ds_ptn->d_name);
    return static_cast<const char*>(ds_ptn->d_name);
  }
  else
  {
    // Read the next directory entry without stat information
    if (!(d_pnt = XrdxCastor2FsUFS::ReadDir(dh)))
    {
      if (serrno != 0)
        gMgr->Emsg(epname, error, serrno, "read directory", fname);

      return static_cast<const char*>(0);
    }

    xcastor_debug("dir next entry: %s", d_pnt->d_name);
    return static_cast<const char*>(d_pnt->d_name);
  }
}


//------------------------------------------------------------------------------
// Set stat buffer to automaticaly return stat information
//------------------------------------------------------------------------------
int
XrdxCastor2FsDirectory::autoStat(struct stat* buf)
{
  EPNAME("autostat");

  // Check if this directory is actually open
  if (!dh)
    return gMgr->Emsg(epname, error, EBADF, "autostat directory");

  // Set the auto stat buffer used for bulk requests
  mAutoStat = buf;
  return SFS_OK;
}


//------------------------------------------------------------------------------
// Close directory
//------------------------------------------------------------------------------
int
XrdxCastor2FsDirectory::close()
{
  EPNAME("closedir");

  if (gMgr->mProc)
    gMgr->mStats.IncCmd();

  // Release the handle
  if (dh && XrdxCastor2FsUFS::CloseDir(dh))
  {
    gMgr->Emsg(epname, error, serrno ? serrno : EIO, "close directory", fname);
    return SFS_ERROR;
  }

  if (fname)
  {
    free(fname);
    fname = 0;
  }

  dh = (Cns_DIR*)0;
  return SFS_OK;
}
