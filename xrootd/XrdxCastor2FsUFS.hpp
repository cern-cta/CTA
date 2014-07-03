/*******************************************************************************
 *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2013 CERN
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

#ifndef __XCASTOR_FSUFS_HH__
#define __XCASTOR_FSUFS_HH__

/*-----------------------------------------------------------------------------*/
#include "serrno.h"
#include "Cns_api.h"
/*-----------------------------------------------------------------------------*/


//------------------------------------------------------------------------------
//! Class XrdxCastor2UFS - Unix File System Interface - this is the interface
//! with the Castor name space, except for the open() and close() calls,
//! which deal with a local file descriptor.
//------------------------------------------------------------------------------
class XrdxCastor2FsUFS
{
  public:

    static int Chmod(const char* fn, mode_t mode)
    {
      return Cns_chmod(fn, mode);
    }

    static int Chown(const char* fn, uid_t owner, gid_t group)
    {
      return Cns_chown(fn, owner, group);
    }

    static int Lchown(const char* fn, uid_t owner, gid_t group)
    {
      return Cns_lchown(fn, owner, group);
    }

    static int Close(int fd)
    {
      return close(fd);
    }

    static int Mkdir(const char* fn, mode_t mode)
    {
      return Cns_mkdir(fn, mode);
    }

    static int Open(const char* path, int oflag, mode_t omode)
    {
      return open(path, oflag, omode);
    }

    static int Creat(const char* path, mode_t omode)
    {
      return Cns_creat(path, omode);
    }

    static int Remdir(const char* fn)
    {
      return Cns_rmdir(fn);
    }

    static int Rename(const char* ofn, const char* nfn)
    {
      return Cns_rename(ofn, nfn);
    }

    static int Statfd(int fd, struct stat* buf)
    {
      return fstat(fd, buf);
    }

    static int Statfn(const char* fn, struct Cns_filestatcs* buf)
    {
      return Cns_statcs(fn, buf);
    }

    static int Lstatfn(const char* fn, struct Cns_filestat* buf)
    {
      return Cns_lstat(fn, buf);
    }

    static int Readlink(const char* fn, char* buf, size_t bufsize)
    {
      return Cns_readlink(fn, buf, bufsize);
    }

    static int SetId(uid_t uid, gid_t gid)
    {
      return Cns_setid(uid, gid);
    }

    static int Symlink(const char* oldn, const char* newn)
    {
      return Cns_symlink(oldn, newn);
    }

    static int Unlink(const char* fn)
    {
      return Cns_unlink(fn);
    }

    static int Access(const char* fn, int mode)
    {
      return Cns_access(fn, mode);
    }

    static int Utimes(const char* /*fn*/, struct timeval* /*tvp*/)
    {
      return 0; /* return Cns_utimes(fn,tvp);*/
    }

    static Cns_DIR* OpenDir(const char* dn)
    {
      return Cns_opendir(dn);
    }

    static struct dirent* ReadDir(Cns_DIR* dp)
    {
      return Cns_readdir(dp);
    }

    static struct Cns_direnstatc* ReadDirStat(Cns_DIR* dp)
    {
      return Cns_readdirxc(dp);
    }

    static int CloseDir(Cns_DIR* dp)
    {
      return Cns_closedir(dp);
    }
};

#endif // __XCASTOR_FSUFS_HH__
