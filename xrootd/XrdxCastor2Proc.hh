/*******************************************************************************
 *                      XrdxCastor2Proc.hh
 *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2012  CERN
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
 * @author Elvin Sindrilaru & Andreas Peters
 ******************************************************************************/

#ifndef __XCASTOR2_PROC__
#define __XCASTOR2_PROC__

/*-----------------------------------------------------------------------------*/
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <dirent.h>
/*-----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucString.hh"
#include "XrdOuc/XrdOucHash.hh"
/*-----------------------------------------------------------------------------*/


//------------------------------------------------------------------------------
// Class XrdxCastor2ProcFile
//------------------------------------------------------------------------------
class XrdxCastor2ProcFile
{
public:
 
  XrdxCastor2ProcFile(const char* name, bool syncit=false) {
    fname = name;
    fd=0;
    procsync = syncit;
    lastwrite=0;
  };
  
  virtual ~XrdxCastor2ProcFile() {Close();};

  bool Open();
  bool Close() { if (fd>=0) close(fd); return true; }
  bool Write(long long val, int writedelay=0);
  bool Write(double val, int writedelay=0);
  bool Write(const char* str, int writedelay=0);
  bool WriteKeyVal(const char*        key, 
                   unsigned long long value, 
                   int                writedelay, 
                   bool               truncate=0);
  long long Read();
  bool Read(XrdOucString &str);

private:

  int fd;
  XrdOucString fname;
  bool procsync;
  time_t lastwrite;
};


//------------------------------------------------------------------------------
// Class XrdxCastor2Proc
//------------------------------------------------------------------------------
class XrdxCastor2Proc
{
public:

  XrdxCastor2Proc(const char* procdir, bool syncit) { 
    procdirectory = procdir; 
    procsync = syncit;
  };

  virtual ~XrdxCastor2Proc() {};

  XrdxCastor2ProcFile* Handle(const char* name);

  bool Open() {
    XrdOucString doit="mkdir -p ";
    doit+=procdirectory;
    system(doit.c_str());
    DIR* pd=opendir(procdirectory.c_str());
    if (!pd) {
      return false;
    } else {
      closedir(pd);
      return true;
    }
  }

private:
  
  bool procsync;
  XrdOucString procdirectory;
  XrdOucHash<XrdxCastor2ProcFile> files;

};
#endif

