/*******************************************************************************
 *                      XrdxCastor2Ofs.hh
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
 * @author Castor Dev team, castor-dev@cern.ch
 * @author Castor Dev team, castor-dev@cern.ch
 *
 ******************************************************************************/

#pragma once

/*-----------------------------------------------------------------------------*/
#include <map>
#include "pwd.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <zlib.h>
/*-----------------------------------------------------------------------------*/
#include "movers/moverclose.h"
/*-----------------------------------------------------------------------------*/
#include "XrdNet/XrdNetSocket.hh"
#include "XrdOuc/XrdOucEnv.hh"
#include "XrdOuc/XrdOucString.hh"
#include "XrdOfs/XrdOfs.hh"
#include "XrdOfs/XrdOfsTrace.hh"
#include "XrdSfs/XrdSfsInterface.hh"
#include "XrdSys/XrdSysTimer.hh"
#include "XrdSys/XrdSysPthread.hh"
#include "XrdClient/XrdClientAdmin.hh"
/*-----------------------------------------------------------------------------*/
#include "XrdxCastor2Logging.hpp"
/*-----------------------------------------------------------------------------*/


//~ Forward declaration
class XrdxCastor2Ofs2StagerJob;


//! TpcInfo structure containing informationn about a third-party transfer
struct TpcInfo
{
  std::string path;   ///< castor lfn path
  std::string org;    ///< TPC origin e.g. user@host
  std::string opaque; ///< opaque information
  time_t expire;      ///< entry expire timestamp
};


//------------------------------------------------------------------------------
//! Class XrdxCastor2OfsFile
//------------------------------------------------------------------------------
class XrdxCastor2OfsFile : public XrdOfsFile, public LogId
{
  public:

    //--------------------------------------------------------------------------
    //! Constuctor
    //--------------------------------------------------------------------------
    XrdxCastor2OfsFile(const char* user, int MonID = 0);


    //--------------------------------------------------------------------------
    //! Destructor
    //--------------------------------------------------------------------------
    virtual ~XrdxCastor2OfsFile();


    //--------------------------------------------------------------------------
    //! Open file
    //--------------------------------------------------------------------------
    int open(const char* fileName,
             XrdSfsFileOpenMode openMode,
             mode_t createMode,
             const XrdSecEntity* client,
             const char* opaque = 0);


    //--------------------------------------------------------------------------
    //! Close file
    //--------------------------------------------------------------------------
    int close();


    //--------------------------------------------------------------------------
    //! Read from file
    //--------------------------------------------------------------------------
    int read(XrdSfsFileOffset fileOffset,   // Preread only
             XrdSfsXferSize amount);


    //--------------------------------------------------------------------------
    //! Read from file
    //--------------------------------------------------------------------------
    XrdSfsXferSize read(XrdSfsFileOffset fileOffset,
                        char*            buffer,
                        XrdSfsXferSize   buffer_size);


    //--------------------------------------------------------------------------
    //! Read from file
    //--------------------------------------------------------------------------
    int read(XrdSfsAio* aioparm);


    //--------------------------------------------------------------------------
    //! Write to file
    //--------------------------------------------------------------------------
    XrdSfsXferSize write(XrdSfsFileOffset fileOffset,
                         const char* buffer,
                         XrdSfsXferSize buffer_size);


    //--------------------------------------------------------------------------
    //! Write to file
    //--------------------------------------------------------------------------
    int write(XrdSfsAio* aioparm);
 
  private:

    //--------------------------------------------------------------------------
    //! TPC flags - indicating the current access type
    //--------------------------------------------------------------------------
    struct TpcFlag
    {
      enum Flag
      {
        kTpcNone     = 0, ///< no TPC access
        kTpcSrcSetup = 1, ///< access setting up a source TPC access
        kTpcDstSetup = 2, ///< access setting up a destination TPC access
        kTpcSrcRead  = 3, ///< read access from a TPC destination
        kTpcSrcCanDo = 4  ///< read access to evaluate if source is available
      };
    };

    //--------------------------------------------------------------------------
    //! Prepare TPC transfer - do the necessary operations need to support
    //! native TPC like saving the tpc.key and tpc.org for the rendez-vous etc.
    //!
    //! @param path castor pfn value
    //! @param opaque opaque information
    //! @param client client identity
    //!
    //! @return SFS_OK if successful, otherwise SFS_ERROR.
    //--------------------------------------------------------------------------
    int PrepareTPC(XrdOucString& path,
                   XrdOucString& opaque,
                   const XrdSecEntity* client);


    //--------------------------------------------------------------------------
    //! Verify checksum
    //--------------------------------------------------------------------------
    bool VerifyChecksum();


    //--------------------------------------------------------------------------
    //! Extract transfer info from the opaque data. This includes the port where
    //! we need to connect to the disk manager and the request uuid.
    //!
    //! @param env_opaque env containing opaque information
    //!
    //! @return SFS_OK if successful, otherwise SFS_ERROR.
    //--------------------------------------------------------------------------
    int ExtractTransferInfo(XrdOucEnv& env_opaque);


    static const int sKeyExpiry; ///< validity time of a tpc key
    XrdOucEnv* mEnvOpaque; ///< initial opaque information
    bool mIsRW; ///< file opened for writing
    bool mHasWrite; ///< mark is file has writes
    bool mViaDestructor; ///< mark close via destructor - not properly closed
    std::string mReqId; ///< request id received from the redirector
    unsigned int mAdlerXs; ///< adler checksum
    bool mHasAdlerErr; ///< mark if there was an adler error
    bool mHasAdler; ///< mark if it has adler xs computed
    XrdSfsFileOffset mAdlerOffset; ///< current adler offset
    XrdOucString mXsValue; ///< checksum value
    XrdOucString mXsType; ///< checksum type: adler, crc32c etc.
    bool mIsClosed; ///< make when file is closed
    struct stat mStatInfo; ///< file stat info
    std::string mTpcKey; ///< tpc key allocated to this file
    TpcFlag::Flag mTpcFlag ;; ///< tpc flag to identify the access type
    int mDiskMgrPort; ///< disk manager port where to connect
};


//------------------------------------------------------------------------------
//! Class XrdxCastor2OfsDirectory
//------------------------------------------------------------------------------
class XrdxCastor2OfsDirectory : public XrdOfsDirectory
{
  public:

    //--------------------------------------------------------------------------
    //! Constructor
    //--------------------------------------------------------------------------
    XrdxCastor2OfsDirectory(const char* user, int MonID = 0) :
      XrdOfsDirectory(user, MonID)  { }


    //--------------------------------------------------------------------------
    //! Destructor
    //--------------------------------------------------------------------------
    virtual ~XrdxCastor2OfsDirectory() { }
};


//------------------------------------------------------------------------------
//! Class XrdxCastor2Ofs
//------------------------------------------------------------------------------
class XrdxCastor2Ofs : public XrdOfs, public LogId
{
    friend class XrdxCastor2OfsDirectory;
    friend class XrdxCastor2OfsFile;

  public:

    //--------------------------------------------------------------------------
    //! Constructor
    //--------------------------------------------------------------------------
    XrdxCastor2Ofs();


    //--------------------------------------------------------------------------
    //! Destructor
    //--------------------------------------------------------------------------
    virtual ~XrdxCastor2Ofs() {}


    //--------------------------------------------------------------------------
    //! Configure function
    //--------------------------------------------------------------------------
    int Configure(XrdSysError& error);


    //--------------------------------------------------------------------------
    //! Create new directory object
    //--------------------------------------------------------------------------
    XrdSfsDirectory* newDir(char* user = 0, int MonID = 0)
    {
      return (XrdSfsDirectory*) new XrdxCastor2OfsDirectory(user, MonID);
    }


    //--------------------------------------------------------------------------
    //! Create new file object
    //--------------------------------------------------------------------------
    XrdSfsFile* newFile(char* user = 0, int MonID = 0)
    {
      return (XrdSfsFile*) new XrdxCastor2OfsFile(user, MonID);
    }


    //--------------------------------------------------------------------------
    //! Chmod - masked
    //--------------------------------------------------------------------------
    int chmod(const char* path,
              XrdSfsMode Mode,
              XrdOucErrInfo& out_error,
              const XrdSecEntity* client,
              const char* opaque = 0)
    {
      EPNAME("chmod");
      return Emsg(epname, out_error, ENOSYS, epname, path);
    }


    //--------------------------------------------------------------------------
    //! Exists - masked
    //--------------------------------------------------------------------------
    int exists(const char* path,
               XrdSfsFileExistence& exists_flag,
               XrdOucErrInfo& out_error,
               const XrdSecEntity* client,
               const char* opaque = 0)
    {
      EPNAME("exists");
      return Emsg(epname, out_error, ENOSYS, epname, path);
    }


    //--------------------------------------------------------------------------
    //! fsctl - masked
    //--------------------------------------------------------------------------
    int fsctl(const int cmd,
              const char* args,
              XrdOucErrInfo& out_error,
              const XrdSecEntity* client)
    {
      EPNAME("fsctl");
      return Emsg(epname, out_error, ENOSYS, epname);
    }


    //--------------------------------------------------------------------------
    //! Mkdir - masked
    //--------------------------------------------------------------------------
    int mkdir(const char* path,
              XrdSfsMode Mode,
              XrdOucErrInfo& out_error,
              const XrdSecEntity* client,
              const char* opaque = 0)
    {
      EPNAME("mkdir");
      return Emsg(epname, out_error, ENOSYS, epname, path);
    }


    //--------------------------------------------------------------------------
    //! prepare - masked
    //--------------------------------------------------------------------------
    int prepare(XrdSfsPrep& pargs,
                XrdOucErrInfo& out_error,
                const XrdSecEntity* client = 0)
    {
      EPNAME("prepare");
      return Emsg(epname, out_error, ENOSYS, epname);
    }


    //--------------------------------------------------------------------------
    //! remdir - masked
    //--------------------------------------------------------------------------
    int remdir(const char* path,
               XrdOucErrInfo& out_error,
               const XrdSecEntity* client,
               const char* info = 0)
    {
      return Emsg("remdir", out_error, ENOSYS, path);
    }


    //--------------------------------------------------------------------------
    //! rename - masked
    //--------------------------------------------------------------------------
    int rename(const char* oldFileName,
               const char* newFileName,
               XrdOucErrInfo& out_error,
               const XrdSecEntity* client,
               const char* infoO = 0,
               const char* infoN = 0)
    {
      EPNAME("rename");
      return Emsg(epname, out_error, ENOSYS, epname, oldFileName);
    }


    //--------------------------------------------------------------------------
    //! Remove file - masked
    //--------------------------------------------------------------------------
    int rem(const char* path,
            XrdOucErrInfo& out_error,
            const XrdSecEntity* client,
            const char* info = 0)
    {
      EPNAME("rem");
      return Emsg(epname, out_error, ENOSYS, epname, path);
    }


    //--------------------------------------------------------------------------
    //! Stat
    //--------------------------------------------------------------------------
    int stat(const char* Name,
             struct stat* buf,
             XrdOucErrInfo& out_error,
             const XrdSecEntity* client,
             const char* opaque = 0);


    //--------------------------------------------------------------------------
    //! Set the log level for the xrootd daemon
    //!
    //! @param logLevel new loglevel to be set
    //!
    //--------------------------------------------------------------------------
    void SetLogLevel(int logLevel);


    bool doPOSC; ///< 'Persistency on successful close' flag
    XrdSysMutex mTpcMapMutex; ///< mutex to protect access to the TPC map
    std::map<std::string, struct TpcInfo> mTpcMap; ///< TPC map of kety to lfn

  private:

    int mLogLevel; ///< log level from configuration file
    XrdSysError* Eroute; ///< error object
};

extern XrdxCastor2Ofs* gSrv; ///< global diskserver OFS handle

