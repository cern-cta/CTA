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
 *
 ******************************************************************************/

#pragma once

/*-----------------------------------------------------------------------------*/
#include <map>
#include <set>
/*-----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucEnv.hh"
#include "XrdOuc/XrdOucString.hh"
#include "XrdOfs/XrdOfs.hh"
#include "XrdOfs/XrdOfsTrace.hh"
#include "XrdSfs/XrdSfsInterface.hh"
#include "XrdSys/XrdSysPthread.hh"
/*-----------------------------------------------------------------------------*/
#include "XrdxCastor2Logging.hpp"
/*-----------------------------------------------------------------------------*/

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

  //----------------------------------------------------------------------------
  //! Constuctor
  //----------------------------------------------------------------------------
  XrdxCastor2OfsFile(const char* user, int MonID = 0);


  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~XrdxCastor2OfsFile();


  //----------------------------------------------------------------------------
  //! Open file
  //----------------------------------------------------------------------------
  int open(const char* fileName,
	   XrdSfsFileOpenMode openMode,
	   mode_t createMode,
	   const XrdSecEntity* client,
	   const char* opaque = 0);


  //----------------------------------------------------------------------------
  //! Close file
  //----------------------------------------------------------------------------
  int close();


  //----------------------------------------------------------------------------
  //! Read from file
  //----------------------------------------------------------------------------
  int read(XrdSfsFileOffset fileOffset,   // Preread only
	   XrdSfsXferSize amount);


  //----------------------------------------------------------------------------
  //! Read from file
  //----------------------------------------------------------------------------
  XrdSfsXferSize read(XrdSfsFileOffset fileOffset,
		      char* buffer,
		      XrdSfsXferSize buffer_size);


  //----------------------------------------------------------------------------
  //! Read from file
  //----------------------------------------------------------------------------
  int read(XrdSfsAio* aioparm);


  //----------------------------------------------------------------------------
  //! Write to file
  //----------------------------------------------------------------------------
  XrdSfsXferSize write(XrdSfsFileOffset fileOffset,
		       const char* buffer,
		       XrdSfsXferSize buffer_size);


  //----------------------------------------------------------------------------
  //! Write to file
  //----------------------------------------------------------------------------
  int write(XrdSfsAio* aioparm);

 private:

  //----------------------------------------------------------------------------
  //! TPC flags - indicating the current access type
  //----------------------------------------------------------------------------
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

  //----------------------------------------------------------------------------
  //! Prepare TPC transfer - do the necessary operations need to support
  //! native TPC like saving the tpc.key and tpc.org for the rendez-vous etc.
  //!
  //! @param path castor pfn value
  //! @param opaque opaque information
  //! @param client client identity
  //!
  //! @return SFS_OK if successful, otherwise SFS_ERROR
  //----------------------------------------------------------------------------
  int PrepareTPC(XrdOucString& path,
		 XrdOucString& opaque,
		 const XrdSecEntity* client);


  //----------------------------------------------------------------------------
  //! Verify checksum - check that the checksum value obtained by reading the
  //! whole file matches the one saved in the extended attributes of the file.
  //!
  //! @return True if checksums match, otherwise false
  //----------------------------------------------------------------------------
  bool VerifyChecksum();


  //----------------------------------------------------------------------------
  //! Extract transfer info from the opaque data. This includes the port where
  //! we need to connect to the disk manager and the request uuid.
  //!
  //! @param env_opaque env containing opaque information
  //!
  //! @return SFS_OK if successful, otherwise SFS_ERROR
  //----------------------------------------------------------------------------
  int ExtractTransferInfo(XrdOucEnv& env_opaque);

  //----------------------------------------------------------------------------
  //! Build transfer identifier
  //!
  //! @param client Client identity from which we extract the tident and add
  //!               to it the FQDN of the client origin therefore getting:
  //!               userid.pid.fd@origin_host
  //! @param env XrdOucEnv containing the opaque information from the open req.
  //----------------------------------------------------------------------------
  void BuildTransferId(const XrdSecEntity* client, XrdOucEnv* env);

  //----------------------------------------------------------------------------
  //! Sanitize the opaque information
  //!
  //! @param opque string opaque information with tokens separated by '&',
  //!        which will hold the sanitized info at the end of the function
  //! @param is_tpc true if this is a TPC transfer, otherwise false
  //!
  //----------------------------------------------------------------------------
  void SanitizeOpaque(XrdOucString& opaque, bool& is_tpc);


  static const int sKeyExpiry; ///< validity time of a tpc key
  struct stat mStatInfo; ///< file stat info
  bool mIsClosed; ///< mark when file is closed
  bool mIsRW; ///< file opened for writing
  bool mHasWrite; ///< mark is file has writes
  bool mViaDestructor; ///< mark close via destructor - not properly closed
  bool mHasAdlerErr; ///< mark if there was an adler error
  bool mHasAdler; ///< mark if it has adler xs computed
  unsigned int mAdlerXs; ///< adler checksum
  XrdSfsFileOffset mAdlerOffset; ///< current adler offset
  int mDiskMgrPort; ///< disk manager port where to connect
  std::string mXsValue; ///< checksum value
  std::string mXsType; ///< checksum type: adler, crc32c etc.
  std::string mTpcKey; ///< tpc key allocated to this file
  std::string mReqId; ///< request id received from the redirector
  std::string mTransferId; ///< unique id identifying current transfer
  TpcFlag::Flag mTpcFlag; ///< tpc flag to identify the access type
  XrdOucEnv* mEnvOpaque; ///< initial opaque information
};


//------------------------------------------------------------------------------
//! Class XrdxCastor2OfsDirectory
//------------------------------------------------------------------------------
class XrdxCastor2OfsDirectory : public XrdOfsDirectory
{
 public:

  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  XrdxCastor2OfsDirectory(const char* user, int MonID = 0) :
      XrdOfsDirectory(user, MonID)  { }


  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
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

  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  XrdxCastor2Ofs();


  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~XrdxCastor2Ofs() { }


  //----------------------------------------------------------------------------
  //! Configure function - parse the xrd.cf.server file
  //!
  //! @param error error object
  //! @param envP env holding implementation specific information
  //!
  //! @return SFS_OK if successful, otherwise SFS_ERROR
  //----------------------------------------------------------------------------
  int Configure(XrdSysError& error, XrdOucEnv* envP);


  //----------------------------------------------------------------------------
  //! Create new directory object
  //----------------------------------------------------------------------------
  XrdSfsDirectory* newDir(char* user = 0, int MonID = 0)
  {
    return (XrdSfsDirectory*) new XrdxCastor2OfsDirectory(user, MonID);
  }


  //----------------------------------------------------------------------------
  //! Create new file object
  //----------------------------------------------------------------------------
  XrdSfsFile* newFile(char* user = 0, int MonID = 0)
  {
    return (XrdSfsFile*) new XrdxCastor2OfsFile(user, MonID);
  }


  //----------------------------------------------------------------------------
  //! Chmod - masked
  //----------------------------------------------------------------------------
  int chmod(const char* path,
	    XrdSfsMode Mode,
	    XrdOucErrInfo& out_error,
	    const XrdSecEntity* client,
	    const char* opaque = 0)
  {
    EPNAME("chmod");
    return Emsg(epname, out_error, ENOSYS, epname, path);
  }


  //----------------------------------------------------------------------------
  //! Exists - masked
  //----------------------------------------------------------------------------
  int exists(const char* path,
	     XrdSfsFileExistence& exists_flag,
	     XrdOucErrInfo& out_error,
	     const XrdSecEntity* client,
	     const char* opaque = 0)
  {
    EPNAME("exists");
    return Emsg(epname, out_error, ENOSYS, epname, path);
  }


  //----------------------------------------------------------------------------
  //! fsctl - masked
  //----------------------------------------------------------------------------
  int fsctl(const int cmd,
	    const char* args,
	    XrdOucErrInfo& out_error,
	    const XrdSecEntity* client)
  {
    EPNAME("fsctl");
    return Emsg(epname, out_error, ENOSYS, epname);
  }


  //----------------------------------------------------------------------------
  //! Mkdir - masked
  //----------------------------------------------------------------------------
  int mkdir(const char* path,
	    XrdSfsMode Mode,
	    XrdOucErrInfo& out_error,
	    const XrdSecEntity* client,
	    const char* opaque = 0)
  {
    EPNAME("mkdir");
    return Emsg(epname, out_error, ENOSYS, epname, path);
  }


  //----------------------------------------------------------------------------
  //! Prepare - masked
  //----------------------------------------------------------------------------
  int prepare(XrdSfsPrep& pargs,
	      XrdOucErrInfo& out_error,
	      const XrdSecEntity* client = 0)
  {
    EPNAME("prepare");
    return Emsg(epname, out_error, ENOSYS, epname);
  }


  //----------------------------------------------------------------------------
  //! Remdir - masked
  //----------------------------------------------------------------------------
  int remdir(const char* path,
	     XrdOucErrInfo& out_error,
	     const XrdSecEntity* client,
	     const char* info = 0)
  {
    return Emsg("remdir", out_error, ENOSYS, path);
  }


  //----------------------------------------------------------------------------
  //! Rename - masked
  //----------------------------------------------------------------------------
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


  //----------------------------------------------------------------------------
  //! Remove file - masked
  //----------------------------------------------------------------------------
  int rem(const char* path,
	  XrdOucErrInfo& out_error,
	  const XrdSecEntity* client,
	  const char* info = 0)
  {
    EPNAME("rem");
    return Emsg(epname, out_error, ENOSYS, epname, path);
  }


  //----------------------------------------------------------------------------
  //! Stat
  //----------------------------------------------------------------------------
  int stat(const char* Name,
	   struct stat* buf,
	   XrdOucErrInfo& out_error,
	   const XrdSecEntity* client,
	   const char* opaque = 0);


  //----------------------------------------------------------------------------
  //! Perform a filesystem control operation
  //!
  //! @param  cmd    - The operation to be performed:
  //!                  SFS_FSCTL_PLUGIO  Return implementation dependent data
  //! @param  args   - Arguments specific to cmd.
  //!                  SFS_FSCTL_PLUGIO
  //! @param  eInfo  - The object where error info or results are to be returned.
  //! @param  client - Client's identify (see common description).
  //!
  //! @return SFS_OK   a null response is sent.
  //! @return SFS_DATA error.code    length of the data to be sent.
  //!                  error.message contains the data to be sent.
  //! @return SFS_STARTED Operation started result will be returned via callback.
  //!                  Valid only for for SFS_FSCTL_LOCATE, SFS_FSCTL_STATFS, and
  //!                  SFS_FSCTL_STATXA
  //!         o/w      one of SFS_ERROR, SFS_REDIRECT, or SFS_STALL.
  //----------------------------------------------------------------------------
  int FSctl(const int cmd,
	    XrdSfsFSctl& args,
	    XrdOucErrInfo& eInfo,
	    const XrdSecEntity* client = 0);


  //----------------------------------------------------------------------------
  //! Set the log level for the xrootd daemon
  //!
  //! @param logLevel new loglevel to be set
  //----------------------------------------------------------------------------
  void SetLogLevel(int logLevel);

  //----------------------------------------------------------------------------
  //! Decide if we notify or not the disk manager about ongoing transfers. This
  //! is used only for testing the functionality of the XRootD server in without
  //! coupling it with the rest of the CASTOR infrastructure.
  //!
  //! @return true if notifications are sent, otherwise false
  //----------------------------------------------------------------------------
  inline bool NotifyDiskMgr() const
  {
    return mNotifyDMgr;
  };

  XrdSysMutex mTpcMapMutex; ///< mutex to protect access to the TPC map
  std::map<std::string, struct TpcInfo> mTpcMap; ///< TPC map of key to lfn

 private:

  //----------------------------------------------------------------------------
  //! Add entry to the set of transfers
  //!
  //! @param entry string uniquely identifying a transfer
  //!
  //! @return True if successfully added, otherwise false
  //----------------------------------------------------------------------------
  bool AddTransfer(const std::string transfer_id);


  //----------------------------------------------------------------------------
  //! Remove entry from the set of transfers
  //!
  //! @param entry string uniquely identifying a transfer
  //!
  //! @return Number of entries erased from the set of transfers
  //----------------------------------------------------------------------------
  size_t RemoveTransfer(const std::string transfer_id);


  int mLogLevel; ///< log level from configuration file
  bool mNotifyDMgr; ///< if false skip notifying the diskmgr about transfers
  XrdSysError* Eroute; ///< error object
  XrdSysMutex mMutexTransfers; ///< mutex to protecte access to the transfer list
  std::set<std::string> mSetTransfers; ///< set of ongoing transfers
};

extern XrdxCastor2Ofs* gSrv; ///< global diskserver OFS handle
