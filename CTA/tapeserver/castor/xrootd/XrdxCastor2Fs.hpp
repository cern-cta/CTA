/*******************************************************************************
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
#include <list>
#include <set>
#include <map>
/*-----------------------------------------------------------------------------*/
#include "XrdxCastor2FsStats.hpp"
#include "XrdxCastor2Acc.hpp"
#include "XrdxCastor2FsUFS.hpp"
/*-----------------------------------------------------------------------------*/
#include "XrdAcc/XrdAccAuthorize.hh"
#include "XrdSfs/XrdSfsInterface.hh"
/*-----------------------------------------------------------------------------*/

#define RFIO_NOREDEFINE

//------------------------------------------------------------------------------
//! Get authorization plugin object
//------------------------------------------------------------------------------
extern "C" XrdAccAuthorize* XrdAccAuthorizeObject(XrdSysLogger* lp,
                                                  const char* cfn,
                                                  const char* parm);

//! Forward declarations
class XrdSecEntity;
class XrdSysError;
class XrdxCastor2FsFile;
class XrdxCastor2FsDirectory;

namespace xcastor
{
  class XrdxCastorClient;
}


//------------------------------------------------------------------------------
//! Class XrdxCastor2Fs
//------------------------------------------------------------------------------
class XrdxCastor2Fs : public XrdSfsFileSystem, public LogId
{
  friend class XrdxCastor2FsFile;
  friend class XrdxCastor2FsDirectory;
  friend class XrdxCastor2FsStats;

public:

  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  XrdxCastor2Fs();


  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~XrdxCastor2Fs();


  //----------------------------------------------------------------------------
  //! Configure plugin
  //!
  //! @return 0 upon success or non zero otherwise
  //----------------------------------------------------------------------------
  virtual int Configure(XrdSysError& fsEroute);


  //----------------------------------------------------------------------------
  //! Initialisation
  //----------------------------------------------------------------------------
  virtual bool Init();


  //----------------------------------------------------------------------------
  //! Notification to filesystem when client disconnects
  //!
  //! @param client - client identity
  //----------------------------------------------------------------------------
  virtual void Disc(const XrdSecEntity* client = 0);


  //----------------------------------------------------------------------------
  //! Create new directory
  //----------------------------------------------------------------------------
  XrdSfsDirectory* newDir(char* user = 0, int MonID = 0);


  //----------------------------------------------------------------------------
  //! Create new file
  //----------------------------------------------------------------------------
  XrdSfsFile* newFile(char* user = 0, int MonID = 0);


  //----------------------------------------------------------------------------
  //! Change the mode on a file or directory
  //!
  //! @param path fully qualified name of the file tob created
  //! @param Mode POSIX mode
  //! @param out_error error information object to hold error details
  //! @param client authentication credentials, if any
  //! @param info opaque information, if any
  //!
  //! @return SFS_OK upon success and SFS_ERROR upon failure
  //----------------------------------------------------------------------------
  int chmod(const char* Name,
            XrdSfsMode Mode,
            XrdOucErrInfo& out_error,
            const XrdSecEntity* client = 0,
            const char* opaque = 0);


  //--------------------------------------------------------------------------
  //! Obtain checksum information for a file.
  //!
  //! @param  Func   - The checksum operation to be performed:
  //!                  csCalc  - (re)calculate and return the checksum value
  //!                  csGet   - return the existing checksum value, if any
  //!                  csSize  - return the size of the checksum value that
  //!                            corresponds to csName (path may be null).
  //! @param  csName - The name of the checksum value wanted.
  //! @param  path   - Pointer to the path of the file in question.
  //! @param  eInfo  - The object where error info or results are to be returned.
  //! @param  client - Client's identify (see common description).
  //! @param  opaque - Path's CGI information (see common description).
  //!
  //! @return One of SFS_OK, SFS_ERROR, or SFS_REDIRECT. When SFS_OK is returned,
  //!         eInfo should contain results, as follows:
  //!         csCalc/csGet eInfo.message - null terminated string with the checksum
  //!         csSize       eInfo.code    - size of binary checksum value.
  //--------------------------------------------------------------------------
  int chksum(csFunc Func,
             const char* csName,
             const char* path,
             XrdOucErrInfo& eInfo,
             const XrdSecEntity* client = 0,
             const char* opaque = 0);


  //----------------------------------------------------------------------------
  //! Determine if file 'path' actually exists
  //!
  //! @param path fully qualified name of the file to be tested
  //! @param file_exists address of the variable to hold the status of
  //!             'path' when success is returned. The values may be:
  //!              XrdSfsFileExistsIsDirectory - file not found but path is valid
  //!              XrdSfsFileExistsIsFile      - file found
  //!              XrdSfsFileExistsIsNo        - neither file nor directory
  //! @param error error information object holding the details
  //! @param client authentication credentials, if any
  //! @param info opaque information, if any
  //!
  //! @return SFS_OK upon success and SFS_ERROR upon failure
  //! Notes:    When failure occurs, 'file_exists' is not modified.
  //----------------------------------------------------------------------------
  int exists(const char* path,
             XrdSfsFileExistence& exists_flag,
             XrdOucErrInfo& error,
             const XrdSecEntity* client = 0,
             const char* info = 0);


  //----------------------------------------------------------------------------
  //! Determine if file 'path' actually exists - low level
  //----------------------------------------------------------------------------
  int _exists(const char* path,
              XrdSfsFileExistence& exists_flag,
              XrdOucErrInfo& error,
              const XrdSecEntity* client = 0,
              const char* info = 0);


  //----------------------------------------------------------------------------
  //! Implement file system custom command
  //----------------------------------------------------------------------------
  int fsctl(const int cmd,
            const char* args,
            XrdOucErrInfo& out_error,
            const XrdSecEntity* client = 0);


  //----------------------------------------------------------------------------
  //! Get stats information
  //----------------------------------------------------------------------------
  int getStats(char* buff, int blen)
  {
    return 0;
  }


  //----------------------------------------------------------------------------
  //! Get version of the plugin
  //----------------------------------------------------------------------------
  const char* getVersion();


  //----------------------------------------------------------------------------
  //! Create a directory entry
  //!
  //! @param path fully qualified name of the file tob created
  //! @param Mode POSIX mode setting for the directory. If the
  //!        mode contains SFS_O_MKPTH, the full path is created
  //! @param out_error error information object to hold error details
  //! @param client authentication credentials, if any
  //! @param info opaque information, if any
  //!
  //! @return SFS_OK upon success and SFS_ERROR upon failure
  //----------------------------------------------------------------------------
  int mkdir(const char* dirName,
            XrdSfsMode Mode,
            XrdOucErrInfo& out_error,
            const XrdSecEntity* client = 0,
            const char* opaque = 0);


  //----------------------------------------------------------------------------
  //! Create a directory entry - low level
  //----------------------------------------------------------------------------
  int _mkdir(const char* dirName,
             XrdSfsMode Mode,
             XrdOucErrInfo& out_error,
             const XrdSecEntity* client = 0,
             const char* opaque = 0);


  //----------------------------------------------------------------------------
  //! Stage prepare
  //----------------------------------------------------------------------------
  int stageprepare(const char* path,
                   XrdOucErrInfo& error,
                   const XrdSecEntity* client,
                   const char* info);


  //----------------------------------------------------------------------------
  //! Prepare request
  //----------------------------------------------------------------------------
  int prepare(XrdSfsPrep& pargs,
              XrdOucErrInfo& out_error,
              const XrdSecEntity* client = 0);


  //----------------------------------------------------------------------------
  //! Delete a file - stager_rm
  //!
  //! @param path fully qualified name of the file to be removed
  //! @param einfo error information object to hold error details
  //! @param client authentication credentials, if any
  //! @param info opaque information, if any
  //!
  //! @return SFS_OK upon success and SFS_ERROR upon failure
  //----------------------------------------------------------------------------
  int rem(const char* path,
          XrdOucErrInfo&  out_error,
          const XrdSecEntity* client = 0,
          const char* opaque = 0);


  //----------------------------------------------------------------------------
  //! Delete a file from the namespace
  //!
  //! @param path fully qualified name of the file to be removed
  //! @param einfo error information object to hold error details
  //! @param client authentication credentials, if any
  //! @param info opaque information, if any
  //!
  //! @return SFS_OK upon success and SFS_ERROR upon failure
  //----------------------------------------------------------------------------
  int _rem(const char* path,
           XrdOucErrInfo& out_error,
           const char* opaque = 0);


  //----------------------------------------------------------------------------
  //! Delete a directory from the namespace
  //!
  //! @param fully qualified name of the dir to be removed
  //! @param einfo error information object to hold error details
  //! @param client authentication credentials, if any
  //! @param info opaque information, if any
  //!
  //! @return SFS_OK upon success and SFS_ERROR upon failure
  //----------------------------------------------------------------------------
  int remdir(const char* path,
             XrdOucErrInfo& out_error,
             const XrdSecEntity* client = 0,
             const char* info = 0);


  //----------------------------------------------------------------------------
  //! Delete a directory from the namespace
  //!
  //! @param fully qualified name of the dir to be removed
  //! @param einfo error information object to hold error details
  //! @param client authentication credentials, if any
  //! @param info opaque information, if any
  //!
  //! @return SFS_OK upon success and SFS_ERROR upon failure
  //----------------------------------------------------------------------------
  int _remdir(const char* path,
              XrdOucErrInfo& out_error,
              const XrdSecEntity* client = 0,
              const char* opaque = 0);


  //----------------------------------------------------------------------------
  //! Renames a file/directory with name 'old_name' to 'new_name'
  //!
  //! @param old_name fully qualified name of the file to be renamed
  //! @param new_name fully qualified name that the file is to have
  //! @param error error information structure, if an error occurs
  //! @param client authentication credentials, if any
  //! @param infoO old_name opaque information, if any
  //! @param infoN new_name opaque information, if any
  //!
  //! @return SFS_OK upon success and SFS_ERROR upon failure
  //----------------------------------------------------------------------------
  int rename(const char* old_name,
             const char* new_name,
             XrdOucErrInfo& error,
             const XrdSecEntity* client = 0,
             const char* infoO = 0,
             const char* infoN = 0);


  //----------------------------------------------------------------------------
  //!  Get info on 'path' - all
  //!
  //! @param path fully qualified name of the file to be tested
  //! @param buf stat structure to hold the results
  //! @param error error information object holding the details
  //! @param client authentication credentials, if any
  //! @param info opaque information, if any
  //!
  //! @return SFS_OK upon success and SFS_ERROR upon failure
  //----------------------------------------------------------------------------
  int stat(const char* path,
           struct stat* buf,
           XrdOucErrInfo& error,
           const XrdSecEntity* client = 0,
           const char* info = 0);


  //----------------------------------------------------------------------------
  //!  Get info on 'path' - mode
  //!
  //! @param path fully qualified name of the file to be tested
  //! @param buf stat structure to hold the results
  //! @param error error information object holding the details
  //! @param client authentication credentials, if any
  //! @param info opaque information, if any
  //!
  //! @return SFS_OK upon success and SFS_ERROR upon failure
  //----------------------------------------------------------------------------
  int stat(const char* Name,
           mode_t& mode,
           XrdOucErrInfo& out_error,
           const XrdSecEntity* client = 0,
           const char* opaque = 0);


  //----------------------------------------------------------------------------
  //! Get info on 'path'
  //!
  //! @param path fully qualified name of the file to be tested
  //! @param buf stat structiure to hold the results
  //! @param error error information object holding the details
  //! @param client authentication credentials, if any
  //! @param info opaque information, if any
  //!
  //! @return SFS_OK upon success and SFS_ERROR upon failure
  //----------------------------------------------------------------------------
  int lstat(const char* path,
            struct stat* buf,
            XrdOucErrInfo& error,
            const XrdSecEntity* client = 0,
            const char* info = 0);


  //----------------------------------------------------------------------------
  //! Truncate
  //----------------------------------------------------------------------------
  int truncate(const char*, XrdSfsFileOffset, XrdOucErrInfo&,
               const XrdSecEntity*, const char*);


  //----------------------------------------------------------------------------
  //! Symbolic link
  //----------------------------------------------------------------------------
  int symlink(const char*, const char*, XrdOucErrInfo&,
              const XrdSecEntity*, const char*);


  //----------------------------------------------------------------------------
  //! Read link
  //----------------------------------------------------------------------------
  int readlink(const char* path,
               XrdOucString& linkpath,
               XrdOucErrInfo& error,
               const XrdSecEntity* client = 0,
               const char* info = 0);


  //----------------------------------------------------------------------------
  //! Access
  //----------------------------------------------------------------------------
  int access(const char* path,
             int mode,
             XrdOucErrInfo& error,
             const XrdSecEntity* client,
             const char* info);


  //----------------------------------------------------------------------------
  //! Utimes
  //----------------------------------------------------------------------------
  int utimes(const char*, struct timeval* tvp, XrdOucErrInfo&,
             const XrdSecEntity*, const char*);


  //----------------------------------------------------------------------------
  //! Compose error message
  //!
  //! @param pfx message prefix value
  //! @param einfo place to put text and error code
  //! @param ecode the error code
  //! @param op operation beegin performed
  //! @param target the target (e.g. file name )
  //----------------------------------------------------------------------------
  static int Emsg(const char* pfx,
                  XrdOucErrInfo& einfo,
                  int ecode,
                  const char* op,
                  const char* target = "");


  //----------------------------------------------------------------------------
  //! Get stager host value
  //!
  //! @return stager host
  //----------------------------------------------------------------------------
  inline const std::string& GetStagerHost() const
  {
    return mStagerHost;
  }

  static xcastor::XrdxCastorClient* msCastorClient; ///< obj dealing with async requests/responses
  static  int msTokenLockTime;  ///< specifies the grace period for client to show
                                ///< up on a disk server in seconds before the token expires
  char* ConfigFN; ///< path to config file
  XrdxCastor2Acc* mServerAcc; ///< authorization module for token encryption/decryption
  std::string mZeroProc; ///< path to a 0-byte file in the proc filesystem
  std::string mSrvTargetPort; ///< xrootd port where redirections go on the OFSs - default is 1094
  long long xCastor2FsDelayRead; ///< if true, all reads get a default delay to come back later
  long long xCastor2FsDelayWrite; ///< if true, all writes get a default dealy to come back later

protected:

  char* HostName; ///< our hostname as derived in XrdOfs
  char* HostPref; ///< our hostname as derived in XrdOfs without domain
  XrdOucString ManagerId;  ///< manager id in <host>:<port> format
  XrdOucString ManagerLocation; ///< [::<IP>]:<port>
  bool mIssueCapability; ///< attach an opaque capability for verification on the disk server
  XrdxCastor2Proc* mProc; ///< proc handling object
  XrdxCastor2FsStats mStats; ///< FS statistics

private:

  //----------------------------------------------------------------------------
  //! Cache diskserver host name with and without the domain name so that
  //! subsequent prepare requests from the diskservers are allowed to pass
  //! through
  //!
  //! @param host hostaname to be cached
  //----------------------------------------------------------------------------
  void CacheDiskServer(const std::string& hostname);


  //----------------------------------------------------------------------------
  //! Check if the diskserver hostname is known at the redirector
  //!
  //! @param hostname hostname to look for in the cache
  //!
  //! @return true if hostname knowns, otherwise false
  //----------------------------------------------------------------------------
  bool FindDiskServer(const std::string& hostname);


  //----------------------------------------------------------------------------
  //! Set the log level for the manager xrootd server
  //!
  //! @param logLevel interger representing the log level according to syslog
  //----------------------------------------------------------------------------
  void SetLogLevel(int logLevel);


  //----------------------------------------------------------------------------
  //! Get uid/gid mapping for client object. The default values are uid/gid =
  //! 99/99 (nobody/nobody)
  //!
  //! @param client client object to be mapped
  //! @param uid found uid mapping for client
  //! @param gid found gid mapping for client
  //----------------------------------------------------------------------------
  void GetIdMapping(const XrdSecEntity* client, uid_t& uid, gid_t& gid);


  //----------------------------------------------------------------------------
  //! Set the uid/gid mapping by calling Cns_SetId with the values obtained
  //! after doing the mapping of the XrdSecEntity object to uid/gid. This is
  //! a convenience method when we are do not want to return the uid/gid pair
  //! the the calling function like for GetIdMapping.
  //!
  //! @param client client object
  //----------------------------------------------------------------------------
  void SetIdentity(const XrdSecEntity* client);


  //----------------------------------------------------------------------------
  //! Set the acl for a file given the current client
  //!
  //! @param path file path
  //! @param uid user uid
  //! @param gid user gid
  //! @param isLink true if file is link
  //----------------------------------------------------------------------------
  void SetAcl(const char* path, uid_t uid, gid_t gid, bool isLink);


  //----------------------------------------------------------------------------
  //! Map input path according to policy
  //!
  //! @param input the initial input path
  //!
  //! @return the mapped path according to the directives in the xrd.cf file
  //----------------------------------------------------------------------------
  std::string NsMapping(const std::string& input);


  //----------------------------------------------------------------------------
  //! Get allowed service class for the requested path and desired svc
  //!
  //! @param path path of  the request
  //! @param svcClass desired service class by the user
  //!
  //! @return allowed service class or empty string if none alowed
  //----------------------------------------------------------------------------
  std::string GetAllowedSvc(const char* path,
                            const std::string& desired_svc);


  //----------------------------------------------------------------------------
  //! Get all allowed service classes for the requested path and desired svc
  //!
  //! @param path path of  the request
  //! @param no_hsm true if option set, otherwise false
  //!
  //! @return list of allowed service classes or NULL if none found
  //----------------------------------------------------------------------------
  std::list<std::string>* GetAllAllowedSvc(const char* path, bool& no_hsm);


  //----------------------------------------------------------------------------
  //! Create stall message
  //!
  //! @param error error text and code
  //! @param stime seconds to stall
  //! @param msg message to give
  //!
  //! @return number of seconds to stall
  //----------------------------------------------------------------------------
  int Stall(XrdOucErrInfo& error, int stime, const char* msg);


  //----------------------------------------------------------------------------
  //! Get delay for the current operation set for the entire instance by the
  //! admin. The delay value is read from the xcastor2.proc value specified
  //! in the /etc/xrd.cf.manager file.
  //!
  //! @param msg message to be returned to the client
  //! @param isRW true if delay value is requested for a write operation,
  //!        otherwise false for read operations
  //!
  //! @return the delay value specified in seconds
  //----------------------------------------------------------------------------
  int64_t GetAdminDelay(XrdOucString& msg, bool isRW);


  int mLogLevel; ///< log level from config file or set in the proc "trace" file
  std::set<std::string> mFsSet; ///< set of known diskserver hosts
  XrdSysMutex mMutexFsSet; ///< mutex for the set of known diskservers
  std::map<std::string, std::string> mNsMap; ///< namespace mapping
  //! Map path to allowed svcClasses and the no_hsm option
  std::map< std::string, std::pair<std::list<std::string>, bool> > mStageMap;
  XrdOucHash<struct passwd>* mPasswdStore; ///< cache passwd struct info
  std::map<std::string, std::string> mRoleMap; ///< user role map
  XrdSysMutex mMutexPasswd; ///< mutex for the passwd store
  XrdAccAuthorize* mAuthorization; ///< authorization service used only by ALICE
  std::string mStagerHost; ///< stager host to which requests are sent
  bool mSkipAbort; ///< if true no abort is issued on client disconnect
};

extern XrdxCastor2Fs* gMgr; ///< global instance of the redirector OFS subsystem
