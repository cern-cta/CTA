/*******************************************************************************
 *                      XrdxCastor2FsFile.cpp
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
#include "XrdxCastor2Fs.hpp"
#include "XrdOss/XrdOss.hh"
#include "XrdSfs/XrdSfsAio.hh"
#include "XrdxCastorTiming.hpp"
#include "XrdxCastor2Stager.hpp"
#include "XrdxCastor2FsSecurity.hpp"
/*----------------------------------------------------------------------------*/


//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
XrdxCastor2FsFile::XrdxCastor2FsFile(const char* user, int MonID) :
  XrdSfsFile(user, MonID),
  LogId()
{
  oh = -1;
  fname = 0;
  ohp = NULL;
  ohperror = NULL;
  ohperror_cnt = 0;
}


//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
XrdxCastor2FsFile::~XrdxCastor2FsFile()
{
  if (oh) close();
  if (ohp) pclose(ohp);
  if (ohperror) pclose(ohperror);
}


//------------------------------------------------------------------------------
// Open file
//------------------------------------------------------------------------------
int
XrdxCastor2FsFile::open(const char*         path,
                        XrdSfsFileOpenMode  open_mode,
                        mode_t              Mode,
                        const XrdSecEntity* client,
                        const char*         ininfo)
{
  static const char* epname = "open";
  const char* tident = error.getErrUser();
  const char* origpath = path;
  xcastor::Timing opentiming("fileopen");
  XrdOucString info_str = (ininfo ? ininfo : "");
  const char* info;
  int qpos = 0;

  // => case where open fails and client bounces back
  if ((qpos = info_str.find("?") != STR_NPOS))
    info_str.erase(qpos);

  // If the clients send tried info, we have to erase it
  if ((qpos = info_str.find("tried") != STR_NPOS))
    info_str.erase(qpos);

  // If the clients send old redirection info, we have to erase it
  if ((qpos = info_str.find("castor2fs.sfn=")))
    info_str.erase(qpos);

  info = (ininfo ? info_str.c_str() : 0);
  xcastor_debug("path=%s, info=%s", path, (info ? info : ""));

  TIMING("START", &opentiming);
  std::string map_path = gMgr->NsMapping(path);

  if (map_path == "")
  {
    error.setErrInfo(ENOMEDIUM, "No mapping for file name");
    return SFS_ERROR;
  }

  TIMING("MAPPING", &opentiming);
  xcastor_debug("open_mode=%x, fn=%s, tident=%s opaque=%s", 
                Mode, map_path.c_str(), tident, ininfo);
  xcastor_debug("client->role=%s, client->tident=%s", client->role, client->tident);

  // Check if user is coming back for a response after a stall
  int aop = AOP_Read;
  int retc, open_flag = 0;
  struct Cns_filestatcs buf;
  int isRW = 0;
  int isRewrite = 0;
  int crOpts = (Mode & SFS_O_MKPTH ? XRDOSS_mkpath : 0);
  XrdOucEnv Open_Env(info);

  // Set the actual open mode and find mode
  if (open_mode & SFS_O_CREAT) open_mode = SFS_O_CREAT;
  else if (open_mode & SFS_O_TRUNC) open_mode = SFS_O_TRUNC;

  switch (open_mode & (SFS_O_RDONLY | SFS_O_WRONLY | 
                       SFS_O_RDWR | SFS_O_CREAT  | SFS_O_TRUNC))
  {
  case SFS_O_CREAT:
    open_flag  = O_RDWR | O_CREAT | O_EXCL;
    crOpts |= XRDOSS_new;
    isRW = 1;
    break;

  case SFS_O_TRUNC:
    open_flag |= O_RDWR | O_CREAT | O_TRUNC;
    isRW = 1;
    break;

  case SFS_O_RDONLY:
    open_flag = O_RDONLY;
    isRW = 0;
    break;

  case SFS_O_WRONLY:
    open_flag = O_WRONLY;
    isRW = 1;
    break;

  case SFS_O_RDWR:
    open_flag = O_RDWR;
    isRW = 1;
    break;

  default:
    open_flag = O_RDONLY;
    isRW = 0;
    break;
  }

  if (open_flag & O_CREAT)
    AUTHORIZE(client, &Open_Env, AOP_Create, "create", map_path.c_str(), error)
  else
    AUTHORIZE(client, &Open_Env, (isRW ? AOP_Update : AOP_Read), "open", map_path.c_str(), error)
    
  // See if the cluster is in maintenance mode and has delays configured for write/read
  XrdOucString msg_delay;
  int64_t delay = gMgr->GetAdminDelay(msg_delay, isRW);

  if (delay) 
  {
    TIMING("RETURN", &opentiming);
    opentiming.Print();
    return gMgr->Stall(error, delay, msg_delay.c_str());
  }

  TIMING("DELAYCHECK", &opentiming);

  // Prepare to create or open the file, only if this is the first time the user
  // comes with this request. If the user comes after a stall we skip this part.
  bool stall_comeback = gMgr->msCastorClient->HasSubmittedReq(map_path.c_str(), error);

  if (!stall_comeback)
  {
    if (isRW)
    {
      if (!(retc = XrdxCastor2FsUFS::Statfn(map_path.c_str(), &buf)))
      {
        if (buf.filemode & S_IFDIR)
          return XrdxCastor2Fs::Emsg(epname, error, EISDIR, "open directory as file", map_path.c_str());

        if (crOpts & XRDOSS_new)
        {
          return XrdxCastor2Fs::Emsg(epname, error, EEXIST, "open file in create mode - file exists");
        }
        else
        {
          if (open_mode == SFS_O_TRUNC)
          {
            // We delete the file because the open specified the truncate flag
            if ((retc = XrdxCastor2FsUFS::Unlink(map_path.c_str())))
            {
              return XrdxCastor2Fs::Emsg(epname, error, serrno, "remove file as requested by truncate flag",
                                         map_path.c_str());
            }
          }
          else
          {
            isRewrite = 1;
          }
        }
      }
    }
    else
    {
      // This is the read/stage case
      if ((retc = XrdxCastor2FsUFS::Statfn(map_path.c_str(), &buf)))
        return XrdxCastor2Fs::Emsg(epname, error, serrno, "stat file", map_path.c_str());
      
      if (!retc && !(buf.filemode & S_IFREG))
      {
        if (buf.filemode & S_IFDIR)
          return XrdxCastor2Fs::Emsg(epname, error, EISDIR, "open directory as file", map_path.c_str());
        else
          return XrdxCastor2Fs::Emsg(epname, error, ENOTBLK, "open not regular file", map_path.c_str());
      }
    
      // For 0 file size use the dummy "zero" file and don't redirect
      if (buf.filesize == 0)
      {
        oh = XrdxCastor2FsUFS::Open(gMgr->zeroProc.c_str(), 0, 0);
        fname = strdup(gMgr->zeroProc.c_str());
        return SFS_OK;
      }
    }
  }
  else 
  {
    xcastor_debug("skip some checks as request was previously submitted");
  }
  
  TIMING("PREOPEN", &opentiming);
  uid_t client_uid;
  gid_t client_gid;
  XrdOucString sclient_uid = "";
  XrdOucString sclient_gid = "";
  gMgr->GetIdMapping(client, client_uid, client_gid);
  sclient_uid += (int) client_uid;
  sclient_gid += (int) client_gid;

  const char* val; 
  std::string desired_svc= "default";
  std::string allowed_svc = "";
    
  // Try the user specified service class
  if ((val = Open_Env.Get("svcClass"))) 
    desired_svc = val;

  allowed_svc = gMgr->GetAllowedSvc(map_path.c_str(), desired_svc);

  if (allowed_svc.empty())
    return XrdxCastor2Fs::Emsg(epname, error, EINVAL,  "open - no valid service"
                               " class for fn=", map_path.c_str());

  // Open response structure in which we initialize rpfn2 for not scheduled access
  struct XrdxCastor2Stager::RespInfo resp_info;
  resp_info.mRedirectionPfn2 = 0;
  resp_info.mRedirectionPfn2 += ":";
  resp_info.mRedirectionPfn2 += allowed_svc.c_str();
  resp_info.mRedirectionPfn2 += ":0";
  resp_info.mRedirectionPfn2 += ":0";

  // Delay tag used to stall the current client incrementally
  XrdOucString delaytag = tident;
  delaytag += ":";
  delaytag += map_path.c_str();

  if (isRW)
  {
    if (!stall_comeback && !isRewrite)
    {
      // File doesn't exist, we have to create the path - check if we need to mkpath
      if (Mode & SFS_O_MKPTH)
      {
        XrdOucString newpath = map_path.c_str();

        while (newpath.replace("//", "/")) {};

        int rpos = STR_NPOS;

        while ((rpos = newpath.rfind("/", rpos)) != STR_NPOS)
        {
          rpos--;
          struct Cns_filestatcs cstat;
          XrdOucString spath;
          spath.assign(newpath, 0, rpos);

          if (XrdxCastor2FsUFS::Statfn(spath.c_str(), &cstat))
          {
            // Protect against misconfiguration ( all.export / ) and missing stat result on Cns_stat('/');
            if (rpos < 0)
              return XrdxCastor2Fs::Emsg(epname, error, serrno , "create path in root directory");
            
            continue;
          }
          else
          {
            // Start to create from here and finish
            int fpos = rpos + 2;

            while ((fpos = newpath.find("/", fpos)) != STR_NPOS)
            {
              XrdOucString createpath;
              createpath.assign(newpath, 0, fpos);
              xcastor_debug("creating path as uid=%i, gid=%i", (int)client_uid, (int)client_gid);
              mode_t cmask = 0;
              Cns_umask(cmask);
              fpos++;

              if (XrdxCastor2FsUFS::Mkdir(createpath.c_str(), S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH) && (serrno != EEXIST))
                return XrdxCastor2Fs::Emsg(epname, error, serrno , "create path need dir = ", createpath.c_str());
            }

            const char* file_class = 0;

            if ((file_class = Open_Env.Get("fileClass")) &&
                (Cns_chclass(newpath.c_str(), 0, (char*)file_class)))
            {
              return XrdxCastor2Fs::Emsg(epname, error, serrno , "set fileclass to ", file_class);
            }

            break;
          }
        }
      }
    }

    // For all write operations
    TIMING("PUT", &opentiming);
    int status = 0;

    // Create structures for request and response and call the get method
    struct XrdxCastor2Stager::ReqInfo req_info(client_uid, client_gid, 
                                               map_path.c_str(), allowed_svc.c_str());
   
    if (!isRewrite)
    {
      xcastor_debug("Put allowed_svc=%s", allowed_svc.c_str());
      status = XrdxCastor2Stager::DoAsyncReq(error, "put", &req_info, resp_info);
      delaytag += ":put";
      aop = AOP_Create;
    }
    else
    {
      xcastor_debug("Update allowed_svc=%s", allowed_svc.c_str());
      status = XrdxCastor2Stager::DoAsyncReq(error, "update", &req_info, resp_info);
      delaytag += ":update";
      aop = AOP_Update;
    }

    if (status == SFS_ERROR)
    {
      // If the file is not yet closed from the last write delay the client
      if (error.getErrInfo() == EBUSY)
      {
        TIMING("RETURN", &opentiming);

        if (gMgr->mLogLevel == LOG_DEBUG)
          opentiming.Print();
      
        return gMgr->Stall(error, XrdxCastor2Stager::GetDelayValue(delaytag.c_str()) , 
                           "file is still busy, please wait");
      }

      TIMING("RETURN", &opentiming);

      if (gMgr->mLogLevel == LOG_DEBUG)
        opentiming.Print();

      return XrdxCastor2Fs::Emsg(epname, error, error.getErrInfo(), "async request failed");

    }
    else if (status >= SFS_STALL)
    {
      TIMING("RETURN", &opentiming);

      if (gMgr->mLogLevel == LOG_DEBUG)
        opentiming.Print();

      return gMgr->Stall(error, XrdxCastor2Stager::GetDelayValue(delaytag.c_str()), 
                         "request queue full or response not ready yet");
    }

    // Clean-up any possible delay tag remainings
    XrdxCastor2Stager::DropDelayTag(delaytag.c_str());
    
    if (resp_info.mStageStatus != "READY")
    {
      TIMING("RETURN", &opentiming);
      
      if (gMgr->mLogLevel == LOG_DEBUG)
        opentiming.Print();

      return XrdxCastor2Fs::Emsg(epname, error, EINVAL, "access file in stager (Put request failed)  fn = ",
                                 map_path.c_str());
    }
  }
  else
  {
    // Read operation
    bool possible = false;
    std::set<std::string> set_svc = gMgr->GetAllAllowedSvc(map_path.c_str());
    
    for (std::set<std::string>::iterator iter = set_svc.begin(); 
         iter != set_svc.end(); ++iter)
    {
      TIMING("PREP2GET", &opentiming);
      allowed_svc = *iter;
      
      if (!XrdxCastor2Stager::Prepare2Get(error, (uid_t) client_uid, (gid_t) client_gid, 
                                          map_path.c_str(), allowed_svc.c_str(), resp_info))
      {
        TIMING("RETURN", &opentiming);
        
        if (gMgr->mLogLevel == LOG_DEBUG)
          opentiming.Print();
        
        continue;
      }

      xcastor_debug("Got redirection to:%s and stagestatus is:%s", 
                    resp_info.mRedirectionPfn1.c_str(), 
                    resp_info.mStageStatus.c_str());

      if (resp_info.mRedirectionPfn1.length() && (resp_info.mStageStatus == "READY"))
      {
        // That is perfect, we can use the setting to read
        possible = true;
        XrdxCastor2Stager::DropDelayTag(delaytag.c_str());
        break;
      }

      // We select this setting and tell the client to wait for the staging in this svcClass
      if (resp_info.mStageStatus == "READY")
      {
        TIMING("RETURN", &opentiming);
        
        if (gMgr->mLogLevel == LOG_DEBUG)
          opentiming.Print();
        
        return gMgr->Stall(error, XrdxCastor2Stager::GetDelayValue(delaytag.c_str()), 
                           "file is being staged in");
      }      
    }

    if (!possible)
    {
      TIMING("RETURN", &opentiming);
      if (gMgr->mLogLevel == LOG_DEBUG)
        opentiming.Print();

      XrdxCastor2Stager::DropDelayTag(delaytag.c_str());
      return XrdxCastor2Fs::Emsg(epname, error, EINVAL, "access file in ANY stager (all prepare2get failed) fn = ",
                                 map_path.c_str());
    }

    // If there was no stager_get we fill request id 0
    if (!resp_info.mRedirectionPfn2.length())
    {
      resp_info.mRedirectionPfn2 += ":";
      resp_info.mRedirectionPfn2 += allowed_svc.c_str();
    }
      
    // Create structures for request and response and call the get method
    struct XrdxCastor2Stager::ReqInfo req_info(client_uid, client_gid, 
                                               map_path.c_str(), allowed_svc.c_str());
   
    TIMING("GET", &opentiming); 
    int status = XrdxCastor2Stager::DoAsyncReq(error, "get", &req_info, resp_info);
    delaytag += ":get";
    
    if (status == SFS_ERROR)
    {
      TIMING("RETURN", &opentiming);
      
      if (gMgr->mLogLevel == LOG_DEBUG)
        opentiming.Print();
      
      return XrdxCastor2Fs::Emsg(epname, error, EINVAL, "async req failed fn=", map_path.c_str());
    }
    else if (status >= SFS_STALL)
    {
      TIMING("RETURN", &opentiming);
      
      if (gMgr->mLogLevel == LOG_DEBUG)
        opentiming.Print();
      
      return gMgr->Stall(error, XrdxCastor2Stager::GetDelayValue(delaytag.c_str()), 
                         "request queue full or response not ready");
    }
    
    if (resp_info.mStageStatus != "READY")
    {
      TIMING("RETURN", &opentiming);
      
      if (gMgr->mLogLevel == LOG_DEBUG)
        opentiming.Print();
      
      return XrdxCastor2Fs::Emsg(epname, error, EINVAL, "access stager (Get request failed) fn=", 
                                 map_path.c_str());
    }
  }

  // Save the redirection host in the set of known diskservers 
  gMgr->CacheDiskServer(resp_info.mRedirectionHost.c_str());

  TIMING("REDIRECTION", &opentiming);
  xcastor_debug("redirection to:%s", resp_info.mRedirectionHost.c_str());

  // Save statistics about server read/write operations 
  if (gMgr->Proc)
  {
    std::ostringstream ostreamclient;
    std::ostringstream ostreamserver;

    ostreamclient << allowed_svc << "::" << client->name;
    ostreamserver << allowed_svc << "::" << resp_info.mRedirectionHost;
    
    gMgr->Stats.IncServerRdWr(ostreamserver.str().c_str(), isRW);
    gMgr->Stats.IncUserRdWr(ostreamclient.str().c_str(), isRW);
  }

  // Add the opaque authorization information for the server for read & write
  XrdxCastor2ServerAcc::AuthzInfo authz;
  authz.sfn = (char*) origpath;
  authz.pfn1 = (char*) resp_info.mRedirectionPfn1.c_str();
  authz.pfn2 = (char*) resp_info.mRedirectionPfn2.c_str();
  authz.id  = (char*)tident;
  authz.client_sec_uid = sclient_uid.c_str();
  authz.client_sec_gid = sclient_gid.c_str();
  authz.accessop = aop;
  time_t now = time(NULL);
  authz.exptime  = (now + gMgr->msTokenLockTime);
  authz.token = "";
  authz.signature = "";
  authz.manager = (char*)gMgr->ManagerId.c_str();
  std::string acc_opaque = "";

  // Build and sign the authorization token with the server's private key
  acc_opaque = gMgr->mServerAcc->GetOpaqueAcc(authz, gMgr->mIssueCapability);

  if (acc_opaque.empty())
  {
    XrdxCastor2Fs::Emsg(epname, error, retc, "build authorization token for sfn = ", map_path.c_str());
    return SFS_ERROR;
  }

  // Add the internal token opaque tag
  resp_info.mRedirectionHost += "?";
  
  // Add the user defined svcClass also to the redirection in case the client
  // comes back from a failure
  if ((val = Open_Env.Get("svcClass")))
  {
    resp_info.mRedirectionHost += "svcClass=";
    resp_info.mRedirectionHost += val;
    resp_info.mRedirectionHost += "&";
  }
  
  // Add the external token opaque tag
  resp_info.mRedirectionHost += acc_opaque.c_str();
  
  if (!isRW)
  {
    // We always assume adler, but we should check the type here ...
    if (strncmp(buf.csumtype, "AD", 2) == 0)
    {
      resp_info.mRedirectionHost += "adler32=";
      resp_info.mRedirectionHost += buf.csumvalue;
      resp_info.mRedirectionHost += "&";
    }

    // Add the 0-size create flag
    if (buf.filesize == 0)
      resp_info.mRedirectionHost += "createifnotexist=1&";
  }
  
  int ecode = atoi(gMgr->xCastor2FsTargetPort.c_str());
  error.setErrInfo(ecode, resp_info.mRedirectionHost.c_str());
  TIMING("AUTHZ", &opentiming);

  // Do the proc statistics if a proc file system is specified
  if (gMgr->Proc)
    gMgr->Stats.IncRdWr(isRW);

  // Check if a mode was given
  if (Open_Env.Get("mode"))
  {
    mode_t mode = strtol(Open_Env.Get("mode"), NULL, 8);

    if (XrdxCastor2FsUFS::Chmod(map_path.c_str(), mode))
      return XrdxCastor2Fs::Emsg(epname, error, serrno, "set mode on", map_path.c_str());
  }

  TIMING("PROC/DONE", &opentiming);
  
  if (gMgr->mLogLevel == LOG_DEBUG)
    opentiming.Print();

  xcastor_debug("redirection to:%s", resp_info.mRedirectionHost.c_str());
  return SFS_REDIRECT;
}


//------------------------------------------------------------------------------
// Close file
//------------------------------------------------------------------------------
int
XrdxCastor2FsFile::close()
{
  static const char* epname = "close";

  if (gMgr->Proc)
  {
    gMgr->Stats.IncCmd();
  }

  // Release the handle and return
  if (oh >= 0  && XrdxCastor2FsUFS::Close(oh))
    return XrdxCastor2Fs::Emsg(epname, error, serrno, "close", fname);

  oh = -1;

  if (fname)
  {
    free(fname);
    fname = 0;
  }

  if (ohp != NULL)
  {
    pclose(ohp);
    ohp = NULL;
  }

  if (ohperror_cnt != 0)
  {
    XrdOucString ohperrorfile = "/tmp/xrdfscmd.";
    ohperrorfile += (int)ohperror_cnt;
    ::unlink(ohperrorfile.c_str());
  }

  if (ohperror != NULL)
  {
    fclose(ohperror);
    ohperror = NULL;
  }

  return SFS_OK;
}


//------------------------------------------------------------------------------
// Read from file
//------------------------------------------------------------------------------
XrdSfsXferSize
XrdxCastor2FsFile::read(XrdSfsFileOffset offset,
                        char*            buff,
                        XrdSfsXferSize   blen)
{
  static const char* epname = "read";
  XrdSfsXferSize nbytes;

  if (gMgr->Proc)
  {
    gMgr->Stats.IncCmd();
  }

  // Make sure the offset is not too large
#if _FILE_OFFSET_BITS!=64

  if (offset >  0x000000007fffffff)
    return XrdxCastor2Fs::Emsg(epname, error, EFBIG, "read", fname);

#endif

  // Pipe reads
  if (ohp)
  {
    char linestorage[4096];
    char* lineptr = linestorage;
    char* bufptr = buff;
    size_t n = 4096;
    int pn;
    nbytes = 0;

    while ((pn = getline(&lineptr, &n, ohp)) > 0)
    {
      XrdOucString line = lineptr;
      XrdOucString rep1 = "/";
      rep1 += gMgr->xCastor2FsName;
      rep1 += "/";

      if (line.beginswith(rep1))
        line.replace(rep1, "/");

      memcpy(bufptr, line.c_str(), line.length());
      bufptr[line.length()] = 0;
      nbytes += line.length();
      bufptr += line.length();

      if (nbytes > (blen - 1024))
        break;

      // Read something from stderr if, existing
      if (ohperror)
      {
        while ((pn = getline(&lineptr, &n, ohperror)) > 0)
        {
          XrdOucString line = "__STDERR__>";
          line += lineptr;
          XrdOucString rep1 = "/";
          rep1 += gMgr->xCastor2FsName;
          rep1 += "/";
          line.replace(rep1, "/");
          memcpy(bufptr, line.c_str(), line.length());
          bufptr[line.length()] = 0;
          nbytes += line.length();
          bufptr += line.length();

          if (nbytes > (blen - 1024))
            break;
        }
      }
    }

    // Read something from stderr if, existing
    if (ohperror)
      while ((pn = getline(&lineptr, &n, ohperror)) > 0)
      {
        XrdOucString line = "__STDERR__>";
        line += lineptr;
        XrdOucString rep1 = "/";
        rep1 += gMgr->xCastor2FsName;
        rep1 += "/";
        line.replace(rep1, "/");
        memcpy(bufptr, line.c_str(), line.length());
        bufptr[line.length()] = 0;
        nbytes += line.length();
        bufptr += line.length();

        if (nbytes > (blen - 1024))
          break;
      }

    return nbytes;
  }

  // Read the actual number of bytes
  do
  {
    nbytes = pread(oh, (void*)buff, (size_t)blen, (off_t)offset);
  }
  while (nbytes < 0 && errno == EINTR);

  if (nbytes  < 0)
  {
    return XrdxCastor2Fs::Emsg(epname, error, errno, "read", fname);
  }

  // Return number of bytes read
  return nbytes;
}


//------------------------------------------------------------------------------
// Read from file - asynchronous
//------------------------------------------------------------------------------
int
XrdxCastor2FsFile::read(XrdSfsAio* aiop)
{
  if (gMgr->Proc)
  {
    gMgr->Stats.IncCmd();
  }

  // Execute this request in a synchronous fashion
  aiop->Result = this->read((XrdSfsFileOffset)aiop->sfsAio.aio_offset,
                            (char*)aiop->sfsAio.aio_buf,
                            (XrdSfsXferSize)aiop->sfsAio.aio_nbytes);
  aiop->doneRead();
  return 0;
}


//------------------------------------------------------------------------------
// Write to file
//------------------------------------------------------------------------------
XrdSfsXferSize
XrdxCastor2FsFile::write(XrdSfsFileOffset offset,
                         const char*      buff,
                         XrdSfsXferSize   blen)
{
  static const char* epname = "write";
  XrdSfsXferSize nbytes;

  if (gMgr->Proc)
  {
    gMgr->Stats.IncCmd();
  }

  // Make sure the offset is not too large
#if _FILE_OFFSET_BITS!=64

  if (offset >  0x000000007fffffff)
    return XrdxCastor2Fs::Emsg(epname, error, EFBIG, "write", fname);

#endif

  // Write the requested bytes
  do
  {
    nbytes = pwrite(oh, (void*)buff, (size_t)blen, (off_t)offset);
  }
  while (nbytes < 0 && errno == EINTR);

  if (nbytes  < 0)
    return XrdxCastor2Fs::Emsg(epname, error, errno, "write", fname);

  // Return number of bytes written
  return nbytes;
}


//------------------------------------------------------------------------------
// Write to file - asynchrnous
//------------------------------------------------------------------------------
int
XrdxCastor2FsFile::write(XrdSfsAio* aiop)
{
  // Execute this request in a synchronous fashion
  aiop->Result = this->write((XrdSfsFileOffset)aiop->sfsAio.aio_offset,
                             (char*)aiop->sfsAio.aio_buf,
                             (XrdSfsXferSize)aiop->sfsAio.aio_nbytes);
  aiop->doneWrite();
  return 0;
}


//------------------------------------------------------------------------------
// Stat the file
//------------------------------------------------------------------------------
int
XrdxCastor2FsFile::stat(struct stat* buf)
{
  static const char* epname = "stat";

  // Fake for pipe reads
  if (ohp)
  {
    memset(buf, sizeof(struct stat), 0);
    buf->st_size = 1024 * 1024 * 1024;
    return SFS_OK;
  }

  if (oh)
  {
    int rc = ::stat(fname, buf);

    if (rc)
      return XrdxCastor2Fs::Emsg(epname, error, errno, "stat", fname);

    return SFS_OK;
  }

  // Execute the function
  struct Cns_filestatcs cstat;

  if (XrdxCastor2FsUFS::Statfn(fname, &cstat))
    return XrdxCastor2Fs::Emsg(epname, error, serrno, "stat", fname);

  // All went well
  memset(buf, sizeof(struct stat), 0);
  buf->st_dev     = 0xcaff;
  buf->st_ino     = cstat.fileid;
  buf->st_mode    = cstat.filemode;
  buf->st_nlink   = cstat.nlink;
  buf->st_uid     = cstat.uid;
  buf->st_gid     = cstat.gid;
  buf->st_rdev    = 0;     /* device type (if inode device) */
  buf->st_size    = cstat.filesize;
  buf->st_blksize = 4096;
  buf->st_blocks  = cstat.filesize / 4096;
  buf->st_atime   = cstat.atime;
  buf->st_mtime   = cstat.mtime;
  buf->st_ctime   = cstat.ctime;
  return SFS_OK;
}


//------------------------------------------------------------------------------
// Sync the file
//------------------------------------------------------------------------------
int
XrdxCastor2FsFile::sync()
{
  static const char* epname = "sync";

  if (gMgr->Proc)
    gMgr->Stats.IncCmd();

  // Perform the function
  if (fsync(oh))
    return XrdxCastor2Fs::Emsg(epname, error, errno, "synchronize", fname);

  return SFS_OK;
}


//------------------------------------------------------------------------------
// Sync asynchronous
//------------------------------------------------------------------------------
int
XrdxCastor2FsFile::sync(XrdSfsAio* aiop)
{
  // Execute this request in a synchronous fashion
  aiop->Result = this->sync();
  aiop->doneWrite();
  return 0;
}


//------------------------------------------------------------------------------
// Truncate file
//------------------------------------------------------------------------------
int
XrdxCastor2FsFile::truncate(XrdSfsFileOffset flen)
{
  static const char* epname = "trunc";

  if (gMgr->Proc)
    gMgr->Stats.IncCmd();

  // Make sure the offset is not too larg
#if _FILE_OFFSET_BITS!=64

  if (flen >  0x000000007fffffff)
    return XrdxCastor2Fs::Emsg(epname, error, EFBIG, "truncate", fname);

#endif

  // Perform the function
  if (ftruncate(oh, flen))
    return XrdxCastor2Fs::Emsg(epname, error, errno, "truncate", fname);

  return SFS_OK;
}

