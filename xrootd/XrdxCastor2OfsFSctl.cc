/******************************************************************************
 *                      XrdxCastor2OfsFsctl.cc
 *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2003  CERN
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
 * @author Elvin Sindrilaru
 *****************************************************************************/

/*-----------------------------------------------------------------------------*/
#include <sys/types.h>
#include <attr/xattr.h>
/*-----------------------------------------------------------------------------*/
#include "XrdxCastor2Ofs.hh"
#include "XrdOfs/XrdOfsTrace.hh"
#include "XrdOuc/XrdOucEnv.hh"
#include "XrdTransferManager/XrdTransferManager.hh"
/*-----------------------------------------------------------------------------*/

#define XRDXCASTOR2NS_FSCTLPATHLEN 4096
#define XRDXCASTOR2NS_FSCTLOPAQUELEN 16384

extern XrdOucTrace OfsTrace;

//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------
int
XrdxCastor2Ofs::FSctl(const int               cmd,
                      XrdSfsFSctl            &args,
                      XrdOucErrInfo          &error,
                      const XrdSecEntity     *client) {
  char ipath[XRDXCASTOR2NS_FSCTLPATHLEN];
  char iopaque[XRDXCASTOR2NS_FSCTLOPAQUELEN];

  static const char *epname = "FSctl";
  const char *tident = error.getErrUser();

  // accept only plugin calls!
  ZTRACE(fsctl,"Calling FSctl");

  if (cmd!=SFS_FSCTL_PLUGIN) {
    XrdxCastor2Ofs::Emsg(epname, error, EPERM, "execute non-plugin function", "");
    return SFS_ERROR;
  }

  if (args.Arg1Len) {
    if (args.Arg1Len < XRDXCASTOR2NS_FSCTLPATHLEN) {
      strncpy(ipath,args.Arg1,args.Arg1Len);
      ipath[args.Arg1Len] = 0;
    } else {
      XrdxCastor2Ofs::Emsg(epname, error, EINVAL, "convert path argument - string too long", "");
      return SFS_ERROR;
    }
  } else {
    ipath[0] = 0;
  }
  if (args.Arg2Len) {
    if (args.Arg2Len <  XRDXCASTOR2NS_FSCTLOPAQUELEN ) {
      strncpy(iopaque,args.Arg2,args.Arg2Len);
      iopaque[args.Arg2Len] = 0;
    } else {
      XrdxCastor2Ofs::Emsg(epname, error, EINVAL, "convert opaque argument - string too long", "");
      return SFS_ERROR;
    }
  } else {
    iopaque[0] = 0;
  }

  // from here on we can deal with XrdOucString which is more 'comfortable' 
  XrdOucString path    = ipath;
  XrdOucString opaque  = iopaque;
  XrdOucString result  = "";

  ZTRACE(fsctl,ipath);
  ZTRACE(fsctl,iopaque);

  // check if this is a put or get operation
  
  XrdOucEnv env(opaque.c_str());

  //bool hasuuid=false;
  const char* val;
  uuid_t uuid;
  const char* uuidstring;
  // create the transfer object
  if ((val = env.Get("xferuuid"))) {
    uuidstring = val;
    if (uuid_parse(val,uuid)) {
      XrdxCastor2Ofs::Emsg(epname, error, EINVAL, "process your request - can't parse xferuuid", path.c_str());
      return SFS_ERROR;
    }
    
    XrdTransfer* transfer;
    if (XrdTransferManager::TM() && (transfer = XrdTransferManager::TM()->GetTransfer(uuid, error.getErrUser()))) {
      // this transfer has been initialized
      if ((val = env.Get("xfercmd"))) {
	XrdOucString cmd = val;

	if (cmd == "cancel") {
	  ZTRACE(fsctl,"canceling");
	  if (transfer->SetState(XrdTransfer::kCanceled)) {
	    result = "cmd=cancel" ; result +=" " ; result+= "xferuuid="; result += transfer->UuidString.c_str();result+=" retc=0";
	    XrdTransferManager::TM()->DetachTransfer(uuidstring);
	    error.setErrInfo(result.length()+1,result.c_str());
	    return SFS_DATA;
	  } else {
	    XrdTransferManager::TM()->DetachTransfer(uuidstring);
	    return XrdxCastor2Ofs::Emsg(epname, error, EFAULT, "set paused - transfer trace modification failed", path.c_str());
	  }
	}

	if (cmd == "schedule") {
	  ZTRACE(fsctl,"scheduling");
	  if ( (transfer->GetState() == XrdTransfer::kInitialized) || (transfer->GetState() == XrdTransfer::kPaused)){
	    // schedule the transfer
	    if (transfer->SetState(XrdTransfer::kScheduled)) {
	      result = "cmd=schedule" ;result +=" " ; result+= "xferuuid="; result += transfer->UuidString.c_str();result+=" retc=0";
	      XrdTransferManager::TM()->DetachTransfer(uuidstring);
	      error.setErrInfo(result.length()+1,result.c_str());
	      return SFS_DATA;
	    } else {
	      XrdTransferManager::TM()->DetachTransfer(uuidstring);
	      return XrdxCastor2Ofs::Emsg(epname, error, EFAULT, "set scheduled - state transition failed", path.c_str());
	    }
	  } else {
	    // this transfer is not anymore initialized
	    if ( (transfer->State == XrdTransfer::kScheduled) ) {
	      XrdTransferManager::TM()->DetachTransfer(uuidstring);
	      return XrdxCastor2Ofs::Emsg(epname, error, EINPROGRESS, "set schedule - transfer already scheduled", path.c_str());
	    }
	    if ( (transfer->State == XrdTransfer::kRunning) ) {
	      XrdTransferManager::TM()->DetachTransfer(uuidstring);
	      return XrdxCastor2Ofs::Emsg(epname, error, EINPROGRESS, "set schedule - transfer already running", path.c_str());
	    }
	    if ( (transfer->State == XrdTransfer::kFinished) ) {
	      XrdTransferManager::TM()->DetachTransfer(uuidstring);
	      return XrdxCastor2Ofs::Emsg(epname, error, EALREADY, "set schedule - transfer is already finished", path.c_str());
	    }
	    if ( (transfer->State == XrdTransfer::kCanceled) ) {
	      XrdTransferManager::TM()->DetachTransfer(uuidstring);
	      return XrdxCastor2Ofs::Emsg(epname, error, ECANCELED, "set schedule - transfer is already canceled", path.c_str());
	    }
	    if ( (transfer->State == XrdTransfer::kError) ) {
	      XrdTransferManager::TM()->DetachTransfer(uuidstring);
	      return XrdxCastor2Ofs::Emsg(epname, error, EIO, "set schedule - transfer failed", path.c_str());
	    }
	    
	    if ( (transfer->State << XrdTransfer::kInitialized) ) {
	      XrdTransferManager::TM()->DetachTransfer(uuidstring);
	      return XrdxCastor2Ofs::Emsg(epname, error, EINVAL, "set schedule - transfer is in illegal state", path.c_str());
	    }
	  }
	}
	
	if (cmd == "pause") {
	  ZTRACE(fsctl,"pausing");
	  if ( (transfer->State == XrdTransfer::kInitialized) ||
	       (transfer->State == XrdTransfer::kScheduled) ||
	       (transfer->State == XrdTransfer::kRunning) ||
	       (transfer->State == XrdTransfer::kPaused) ) {
	    if (transfer->SetState(XrdTransfer::kPaused)) {
	      result = "cmd=pause" ;result +=" " ; result+= "xferuuid="; result += transfer->UuidString.c_str();result+=" retc=0";
	      error.setErrInfo(result.length()+1,result.c_str());
	      XrdTransferManager::TM()->DetachTransfer(uuidstring);
	      return SFS_DATA;
	    } else {	    
	      XrdTransferManager::TM()->DetachTransfer(uuidstring);
	      return XrdxCastor2Ofs::Emsg(epname, error, EFAULT, "set paused - transfer trace modification failed", path.c_str());
	    }
	  }
	}
	
	if (cmd == "status") {
	  ZTRACE(fsctl,"status");
	  transfer->GetState();
	  result = "cmd="; result += cmd.c_str(); result += " xferuuid=";result += transfer->UuidString.c_str(); result += " state="; result += transfer->StateAsString(); result += " action="; result+= transfer->ActionAsString();
	  XrdTransferManager::TM()->DetachTransfer(uuidstring);
	  error.setErrInfo(result.length()+1,result.c_str());
	  return SFS_DATA;
	}
	
	if (cmd == "checksum") {
	  ZTRACE(fsctl,"checksum");
	  char diskckSumbuf[32+1];
	  char diskckSumalg[32];
	  diskckSumbuf[0]=diskckSumalg[0]=0;
	  XrdOucString DiskChecksum="";
	  XrdOucString DiskChecksumAlgorithm="";
	  char * file=0;
	  int nattr;
	  if (!( file = transfer->Options->Get("source"))) {
	    file = transfer->Options->Get("target");
	  }
	  if (file) {
	    nattr = getxattr(file,"user.castor.checksum.type",diskckSumalg,sizeof(diskckSumalg));
	    if (nattr) diskckSumalg[nattr] = 0;
	    nattr = getxattr(file,"user.castor.checksum.value",diskckSumbuf,sizeof(diskckSumbuf));
	    if (nattr) diskckSumbuf[nattr] = 0;
	    DiskChecksum = diskckSumbuf;
	    DiskChecksumAlgorithm = diskckSumalg;
	  }

	  if (DiskChecksum.length() && DiskChecksumAlgorithm.length()) {
	    result = DiskChecksumAlgorithm; result +=":"; result += DiskChecksum;
	  } else {
	    result = "nochecksum";
	  }
	  XrdTransferManager::TM()->DetachTransfer(uuidstring);
	  error.setErrInfo(result.length()+1,result.c_str());
	  return SFS_DATA;			
	}
	

	if (cmd == "progress") {
	  ZTRACE(fsctl,"progress");

	  char progresstail[XRDTRANSFERMANAGER_LOGFILETAIL];
	  int progressfd = open(transfer->ProgressFile.c_str(),O_RDONLY);
	  progresstail[0] = 0;

	  if (progressfd >=0) {
	    int rb = read(progressfd, progresstail, XRDTRANSFERMANAGER_LOGFILETAIL);
	    if (rb<0) {
	      close (progressfd);
	      XrdTransferManager::TM()->DetachTransfer(uuidstring);
	      return XrdxCastor2Ofs::Emsg(epname, error, errno, "read in logfile", transfer->ProgressFile.c_str());
	    } else {
	      progresstail[rb]=0;
	    }
	    close (progressfd);
	  } else {
	    // couldn' open logfile
	    //	    return XrdxCastor2Ofs::Emsg(epname, error, errno, "open logfile", transfer->ProgressFile.c_str());
	    progresstail[0] = 0;
	  }
	  result = progresstail;
	  error.setErrInfo(result.length()+1,result.c_str());
	  XrdTransferManager::TM()->DetachTransfer(uuidstring);
	  return SFS_DATA;			
	}

	if (cmd == "log") {
	  ZTRACE(fsctl,"log");
	  char logtail[XRDTRANSFERMANAGER_LOGFILETAIL];
	  int logfd = open(transfer->LogFile.c_str(),O_RDONLY);
	  if (logfd >=0) {
	    off_t length = lseek(logfd, 0, SEEK_END);
	    if (length>0) {
	      off_t logposition = 0;
	      if (length > XRDTRANSFERMANAGER_LOGFILETAIL) {
		logposition = length-XRDTRANSFERMANAGER_LOGFILETAIL;
	      }
	      if ((lseek(logfd, logposition,SEEK_SET)>=0)) {
		int rb = read(logfd, logtail, XRDTRANSFERMANAGER_LOGFILETAIL);
		if (rb<0) {
		  close (logfd);
		  XrdTransferManager::TM()->DetachTransfer(uuidstring);
		  return XrdxCastor2Ofs::Emsg(epname, error, errno, "read in logfile", transfer->LogFile.c_str());
		} else {
		  logtail[rb]=0;
		}
	      } else {
		close (logfd);
		XrdTransferManager::TM()->DetachTransfer(uuidstring);
		return XrdxCastor2Ofs::Emsg(epname, error, errno, "seek in logfile", transfer->LogFile.c_str());
	      }
	    } else {
	      logtail[0]=0;
	    }
	    close (logfd);
	  } else {
	    // couldn' open logfile
	    XrdTransferManager::TM()->DetachTransfer(uuidstring);
	    return XrdxCastor2Ofs::Emsg(epname, error, errno, "open logfile", transfer->LogFile.c_str());
	  }
	  result = logtail;
	  XrdTransferManager::TM()->DetachTransfer(uuidstring);
	  error.setErrInfo(result.length()+1,result.c_str());
	  return SFS_DATA;
	}
      }
      XrdxCastor2Ofs::Emsg(epname, error, EINVAL, "process your request - no xfercmd I know specified", path.c_str());
      return SFS_ERROR;
    } else {
      // don't know about this transfer
      XrdxCastor2Ofs::Emsg(epname, error, EINVAL, "process your request - transfer ID not existing", path.c_str());
      return SFS_ERROR;
    }    
  } else {
    XrdxCastor2Ofs::Emsg(epname, error, EINVAL, "process your request - not implemented", path.c_str());
    return SFS_ERROR;
  }
}
