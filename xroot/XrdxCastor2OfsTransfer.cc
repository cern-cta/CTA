//         $Id: XrdxCastor2OfsTransfer.cc,v 1.00 2007/10/04 01:34:19 abh Exp $

#include "XrdxCastor2Fs/XrdxCastor2Ofs.hh"
#include "XrdOfs/XrdOfsTrace.hh"
#include "XrdOuc/XrdOucEnv.hh"
#include "XrdTransferManager/XrdTransferManager.hh"
#include <string.h>

extern XrdOucTrace OfsTrace;

int 
XrdxCastor2OfsFile::ThirdPartyTransfer(const char   *fileName,
			     XrdSfsFileOpenMode   openMode,
			     mode_t               createMode,
			     const XrdSecEntity        *client,
			     const char                *opaque) {
  EPNAME("ThirdPartyTransfer");
  
  XrdOucEnv Open_Env(opaque);
  const char* val = Open_Env.Get("xferuuid");
  const char* uuidstring = val;

  XrdOucString Spath = fileName;

  ZTRACE(open,"path=" << fileName << " opaque" << opaque);

  uuid_t uuid;
  if (uuid_parse(val,uuid)) {
    return XrdOfsFS.Emsg(epname, error, EINVAL, "open file for transfering - illegal xferuuid");
  }    
  XrdTransfer* transfer;

  if (XrdTransferManager::TM() && (transfer = XrdTransferManager::TM()->GetTransfer(uuid, error.getErrUser()))) {
    // cool, this transfer exists ....
    if ((val = Open_Env.Get("xfercmd"))) {
      XrdOucString newaction = val;
      if (newaction == "copy") {
	transfer->Action = XrdTransfer::kCopy;
      }
    }
  } else {
    ZTRACE(open,"Creating new thirdparty transfer xferuuid="<<val);
    ////////////////////////////////////////////////////////////////////////////////////////////////
    // this transfer doesn't exist, we create a new one
    enum e3p {kInvalid, kPrepareToGet,kPrepareToPut};
    int op = kInvalid;
    XrdOucString e3pstring="";
    
    if ((val = Open_Env.Get("xfercmd"))) {
      if (!strcmp(val,"preparetoget")) {
	// prepare a get
	if ((val = Open_Env.Get("source"))) {
	  op = kPrepareToGet;
	  e3pstring = "preparetoget";
	} else {
	  return XrdOfs::Emsg(epname, error, EINVAL, "process your request - source missing", Spath.c_str());
	}
      }
      if (!strcmp(val,"preparetoput")) {
	// prepare a put
	if ((val = Open_Env.Get("target"))) {
	  op = kPrepareToPut;
	  e3pstring = "preparetoput";
	} else {
	  return XrdOfs::Emsg(epname, error, EINVAL, "process your request - target missing", Spath.c_str());
	}
      }
    }
    
    if (op == kInvalid) {
      return XrdOfs::Emsg(epname, error, EINVAL, "process your request", Spath.c_str());
    }
    
    // create the transfer object
    XrdOucString transferentry=Spath;
    transferentry+= "?"; transferentry+=opaque;
    
    ZTRACE(open,"transferentry=" << transferentry.c_str());
    if (XrdTransferManager::TM() && (!XrdTransferManager::TM()->SetupTransfer(transferentry.c_str(),uuid,true))) {
      // ok, that worked out
    } else {
      return XrdOfs::Emsg(epname, error, errno, "process your request - can't setup the transfer for you", Spath.c_str());
    }
    
    // try to attach to the new transfer
    int trycnt=0;
    if (XrdTransferManager::TM() && (transfer = XrdTransferManager::TM()->GetTransfer(uuid,error.getErrUser()))) {
      // cool, this transfer exists now....
    } else {
      return XrdOfs::Emsg(epname, error, EIO, "process you request - can't setup the transfer for you",Spath.c_str());
    }
  }
  
  ZTRACE(open,"state=" << transfer->StateAsString() << " action=" << transfer->ActionAsString() << " xferuuid="<< transfer->UuidString.c_str());
  
  
  ZTRACE(open,"transfer-path=" << transfer->Path.c_str() << " request-path="<<Spath.c_str());

  // check that the registered transfer path is the same like in this open
  if (transfer->Path != Spath) {
    XrdTransferManager::TM()->DetachTransfer(uuidstring);
    return XrdOfsFS.Emsg(epname, error, EPERM,  "proceed with transfer - the registered path differs from your open path");
  }
  
  if ((transfer->Action == XrdTransfer::kGet) || (transfer->Action == XrdTransfer::kPut) || (transfer->Action == XrdTransfer::kState)) {
    // this is used to watch active transfer or get final states
    if (transfer->State == XrdTransfer::kUninitialized) {
      // this is a fresh transfer
      if (!transfer->Initialize()) {
	XrdTransferManager::TM()->DetachTransfer(uuidstring);
	return XrdOfsFS.Emsg(epname, error, EIO , "proceed with transfer - initialization failed");
      }
    } 
    
    if (transfer->State == XrdTransfer::kIllegal) {
      XrdTransferManager::TM()->DetachTransfer(uuidstring);
      return XrdOfsFS.Emsg(epname, error, EILSEQ, "proceed with transfer - transfer is in state <illegal>");
    }
    
    if (transfer->State == XrdTransfer::kFinished) {
      XrdTransferManager::TM()->DetachTransfer(uuidstring);
      return XrdOfsFS.Emsg(epname, error, 0, "proceed with transfer - transfer finished successfully");
    }
    
    if (transfer->State == XrdTransfer::kCanceled) {
      XrdTransferManager::TM()->DetachTransfer(uuidstring);
      return XrdOfsFS.Emsg(epname, error, ECANCELED, "proceed with transfer - transfer has been canceled");
    }
    
    if (transfer->State == XrdTransfer::kError) {
      XrdTransferManager::TM()->DetachTransfer(uuidstring);
      return XrdOfsFS.Emsg(epname, error, EIO, "proceed with transfer - transfer is in error state");
    }
  }
  if (transfer->Action == XrdTransfer::kCopy) {
    transfer->Action = XrdTransfer::kGet;
    // this is the 3rd party copy client starting the copy
    if (envOpaque->Get("xferbytes")) {
      transfer->Options->Put("xferbytes",envOpaque->Get("xferbytes"));
    }
    transfer->SetState(XrdTransfer::kScheduled);
    
    // wait that this transfer get's into running stage
    while (transfer->State == XrdTransfer::kScheduled) {
      // that is not really the 'nicest' way to do that but typically it is scheduled immedeatly
      // (we could just use a mutex for that 
      usleep(100000);
    }

    // the scheduled state increases the attach's
    // we don't detach, if Transfer!=NULL the detach is called in close()
    Transfer = transfer;
    return SFS_OK;
  }

  if (transfer->Action == XrdTransfer::kClose) {
  }

  XrdTransferManager::TM()->DetachTransfer(uuidstring);  
  return SFS_OK;
}

int
XrdxCastor2OfsFile::ThirdPartyTransferClose(const char* uuidstring) {
  uuid_t uuid;
  if (!uuidstring) return SFS_ERROR;

  if (uuid_parse(uuidstring,uuid)) {
    return SFS_ERROR;
  }      
  
  XrdTransfer* transfer = 0;
  if (XrdTransferManager::TM() && (transfer = XrdTransferManager::TM()->GetTransfer(uuid,error.getErrUser()))) {
    // this is the 3rd party copy client finishing the copy
    if (transfer->State <= XrdTransfer::kScheduled) {
      transfer->SetState(XrdTransfer::kError);
    } else {
      if (transfer->State == XrdTransfer::kRunning) {
	// this tells the running thread that it can set this transfer job to finished or error and terminate
	transfer->SetState(XrdTransfer::kTerminate);
      } 
    }
    XrdTransferManager::TM()->DetachTransfer(uuidstring);
    return SFS_OK;
  }
  return SFS_ERROR;
}

void
XrdxCastor2Ofs::ThirdPartySetup(const char* transferdirectory, int slots, int rate) {
  
  if (XrdTransferManager::TM()) {
    if (transferdirectory) {XrdTransferManager::SetTransferDirectory(transferdirectory);}
    XrdTransferManager::TM()->Bandwidth = rate;
    XrdTransferManager::TM()->ThirdPartyScheduler->setParms(1, slots, -1, -1,0);
  }
}

void
XrdxCastor2Ofs::ThirdPartyCleanupOnRestart() {
  XrdTransferCleanup::CleanupOnRestart();
}
