/*******************************************************************************
 *                      XrdxCastor2TransferManager.hh
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
 * @author Andreas Peters - CERN
 *
 ******************************************************************************/

#ifndef __TRANSFERMANAGER_H__
#define __TRANSFERMANAGER_H__

/*-----------------------------------------------------------------------------*/
#include <uuid/uuid.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <openssl/md5.h>
/*-----------------------------------------------------------------------------*/
#include <XrdOuc/XrdOucString.hh>
#include <XrdOuc/XrdOucEnv.hh>
#include <XrdOuc/XrdOucTrace.hh>
#include <XrdSys/XrdSysPthread.hh>
#include <XrdSys/XrdSysLogger.hh>
#include <XrdClient/XrdClient.hh>
#include <XrdClient/XrdClientEnv.hh>
#include <Xrd/XrdScheduler.hh>
#include <Xrd/XrdJob.hh>
/*-----------------------------------------------------------------------------*/


#define XRDTRANSFERMANAGER_LOGFILETAIL 2048

static const char* XrdTransferActionAsString[6] = {
  "invalid",
  "state",
  "copy",
  "close",
  "get",
  "put"
};

static const char* XrdTransferStateAsString[10] = {
  "uninitialized",
  "illegal",
  "initialized",
  "scheduled",
  "running",
  "finished",
  "paused",
  "canceled",
  "error",
  "terminate"
};


//------------------------------------------------------------------------------
//! Class XrdTransferCleanup
//------------------------------------------------------------------------------
class XrdTransferCleanup : XrdJob
{
  public:
    XrdTransferCleanup();
    ~XrdTransferCleanup() {};
    void DoIt();
    static void CleanupOnRestart();
};


//------------------------------------------------------------------------------
//! Class XrdTransfer
//------------------------------------------------------------------------------
class XrdTransfer : XrdJob
{
  public:
    XrdOucString Surl;
    XrdOucString Path;
    XrdOucString UuidString;
    XrdOucString StateFile;
    XrdOucString AttachedFile;
    XrdOucString LogFile;
    XrdOucString ProgressFile;
    XrdOucString Tident;
    XrdSysMutex  LogMutex;

    char defaultbuffer[4096];
    time_t laststatechange;
    int nattached;

    struct TransferRun {
      long long totalbytes;
      long long stopwritebyte;
      long long sourcesize;
      struct timeval abs_start_time;
      struct timeval abs_stop_time;
      struct timezone tz;
      bool progress;
      bool computemd5;
      float bandwidth;
      char* buffer;
      long long buffersize;
      unsigned char md5string[MD5_DIGEST_LENGTH];
      MD5_CTX md5ctx;
    };

    struct TransferRun txf;

    uuid_t Uuid;

    XrdOucEnv* Options;
    int Action;
    int State;
    enum eAction {kInvalid = 0, kState, kCopy, kClose, kGet, kPut};

    enum eState {kUninitialized = 0, kIllegal, kInitialized, kScheduled, kRunning, kFinished, kPaused, kCanceled, kError, kTerminate};

    const char* StateAsString() {
      if ( ( State >= kUninitialized ) && ( State <= kTerminate ) )return XrdTransferStateAsString[State];
      else return "invalid";
    }
    const char* ActionAsString() {
      if ( ( Action >= kInvalid ) && ( Action <= kPut ) ) return XrdTransferActionAsString[Action];
      else return "invalid";
    }
    XrdTransfer( const char* url, uuid_t uuid, const char* uuidstring, const char* tident );
    virtual ~XrdTransfer();

    off_t GetSourceSize();
    off_t GetTargetSize();

    bool Initialize();
    bool SetState( int state );
    int GetState( bool lock = true );
    void SetAttached();
    void ClearAttached();
    bool AddLog( XrdOucString message );

    void UpdateProgress();
    void Summary( bool target = false );

    // third party copy is implemented here
    void  DoIt();
};


//------------------------------------------------------------------------------
//! Class XrdTransferManager
//------------------------------------------------------------------------------
class XrdTransferManager
{
    friend class XrdTransfer;
    static XrdSysError Eroute;
    static XrdOucTrace Trace;

  public:
    XrdScheduler*    ThirdPartyScheduler;
    int Bandwidth;

    XrdTransferManager() {
      Eroute.logger( new XrdSysLogger() );
      ThirdPartyScheduler = new XrdScheduler( &Eroute, &Trace, 1, 4, 0 );
      ThirdPartyScheduler->Start();
      Bandwidth = 9999999;
    }

    // returns an error string if error, otherwise 0
    int Init();

    int SetupTransfer( const char* params, uuid_t& uuid, bool target = false );

    XrdOucHash<XrdTransfer> ScheduledTransfers;
    XrdOucHash<XrdTransfer> InmemoryTransfers;

    virtual ~XrdTransferManager() {};

    static XrdTransferManager* TM() {
      if ( XrdTransferManager::XrdTM ) {
        return ( XrdTransferManager::XrdTM );
      } else {
        ManagerLock.Lock();
        XrdTransferManager::XrdTM = new XrdTransferManager();
        ManagerLock.UnLock();

        if ( !XrdTransferManager::XrdTM->Init() )
          return XrdTransferManager::XrdTM;
        else return 0;
      }
    }
    static void SetTransferDirectory( const char* td ) {
      if ( strcmp( Tdir.c_str(), td ) ) Tdir = td;
    }
    static const char* GetTransferDirectory() {
      return Tdir.c_str();
    }

    XrdTransfer* GetTransfer( uuid_t uuid, const char* tident );
    void DetachTransfer( XrdTransfer* transfer );
    void DetachTransfer( const char* uuidstring );

  private:
    static XrdOucString Tdir;
    static XrdTransferManager* XrdTM;
    static XrdSysMutex ManagerLock;
    static XrdSysMutex ProgressLock;


};

#endif
