/*******************************************************************************
 *                      XrdxCastor2OfsTransfer.cc
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
 * @author Elvin Sindrilaru & Andreas Peters - CERN
 * 
 ******************************************************************************/


/*-----------------------------------------------------------------------------*/
#include <string.h>
/*-----------------------------------------------------------------------------*/
#include "XrdxCastor2Ofs.hh"
/*-----------------------------------------------------------------------------*/

extern XrdOucTrace OfsTrace;


//------------------------------------------------------------------------------
// Do a third party transfer
//------------------------------------------------------------------------------
int
XrdxCastor2OfsFile::ThirdPartyTransfer( const char*         fileName,
                                        XrdSfsFileOpenMode  /*openMode*/,
                                        mode_t              /*createMode*/,
                                        const XrdSecEntity* /*client*/,
                                        const char*         opaque )
{
  XrdOucEnv Open_Env( opaque );
  const char* val = Open_Env.Get( "xferuuid" );
  const char* uuidstring = val;
  XrdOucString Spath = fileName;
  xcastor_debug("path=%s, opaque=%s", fileName, opaque);
  uuid_t uuid;

  if ( uuid_parse( val, uuid ) ) {
    return XrdxCastor2OfsFS.Emsg( "3rdParty", error, EINVAL, 
                                  "open file for transfering - illegal xferuuid" );
  }

  XrdTransfer* transfer;

  if ( XrdTransferManager::TM() && 
       ( transfer = XrdTransferManager::TM()->GetTransfer( uuid, error.getErrUser() ) ) ) 
  {
    // Cool, this transfer exists ....
    if ( ( val = Open_Env.Get( "xfercmd" ) ) ) {
      XrdOucString newaction = val;

      if ( newaction == "copy" ) {
        transfer->Action = XrdTransfer::kCopy;
      }
    }
  } else {
    // This transfer doesn't exist, we create a new one
    xcastor_debug("Creating new thirdparty transfer xferuuid=%s", val);
    enum e3p {kInvalid, kPrepareToGet, kPrepareToPut};
    int op = kInvalid;
    XrdOucString e3pstring = "";

    if ( ( val = Open_Env.Get( "xfercmd" ) ) ) {
      if ( !strcmp( val, "preparetoget" ) ) {
        // Prepare a get
        if ( ( val = Open_Env.Get( "source" ) ) ) {
          op = kPrepareToGet;
          e3pstring = "preparetoget";
        } else {
          return XrdOfs::Emsg( "3rdParty", error, EINVAL, "process your request - source missing", Spath.c_str() );
        }
      }

      if ( !strcmp( val, "preparetoput" ) ) {
        // Prepare a put
        if ( ( val = Open_Env.Get( "target" ) ) ) {
          op = kPrepareToPut;
          e3pstring = "preparetoput";
        } else {
          return XrdOfs::Emsg( "3rdParty", error, EINVAL, "process your request - target missing", Spath.c_str() );
        }
      }
    }

    if ( op == kInvalid ) {
      return XrdOfs::Emsg( "3rdParty", error, EINVAL, "process your request", Spath.c_str() );
    }

    // Create the transfer object
    XrdOucString transferentry = Spath;
    transferentry += "?";
    transferentry += opaque;
    xcastor_debug("transferentry=%s", transferentry.c_str());

    if ( XrdTransferManager::TM() && 
         ( !XrdTransferManager::TM()->SetupTransfer( transferentry.c_str(), uuid, true ) ) ) 
    {
      // OK, that worked out
    } else {
      return XrdOfs::Emsg( "3rdParty", error, errno, "process your request - can't setup the transfer for you", Spath.c_str() );
    }

    // Try to attach to the new transfer
    if ( XrdTransferManager::TM() && 
         ( transfer = XrdTransferManager::TM()->GetTransfer( uuid, error.getErrUser() ) ) ) 
    {
      // Cool, this transfer exists now....
    } else {
      return XrdOfs::Emsg( "3rdParty", error, EIO, "process you request - can't setup the transfer for you", Spath.c_str() );
    }
  }

  xcastor_debug("state=%s, action=%s. xferuuid=%s", transfer->StateAsString(),
                transfer->ActionAsString(), transfer->UuidString.c_str());
  xcastor_debug("transfer_path=%s, request_path=%s", transfer->Path.c_str(),
                Spath.c_str() );

  // Check that the registered transfer path is the same like in this open
  if ( transfer->Path != Spath ) {
    XrdTransferManager::TM()->DetachTransfer( uuidstring );
    return XrdxCastor2OfsFS.Emsg( "3rdParty", error, EPERM,  "proceed with transfer "
                                  "- the registered path differs from the open path" );
  }

  if ( ( transfer->Action == XrdTransfer::kGet ) || 
       ( transfer->Action == XrdTransfer::kPut ) || 
       ( transfer->Action == XrdTransfer::kState ) ) 
  {
    // This is used to watch active transfer or get final states
    if ( transfer->State == XrdTransfer::kUninitialized ) {
      // This is a fresh transfer
      if ( !transfer->Initialize() ) {
        XrdTransferManager::TM()->DetachTransfer( uuidstring );
        return XrdxCastor2OfsFS.Emsg( "3rdParty", error, EIO , "proceed with transfer - initialization failed" );
      }
    }

    if ( transfer->State == XrdTransfer::kIllegal ) {
      XrdTransferManager::TM()->DetachTransfer( uuidstring );
      return XrdxCastor2OfsFS.Emsg( "3rdParty", error, EILSEQ, "proceed with transfer - transfer is in state <illegal>" );
    }

    if ( transfer->State == XrdTransfer::kFinished ) {
      XrdTransferManager::TM()->DetachTransfer( uuidstring );
      return XrdxCastor2OfsFS.Emsg( "3rdParty", error, 0, "proceed with transfer - transfer finished successfully" );
    }

    if ( transfer->State == XrdTransfer::kCanceled ) {
      XrdTransferManager::TM()->DetachTransfer( uuidstring );
      return XrdxCastor2OfsFS.Emsg( "3rdParty", error, ECANCELED, "proceed with transfer - transfer has been canceled" );
    }

    if ( transfer->State == XrdTransfer::kError ) {
      XrdTransferManager::TM()->DetachTransfer( uuidstring );
      return XrdxCastor2OfsFS.Emsg( "3rdParty", error, EIO, "proceed with transfer - transfer is in error state" );
    }
  }

  if ( transfer->Action == XrdTransfer::kCopy ) {
    transfer->Action = XrdTransfer::kGet;

    // This is the 3rd party copy client starting the copy
    if ( envOpaque->Get( "xferbytes" ) ) {
      transfer->Options->Put( "xferbytes", envOpaque->Get( "xferbytes" ) );
    }

    transfer->SetState( XrdTransfer::kScheduled );

    if ( XrdTransferManager::TM()->ScheduledTransfers.Num() < XrdxCastor2OfsFS.ThirdPartyCopySlots ) {
      xcastor_debug("scheduling" );
      transfer->SetState( XrdTransfer::kScheduled );
    } else {
      xcastor_debug("no scheduling - send delay");
      // Sent a stall to the client
      XrdTransferManager::TM()->DetachTransfer( uuidstring );
      return XrdxCastor2OfsFS.Emsg( "3rdParty", error, EBUSY, "schedule transfer - all slots busy" );
    }

    // Wait that this transfer get's into running stage
    while ( transfer->State == XrdTransfer::kScheduled ) {
      // That is not really the 'nicest' way to do that but typically 
      // it is scheduled immediatly (we could just use a mutex for that )
      usleep( 100000 );
    }

    // The scheduled state increases the number of attached transfers 
    // We don't detach, if Transfer != NULL the detach is called in close()
    Transfer = transfer;
    return SFS_OK;
  }

  if ( transfer->Action == XrdTransfer::kClose ) { }

  XrdTransferManager::TM()->DetachTransfer( uuidstring );
  return SFS_OK;
}


//------------------------------------------------------------------------------
// Close a tpc transfer
//------------------------------------------------------------------------------
int
XrdxCastor2OfsFile::ThirdPartyTransferClose( const char* uuidstring )
{
  uuid_t uuid;
  XrdTransfer* transfer = 0;

  if ( !uuidstring ) return SFS_ERROR;

  if ( uuid_parse( uuidstring, uuid ) ) {
    return SFS_ERROR;
  }

  if ( XrdTransferManager::TM() && 
       ( transfer = XrdTransferManager::TM()->GetTransfer( uuid, error.getErrUser() ) ) ) 
  {
    // This is the 3rd party copy client finishing the copy
    if ( transfer->State <= XrdTransfer::kScheduled ) {
      transfer->SetState( XrdTransfer::kError );
    } else {
      if ( transfer->State == XrdTransfer::kRunning ) {
        // This tells the running thread that it can set this transfer job 
        // to finished or error and terminate
        transfer->SetState( XrdTransfer::kTerminate );
      }
    }

    XrdTransferManager::TM()->DetachTransfer( uuidstring );
    return SFS_OK;
  }

  return SFS_ERROR;
}


//------------------------------------------------------------------------------
// Tpc setup
//------------------------------------------------------------------------------
void
XrdxCastor2Ofs::ThirdPartySetup( const char* transferdirectory, int slots, int rate )
{
  if ( XrdTransferManager::TM() ) {
    if ( transferdirectory ) {
      XrdTransferManager::SetTransferDirectory( transferdirectory );
    }

    XrdTransferManager::TM()->Bandwidth = rate;
    XrdTransferManager::TM()->ThirdPartyScheduler->setParms( 1, slots, -1, -1, 0 );
  }
}


//------------------------------------------------------------------------------
// Tpc cleanup
//------------------------------------------------------------------------------
void
XrdxCastor2Ofs::ThirdPartyCleanupOnRestart()
{
  XrdTransferCleanup::CleanupOnRestart();
}

