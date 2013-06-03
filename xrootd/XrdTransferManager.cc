/*******************************************************************************
 *                      XrdxCastor2TransferManager.cc
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

/*-----------------------------------------------------------------------------*/
#include <dirent.h>
#include <signal.h>
/*-----------------------------------------------------------------------------*/
#include "XrdTransferManager.hh"
/*-----------------------------------------------------------------------------*/

XrdTransferManager* XrdTransferManager::XrdTM = NULL;
XrdOucString XrdTransferManager::Tdir = "/tmp/transfers";
XrdSysMutex XrdTransferManager::ManagerLock;
XrdSysMutex XrdTransferManager::ProgressLock;

XrdSysError  XrdTransferManager::Eroute( 0 );
XrdOucTrace  XrdTransferManager::Trace( &XrdTransferManager::Eroute );

#ifndef NAME_RECONNECTWAIT
#define NAME_RECONNECTWAIT NAME_RECONNECTTIMEOUT
#endif


//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------
pid_t run( const char* command, const char* exitfile, const char* pidfile )
{
  XrdOucString doit;
  XrdOucString ldpath = getenv( "LD_LIBRARY_PATH" );

  while ( ldpath.replace( " ", "" ) ) {}

  XrdOucString path  = getenv( "PATH" );
  XrdOucString ofspath = getenv( "XRDOFSLIB" );
  int libpos = ofspath.find( "/lib" );
  ofspath.erase( libpos );
  ofspath += "/bin/";

  while ( path.replace( " ", "" ) ) {}

  doit = "env LD_LIBRARY_PATH=";
  doit += ldpath;
  doit += " ";
  doit += "PATH=";
  doit += ofspath;
  doit += ":";
  doit += "/opt/xrootd/bin:";
  doit += path;
  doit += command;
  doit += " >& /dev/null & echo $! > ";
  doit += pidfile;
  //  fprintf(stderr,"[run]=> %s\n", doit.c_str());
  system( doit.c_str() );
  int fd = 0;
  fd = open( pidfile, O_RDONLY );

  if ( fd ) {
    char spid[1024];
    int nread = read( fd, spid, 256 );

    if ( nread > 0 ) {
      close( fd );
      unlink( pidfile );
      return atoi( spid );
    }

    close( fd );
  }

  return 0;
}


//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------
XrdTransferCleanup::XrdTransferCleanup() : XrdJob( "cleanup-transfer-directory" ) { }


//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------
void
XrdTransferCleanup::DoIt()
{
  XrdOucString CleanupDir;
  CleanupDir = XrdTransferManager::GetTransferDirectory();
  DIR* cleanupdir = opendir( CleanupDir.c_str() );
  time_t now = time( NULL );
  XrdOucString checkfile = "";

  if ( cleanupdir ) {
    struct dirent* dp;

    while ( ( dp = readdir( cleanupdir ) ) != NULL ) {
      struct stat buf;
      checkfile = CleanupDir.c_str();
      checkfile += "/";
      checkfile += dp->d_name;

      if ( !stat( checkfile.c_str(), &buf ) ) {
        if ( ( now - buf.st_mtime ) > ( 24 * 3600 ) ) {
          // remove it
          unlink( checkfile.c_str() );
        }
      }
    }

    closedir( cleanupdir );
  }

  // reschedule myself
  XrdTransferManager::TM()->ThirdPartyScheduler->Schedule( this, time( NULL ) + 3600 );
}


//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------
void
XrdTransferCleanup::CleanupOnRestart()
{
  XrdOucString CleanupDir;
  CleanupDir = XrdTransferManager::GetTransferDirectory();
  DIR* cleanupdir = opendir( CleanupDir.c_str() );
  XrdOucString checkfile = "";

  if ( cleanupdir ) {
    struct dirent* dp;

    while ( ( dp = readdir( cleanupdir ) ) != NULL ) {
      uuid_t uuid;

      if ( ( strstr( dp->d_name, ".log" ) ) || ( strstr( dp->d_name, ".progress" ) ) || ( strstr( dp->d_name, ".state" ) ) )
        continue;

      if ( uuid_parse( dp->d_name, uuid ) ) {
        continue;
      }

      XrdTransfer* transfer = XrdTransferManager::TM()->GetTransfer( uuid, "CleanupOnRestart" );

      if ( transfer && ( transfer->State < XrdTransfer::kFinished ) ) {
        // we have to set this into terminate state
        transfer->SetState( XrdTransfer::kTerminate );
        XrdOucString message = "cleaning up destroyed transfer after restart";
        message += dp->d_name;
        transfer->AddLog( message );
        XrdTransferManager::TM()->DetachTransfer( transfer );
      }
    }

    closedir( cleanupdir );
  }
}


//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------
XrdTransfer::XrdTransfer( const char* url, uuid_t uuid, const char* uuidstring, const char* tident ) : XrdJob( url )
{
  // split the url into path + options
  Surl = url;
  Path = Surl;
  XrdOucString Command = "invalid";
  UuidString = uuidstring;
  nattached = 0;
  memcpy( Uuid, uuid, sizeof( uuid_t ) );
  XrdOucString Opts = Surl;
  int npos = 0;
  Tident = tident;

  if ( ( npos = Surl.find( "?" ) ) != STR_NPOS ) {
    Path.assign( Surl, 0, npos - 1 );
    Opts.assign( Surl, npos + 1 );
    Options = new XrdOucEnv( Opts.c_str() );
  } else {
    Options = new XrdOucEnv( "" );
  }

  Command = Options->Get( "xfercmd" );
  Action = kInvalid;

  if ( Command == "preparetoput" ) {
    Action = kPut;
  }

  if ( Command == "preparetoget" ) {
    Action = kGet;
  }

  if ( Command == "copy" ) {
    Action = kCopy;
  }

  if ( Command == "close" ) {
    Action = kClose;
  }

  StateFile = XrdTransferManager::Tdir;
  StateFile += "/";
  StateFile += UuidString.c_str();
  StateFile += ".state";
  AttachedFile = XrdTransferManager::Tdir;
  AttachedFile += "/";
  AttachedFile += UuidString.c_str();
  AttachedFile += ".attached";
  LogFile = XrdTransferManager::Tdir;
  LogFile += "/";
  LogFile += UuidString.c_str();
  LogFile += ".log";
  ProgressFile = XrdTransferManager::Tdir;
  ProgressFile += "/";
  ProgressFile += UuidString.c_str();
  ProgressFile += ".progress";
  State = GetState( false );
  txf.totalbytes    = 0;
  txf.stopwritebyte = 0;
  txf.sourcesize    = GetSourceSize();
  txf.progress      = true;
  txf.computemd5    = true;
  txf.bandwidth     = XrdTransferManager::TM()->Bandwidth;
  // fix me
  txf.buffer        = 0;
  txf.buffersize    = 0;
  MD5_Init( &( txf.md5ctx ) );
  // set the attached flag
  SetAttached();
  // add ourself to the inmemory queue, we erase after 1 day in any case!
  XrdTransferManager::TM()->InmemoryTransfers.Add( uuidstring, this, 86400, Hash_keepdata );
}


//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------
XrdTransfer::~XrdTransfer()
{
  if ( Options ) delete Options;

  Options = 0;

  if ( txf.buffer ) delete( txf.buffer );

  txf.buffer = 0;
  XrdTransferManager::TM()->InmemoryTransfers.Del( UuidString.c_str() );
}


//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------
void
XrdTransfer::SetAttached()
{
  // set the .attached file in the transfer directory
  int fd = open( AttachedFile.c_str(), O_RDWR | O_CREAT, S_IRWXU | S_IRGRP | S_IROTH );

  if ( fd > 0 )
    close( fd );
}


//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------
void
XrdTransfer::ClearAttached()
{
  unlink( AttachedFile.c_str() );
}



//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------
int
XrdTransfer::GetState( bool lock )
{
  if ( lock ) XrdTransferManager::ManagerLock.Lock();

  int fd = open( StateFile.c_str(), O_RDONLY );

  if ( fd >= 0 ) {
    int rb = read( fd, &State, sizeof( State ) );

    if ( rb != sizeof( State ) ) {
      close( fd );

      if ( lock )XrdTransferManager::ManagerLock.UnLock();

      return kIllegal;
    }

    close( fd );
  } else {
    if ( lock )XrdTransferManager::ManagerLock.UnLock();

    return kInvalid;
  }

  if ( lock )XrdTransferManager::ManagerLock.UnLock();

  return State;
}


//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------
bool
XrdTransfer::SetState( int state )
{
  State = GetState();

  if ( State == state )
    return true;

  XrdTransferManager::ManagerLock.Lock();
  laststatechange = time( NULL );
  State = state;
  XrdOucString message = "set state ";
  message += StateAsString();
  AddLog( message );
  XrdOucString SF = StateFile;
  SF += ".tmp";
  int fd = open( SF.c_str(), O_RDWR | O_CREAT, S_IRWXU | S_IRGRP | S_IROTH );

  if ( fd >= 0 ) {
    int rb;

    while ( ( ( rb = write( fd, &State, sizeof( State ) ) ) < 0 ) && ( errno == EINTR ) ) {}

    if ( rb != sizeof( State ) ) {
      close( fd );
      XrdTransferManager::ManagerLock.UnLock();
      return false;
    }

    close( fd );
  } else {
    XrdTransferManager::ManagerLock.UnLock();
    return false;
  }

  rename( SF.c_str(), StateFile.c_str() );

  if ( State == kScheduled ) {
    // add this to the scheduled hash
    XrdTransferManager::TM()->ScheduledTransfers.Add( UuidString.c_str(), this, 86400, Hash_keepdata );
    XrdTransferManager::TM()->ThirdPartyScheduler->Schedule( this );
    nattached++;
    //    XrdTransfermanager::TM()->Scheduler
  }

  if ( State == kCanceled ) {
    // remove the transfer
    XrdTransfer* rjob = XrdTransferManager::TM()->InmemoryTransfers.Find( UuidString.c_str() );

    if ( rjob ) {
      XrdTransferManager::TM()->ThirdPartyScheduler->Cancel( rjob );
    }

    if ( !XrdTransferManager::TM()->ScheduledTransfers.Del( UuidString.c_str() ) ) {
      rjob->nattached--;
    }
  }

  if ( State == kError ) {
    if ( !XrdTransferManager::TM()->ScheduledTransfers.Del( UuidString.c_str() ) ) {}
  }

  if ( State == kFinished ) {
    if ( !XrdTransferManager::TM()->ScheduledTransfers.Del( UuidString.c_str() ) ) {}
  }

  if ( State == kTerminate ) {
    if ( !XrdTransferManager::TM()->ScheduledTransfers.Del( UuidString.c_str() ) ) {}
  }

  XrdTransferManager::ManagerLock.UnLock();
  return true;
}


bool
XrdTransfer::AddLog( XrdOucString message )
{
  char timebuf[4096];
  time_t now = time( NULL );
  LogMutex.Lock();
  int logfd = open( LogFile.c_str(), O_RDWR | O_CREAT | O_APPEND , S_IRWXU | S_IRGRP | S_IROTH );

  if ( logfd >= 0 ) {
    // filter authz ....
    int apos = message.find( "authz=" );
    int bpos = message.find( "&", apos );
    int cpos = message.find( "\n", apos );

    if ( apos != STR_NPOS ) {
      if ( bpos != STR_NPOS ) {
        message.erase( apos + 6, bpos - apos - 1 );
        message.insert( "...", apos + 6 );
      } else {
        if ( cpos != STR_NPOS ) {
          message.erase( apos + 6, cpos - apos - 1 );
          message.insert( "...", cpos );
        } else {
          message.erase( apos + 6 );
          message.insert( "...", apos + 6 );
        }
      }
    }

    XrdOucString writemessage;
    writemessage += ctime_r( &now, timebuf );
    writemessage.erasefromend( 1 );
    writemessage += " ";
    writemessage += Tident;
    writemessage += " ";
    writemessage += message.c_str();
    writemessage += "\n";
    write( logfd, writemessage.c_str(), writemessage.length() );
    close( logfd );
    XrdOucString stdmessage = "                      [";
    stdmessage += UuidString;
    stdmessage += "] ";
    stdmessage += writemessage;
    fprintf( stderr, "%s", stdmessage.c_str() );
    LogMutex.UnLock();
    return true;
  } else {
    LogMutex.UnLock();
    return false;
  }
}


//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------
bool
XrdTransfer::Initialize()
{
  return SetState( kInitialized );
}


//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------
void
XrdTransfer::Summary( bool istarget )
{
  char tbytes[4096];
  char sbytes[4096];
  sprintf( tbytes, "%lld", txf.totalbytes );
  sprintf( sbytes, "%lld", txf.sourcesize );
  float abs_time = ( ( float )( ( txf.abs_stop_time.tv_sec - txf.abs_start_time.tv_sec ) * 1000 +
                                ( txf.abs_stop_time.tv_usec - txf.abs_start_time.tv_usec ) / 1000 ) );
  char mbps[4096];
  char xft[4096];

  if ( abs_time ) {
    sprintf( mbps, "%.01f", txf.totalbytes / abs_time / 1000.0 );
  } else {
    sprintf( mbps, "0.00" );
  }

  sprintf( xft, "%.03f", abs_time / 1000.0 );
  XrdOucString message = "sourcebytes=";
  message += sbytes;
  message += " xferbytes=";
  message += tbytes;
  message += " rate=";
  message += mbps;
  message += " xfertime=";
  message += xft;

  if ( txf.computemd5 ) {
    if ( !istarget ) {
      message += " md5=";

      for ( int i = 0; i < MD5_DIGEST_LENGTH; i++ ) {
        char m5s[3];
        sprintf( m5s, "%02x", txf.md5string[i] );
        message += m5s;
      }

      message += " ";
    }
  }

  AddLog( message );
}


//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------
void
XrdTransfer::UpdateProgress()
{
  XrdTransferManager::ProgressLock.Lock();
  char tbytes[4096];
  char sbytes[4096];
  sprintf( tbytes, "%lld", txf.totalbytes );
  sprintf( sbytes, "%lld", txf.sourcesize );
  float abs_time = ( ( float )( ( txf.abs_stop_time.tv_sec - txf.abs_start_time.tv_sec ) * 1000 +
                                ( txf.abs_stop_time.tv_usec - txf.abs_start_time.tv_usec ) / 1000 ) );
  XrdOucString PF = ProgressFile;
  PF += ".tmp";
  FILE* fd = fopen( ProgressFile.c_str(), "w+" );

  if ( fd ) {
    fprintf( fd, "[%s] Total %.02f MB\t|", UuidString.c_str(), ( float )txf.sourcesize / 1024 / 1024 );

    for ( int l = 0; l < 20; l++ ) {
      if ( l < ( ( int )( 20.0 * txf.totalbytes / txf.sourcesize ) ) )
        fprintf( fd, "=" );

      if ( l == ( ( int )( 20.0 * txf.totalbytes / txf.sourcesize ) ) )
        fprintf( fd, ">" );

      if ( l > ( ( int )( 20.0 * txf.totalbytes / txf.sourcesize ) ) )
        fprintf( fd, "." );
    }

    if ( abs_time ) {
      fprintf( fd, "| %.02f %% [%.01f MB/s in %.02f sec]", 100.0 * txf.totalbytes / txf.sourcesize, txf.totalbytes / abs_time / 1000.0, abs_time / 1000.0 );
    } else {
      fprintf( fd, "| %.02f %% [~ MB/s in 0.00 sec]", 100.0 * txf.totalbytes / txf.sourcesize );
    }

    fclose( fd );
    rename( PF.c_str(), ProgressFile.c_str() );
  }

  XrdTransferManager::ProgressLock.UnLock();
}


//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------
void
XrdTransfer::DoIt()
{
  // the receiving end is passive
  gettimeofday( &txf.abs_start_time, &txf.tz );
  // hard coded timeout value
  int timeout = 3600;

  // this is the file upload
  if ( Action == kPut ) {
    // allocate a buffer
    txf.buffer        = ( char* )malloc( 1024 * 1024 );

    if ( !txf.buffer ) {
      txf.buffer = defaultbuffer;
      txf.buffersize = sizeof( defaultbuffer );
    } else {
      // fix me
      txf.buffersize = 1024 * 1024;
    }

    XrdOucString target = Options->Get( "target" );
    XrdOucString source = Options->Get( "source" );
    const char* targetopaque = Options->Get( "targetopaque" );

    if ( targetopaque ) {
      // target opaque information is ',' separated in the client opaque information
      XrdOucString stargetopaque = targetopaque;
      // prevent command faking in opaque information
      stargetopaque.replace( "xfercmd=", "oldcmd=" );

      while ( stargetopaque.replace( ",", "&" ) ) {};

      target += "?";

      target += stargetopaque.c_str();
    } else {
      target += "?";
    }

    target += "&xfercmd=copy";
    target += "&xferbytes=";
    char xb[1024];
    sprintf( xb, "%lld", txf.sourcesize );
    target += xb;
    target += "&xferuuid=";
    target += UuidString.c_str();

    // don't forward " or \" ... we quote the whole
    while ( source.replace( "\\\"", "" ) ) {}

    while ( source.replace( "\"", "" ) )  {}

    while ( source.replace( "'", "" ) )   {}

    while ( target.replace( "\\\"", "" ) ) {}

    while ( target.replace( "\"", "" ) )  {}

    while ( target.replace( "'", "" ) )   {}

    // call the external programm here
    XrdOucString progline = "";
    // quote the source and target URLs
    progline += " XrdTransfer ";
    progline += "\"";
    progline += source;
    progline += "\" \"";
    progline += target;
    progline += "\" ";

    if ( txf.progress )
      progline += ProgressFile;
    else
      progline += "\"\"";

    progline += " ";
    progline += LogFile;
    progline += " ";
    progline += ( int ) txf.bandwidth;

    if ( txf.computemd5 )
      progline += " 1";
    else
      progline += " 0";

    progline += " ";
    progline += UuidString;
    progline += " ";
    progline += Tident;
    progline += " ";
    // timeout default
    progline += "3600 ";
    // father pid
    progline += ( int ) getpid();
    progline += " ";
    //    progline += "\"";
    XrdOucString exitfile = "/tmp/.";
    exitfile += UuidString;
    exitfile += ".exit";
    progline += exitfile;
    XrdOucString pidfile = "/tmp/.";
    pidfile += UuidString;
    pidfile += ".pid";
    pid_t pid = run( progline.c_str(), exitfile.c_str(), pidfile.c_str() );
    // fprintf(stderr,"[exec]=> pid=%d\n", pid);

    if ( pid ) {
      bool canceled = false;
      SetState( kRunning );
      time_t starttime = time( NULL );

      while ( 1 ) {
        // check if we are still supposed to run ....
        XrdTransferManager::ManagerLock.Lock();

        if ( !XrdTransferManager::TM()->ScheduledTransfers.Find( UuidString.c_str() ) ) {
          XrdTransferManager::ManagerLock.UnLock();
          // send a cancelling signal
          // we have to do it like that because we run as daemon and cannot kill our child shells ... sigh
          XrdOucString syskill = "kill -s 1 ";
          syskill += ( int ) pid;
          system( syskill.c_str() );
          canceled = true;
          break;
        }

        XrdTransferManager::ManagerLock.UnLock();

        if ( kill( pid, 0 ) ) {
          // process disappeared
          break;
        }

        time_t nowtime = time( NULL );

        if ( ( nowtime - starttime ) > timeout ) {
          XrdOucString message = "error: timeout after 1 hour";
          AddLog( message );
          // we have to do it like that because we run as daemon and cannot kill our child shells ... sigh
          XrdOucString syskill = "kill -s 1 ";
          syskill += ( int ) pid;
          system( syskill.c_str() );
          break;
        }

        usleep( 1000000 );
      }

      if ( canceled ) {
        SetState( kCanceled );
        unlink( exitfile.c_str() );
      } else {
        if ( !unlink( exitfile.c_str() ) ) {
          SetState( kError );
        } else {
          SetState( kFinished );
        }
      }
    } else {
      XrdOucString message = "error: cannot run transfer command";
      AddLog( message );
      SetState( kError );
    }
  }

  // this is the file receiver
  if ( Action == kGet ) {
    SetState( kRunning );
    // we just watch hopefully the growth of the file and wait for the 'dead'
    off_t finalsize = 0;
    // determine how big the file should get
    XrdOucString xb = Options->Get( "xferbytes" );

    if ( xb == "" ) {
      XrdOucString message = "error: xferbytes missing in opaque";
      AddLog( message );
      SetState( kError );
    } else {
      finalsize = strtoll( xb.c_str(), NULL, 0 );
      off_t lasttotalbytes = -1;
      bool found = false;

      do {
        // if the source was still written, this might happen!
        if ( finalsize <= GetTargetSize() ) {
          break;
        } else {
          // in this case the two variables are somehow 'inverted'
          txf.totalbytes = GetTargetSize();
          txf.sourcesize = finalsize;
          gettimeofday( &txf.abs_stop_time, &txf.tz );

          if ( lasttotalbytes != txf.totalbytes ) {
            // update only if there is really a change!
            if ( txf.progress ) {
              gettimeofday( &txf.abs_stop_time, &txf.tz );
              UpdateProgress();
            }

            lasttotalbytes = txf.totalbytes;
          }
        }

        // 0.25s gaps should be sufficient
        usleep( 250000 );
        XrdTransferManager::ManagerLock.Lock();

        if ( XrdTransferManager::TM()->ScheduledTransfers.Find( UuidString.c_str() ) )
          found = true;
        else
          found = false;

        XrdTransferManager::ManagerLock.UnLock();
      } while ( found );

      // do the last update
      if ( txf.progress ) {
        txf.totalbytes = GetTargetSize();
        txf.sourcesize = finalsize;
        gettimeofday( &txf.abs_stop_time, &txf.tz );
        UpdateProgress();
      }

      if ( finalsize == GetTargetSize() ) {
        // the transfer seems complete, let's change to
        SetState( kFinished );
        txf.totalbytes = GetTargetSize();
        txf.sourcesize = finalsize;
        gettimeofday( &txf.abs_stop_time, &txf.tz );
        Summary( true );
      } else {
        // there has been a transfer error, the client disconnected before we received the full file or the filesize changed at the source during the transfer
        XrdOucString message = "error: the client seems to have disconnected or the filesize changed during the transfer ... ";
        message += "Finalsize: ";
        message += ( int )finalsize;
        message += " Targetsize: ";
        message += ( int )GetTargetSize();
        AddLog( message );
        SetState( kError );
        // remove the target file
        char* path =   Options->Get( "target" );

        if ( path ) {
          //    message = "Removing Transfer Target: "; message += path; AddLog(message);
          //    unlink(path);
        }
      }
    }
  }

  // after a transfer try we free the input buffer in any case to save memory
  if ( txf.buffer ) {
    free( txf.buffer );
    txf.buffer = 0;
    txf.buffersize = 0;
  }

  // we detach from this transfer, to be freed !
  //  XrdTransferManager::ManagerLock.Lock();
  //  nattached--;
  // not sure, if this really works
  XrdTransferManager::TM()->DetachTransfer( UuidString.c_str() );
  //  XrdTransferManager::ManagerLock.UnLock();
}


//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------
off_t
XrdTransfer::GetSourceSize()
{
  struct stat buf;
  char* path =   Options->Get( "source" );

  if ( path && stat( path, &buf ) ) {
    return -1;
  } else {
    return buf.st_size;
  }
}


//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------
off_t
XrdTransfer::GetTargetSize()
{
  struct stat buf;
  char* path =   Options->Get( "target" );

  if ( path && stat( path, &buf ) ) {
    return -1;
  } else {
    return buf.st_size;
  }
}


//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------
int
XrdTransferManager::Init()
{
  // the first time we init, transfer dir point's to /tmp/transferdirectory .... which is a little bit fishy .... FIXME!
  // check if the transfer directory is useable
  //  if ( access(Tdir.c_str(),R_OK | W_OK) ) {
  //    return errno;
  //  }
  XrdTransferManager::TM()->ThirdPartyScheduler->Schedule( ( XrdJob* )new XrdTransferCleanup(), time( NULL ) + 60 );
  return 0;
}


//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------
int
XrdTransferManager::SetupTransfer( const char* params, uuid_t& uuid, bool target )
{
  char uuidstring[40];

  if ( !target ) {
    // we will create a transfer id
    uuid_generate_time( uuid );
  } else {
    // we will use an external transfer ID
  }

  uuid_unparse( uuid, uuidstring );
  // avoid memory leaks
  XrdOucString transferfile = Tdir;
  transferfile += "/";
  transferfile += uuidstring;
  int rc = creat( transferfile.c_str(), S_IRWXU | S_IRGRP | S_IROTH );

  if ( rc < 0 ) {
    // failed to create
    return errno;
  } else {
    int rw;
    errno = 0;

    while ( ( ( rw = write( rc, params, strlen( params ) + 1 ) ) < 0 ) && ( errno == EINTR ) ) {}

    if ( ( rw < 0 ) || ( rw != ( ( int )strlen( params ) + 1 ) ) ) {
      close( rc );
      return errno;
    }

    close( rc );
  }

  return 0;
}


//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------
XrdTransfer*
XrdTransferManager::GetTransfer( uuid_t uuid, const char* tident )
{
  char uuidstring[40];
  char inoptions[8192];
  uuid_unparse( uuid, uuidstring );
  ManagerLock.Lock();
  // get a transfer from the memory stack
  XrdTransfer* rjob = XrdTransferManager::TM()->InmemoryTransfers.Find( uuidstring );

  if ( rjob ) {
    rjob->nattached ++;
    //    printf("Attach::FromMemory %s now n=%d \n",uuidstring,rjob->nattached);
    ManagerLock.UnLock();
    return rjob;
  }

  // otherwise return from disk and put on memory stack
  XrdOucString transferfile = Tdir;
  transferfile += "/";
  transferfile += uuidstring;
  int rc = open( transferfile.c_str(), O_RDONLY );

  if ( rc < 0 ) {
    // failed to read
    ManagerLock.UnLock();
    return 0;
  } else {
    // parse the options
    inoptions[0] = 0;
    int rb;
    errno = 0;

    while ( ( ( rb = read( rc, inoptions, sizeof( inoptions ) ) ) < 0 ) && ( errno == EINTR ) ) {};

    if ( rb <= 0 ) {
      ManagerLock.UnLock();
      return 0;
    }

    close( rc );
  }

  XrdTransfer* transfer = new XrdTransfer( inoptions, uuid, uuidstring, tident );
  transfer->nattached++;
  //  printf("Attach::New %s now n=%d\n",uuidstring,transfer->nattached);
  ManagerLock.UnLock();
  return transfer;
}


//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------
void
XrdTransferManager::DetachTransfer( XrdTransfer* transfer )
{
  ManagerLock.Lock();
  transfer->nattached --;

  if ( transfer->nattached <= 0 ) {
    transfer->ClearAttached();
    // this can be freed!
    delete transfer;
  }

  ManagerLock.UnLock();
}


//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------
void
XrdTransferManager::DetachTransfer( const char* uuidstring )
{
  ManagerLock.Lock();
  //  printf("Detach::: Checking for %s\n",uuidstring);
  XrdTransfer* transfer = XrdTransferManager::TM()->InmemoryTransfers.Find( uuidstring );

  //  printf("Detach::: found in memory %d\n",transfer);
  if ( transfer ) {
    transfer->nattached --;

    //    printf("Detach:::n= %d\n",transfer->nattached);
    if ( transfer->nattached <= 0 ) {
      transfer->ClearAttached();
      // this can be freed!
      delete transfer;
    }
  }

  ManagerLock.UnLock();
}
