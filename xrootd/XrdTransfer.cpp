/*******************************************************************************
 *                      XrdxCastor2Transfer.cc
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
#include <stdio.h>
#include <sys/time.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <openssl/md5.h>
#include <signal.h>
/*-----------------------------------------------------------------------------*/
#include "XrdClient/XrdClientEnv.hh"
#include "XrdClient/XrdClient.hh"
#include "XrdOuc/XrdOucString.hh"
/*-----------------------------------------------------------------------------*/


#define ERROR_OPEN_SOURCE 1
#define ERROR_ILLEGAL_ARGUMENT 2
#define ERROR_FATAL 3
#define ERROR_READ 4
#define ERROR_WRITE 5
#define ERROR_OPEN_TARGET 6
#define ERROR_CANCEL 7
#define ERROR_TIMEOUT 8
#define ERROR_ABORT 9

#define COPY_BUFFER_SIZE 1024*1024

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
  XrdOucString uuidstring;
  XrdOucString tident;
};

struct TransferRun txf;
XrdOucString ProgressFile = "";
XrdOucString LogFile = "";
XrdOucString Tident = "";
const char* exitfile = 0;


//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------
void UpdateProgress()
{
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
    fprintf( fd, "[%s] Total %.02f MB\t|", txf.uuidstring.c_str(), ( float )txf.sourcesize / 1024 / 1024 );

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
}


//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------
void
AddLog( XrdOucString message )
{
  char timebuf[4096];
  time_t now = time( NULL );
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
  }
}

void
Summary()
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
    message += " md5=";

    for ( int i = 0; i < MD5_DIGEST_LENGTH; i++ ) {
      char m5s[3];
      sprintf( m5s, "%02x", txf.md5string[i] );
      message += m5s;
    }

    message += " ";
  }

  AddLog( message );
}


//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------
void myexit( int code )
{
  if ( !code ) {
    unlink( exitfile );
  }

  exit( code );
}

XrdClient* newclient = 0;
bool canceled = false;


//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------
void sigproc( int s )
{
  signal( SIGHUP, sigproc );
  canceled = true;
  XrdOucString message = "received cancel signal";
  AddLog( message );
}


//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------
void sigalarm( int s )
{
  XrdOucString message = "overall transfer time exceeded";
  AddLog( message );
  myexit( ERROR_TIMEOUT );
}


//------------------------------------------------------------------------------
//
//------------------------------------------------------------------------------
int main( int argc, char* argv[] )
{
  const char* source = argv[1];
  const char* target = argv[2];
  const char* progressfile = argv[3];
  const char* logfile    = argv[4];
  const char* bandwidth  = argv[5];
  const char* domd5      = argv[6];
  const char* uuidstring = argv[7];
  const char* tident     = argv[8];
  const char* timeout    = argv[9];
  const char* fatherpid  = argv[10];
  exitfile   = argv[11];
  newclient = 0;
  signal( SIGHUP, sigproc );
  signal( SIGALRM, sigalarm );
  MD5_Init( &( txf.md5ctx ) );

  if ( ( !source ) || ( !target ) || ( !progressfile ) || ( !logfile ) ||
       ( !bandwidth ) || ( !domd5 ) || ( !uuidstring ) || ( !tident ) ||
       ( !timeout ) || ( !fatherpid ) || ( !exitfile ) ) {
    myexit( ERROR_ILLEGAL_ARGUMENT );
  }

  ProgressFile = progressfile;
  LogFile      = logfile;
  Tident       = tident;

  if ( ProgressFile.length() )
    txf.progress = true;
  else
    txf.progress = false;

  txf.bandwidth = atof( argv[5] );
  txf.computemd5 = atoi( argv[6] );
  txf.uuidstring = uuidstring;
  txf.tident     = tident;
  txf.buffer        = ( char* )malloc( COPY_BUFFER_SIZE );

  if ( !txf.buffer ) {
    myexit( ERROR_FATAL );
  }

  txf.buffersize = COPY_BUFFER_SIZE;
  txf.totalbytes    = 0;
  txf.stopwritebyte = 0;
  unsigned int TimeOut = atoi( timeout );
  pid_t fpid = atoi( fatherpid );

  if ( ( TimeOut == 0 ) || ( TimeOut > ( 24 * 3600 ) ) ) {
    TimeOut = 24 * 3600;
  }

  alarm( TimeOut );
  EnvPutInt( NAME_CONNECTTIMEOUT, 150 );
  EnvPutInt( NAME_MAXREDIRECTCOUNT, 2 );
#ifndef NAME_RECONNECTWAIT
#define NAME_RECONNECTWAIT NAME_RECONNECTTIMEOUT
#endif
  EnvPutInt( NAME_RECONNECTWAIT, 150 );
  EnvPutInt( NAME_FIRSTCONNECTMAXCNT, 3 );
  EnvPutInt( NAME_DATASERVERCONN_TTL, 3600 );
  EnvPutInt( NAME_READCACHESIZE, 0 );
  newclient = new XrdClient( target );
  newclient->UseCache( false );
  int lfd = open( source, O_RDONLY );

  if ( lfd < 0 ) {
    myexit( ERROR_OPEN_SOURCE );
  }  else {
    struct stat buf;

    if ( !fstat( lfd, &buf ) ) {
      txf.sourcesize = buf.st_size;
    } else {
      myexit( ERROR_OPEN_SOURCE );
    }

    bool isopen = false;

    for ( int i = 0; i < 5; i++ ) {
      isopen = newclient->Open( kXR_ur | kXR_uw | kXR_gw | kXR_gr | kXR_or, kXR_mkpath | kXR_open_updt, false );

      if ( isopen ) {
        break;
      }

      XrdOucString message = "retry: unable to open remote file path=";
      message += target;
      AddLog( message );
      usleep( 1000 * i );
    }

    if ( isopen ) {
      gettimeofday( &txf.abs_start_time, &txf.tz );
      XrdOucString message = "connected to target url=";
      message += target;
      AddLog( message );
      //////////////////////////////////////////////////////////////////////////
      // do the transfer
      // copy - routine
      //////////////////////////////////////////////////////////////////////////
      txf. stopwritebyte = 0;

      while ( 1 ) {
        if ( txf.progress ) {
          gettimeofday( &txf.abs_stop_time, &txf.tz );
          UpdateProgress();
        }

        if ( txf.progress ) {
          gettimeofday( &txf.abs_stop_time, &txf.tz );
          UpdateProgress();
        }

        if ( txf.bandwidth ) {
          gettimeofday( &txf.abs_stop_time, &txf.tz );
          float abs_time = ( ( float )( ( txf.abs_stop_time.tv_sec - txf.abs_start_time.tv_sec ) * 1000 +
                                        ( txf.abs_stop_time.tv_usec - txf.abs_start_time.tv_usec ) / 1000 ) );
          // regulate the io - sleep as desired
          float exp_time = txf.totalbytes / txf.bandwidth / 1000.0;

          if ( abs_time < exp_time ) {
            usleep( ( int )( 1000 * ( exp_time - abs_time ) ) );
          }
        }

        int nread;

        while ( ( ( nread = read( lfd, ( void* )( txf.buffer ), txf.buffersize ) ) < 0 ) && ( errno == EINTR ) ) {};

        if ( nread < 0 ) {
          XrdOucString message = "error: read failed on source file - destination file is incomplete!";
          AddLog( message );
          myexit( ERROR_READ );
        }

        if ( nread == 0 ) {
          // end of file
          break;
        }

        if ( txf.computemd5 ) {
          MD5_Update( &( txf.md5ctx ), txf.buffer, nread );
        }

        ssize_t nwrite;
        nwrite = newclient->Write( ( void* ) txf.buffer, txf.stopwritebyte, nread );

        // this is somehow stupid, but XrdClient gives only true/false
        if ( nwrite ) {
          nwrite = nread;
        }

        if ( nwrite != nread ) {
          XrdOucString message = "error: write failed on destination file - destination file is incomplete!";
          AddLog( message );
          myexit( ERROR_WRITE );
        }

        txf.totalbytes += ( long long ) nwrite;
        txf.stopwritebyte += ( long long ) nwrite;

        if ( kill( fpid, 0 ) ) {
          XrdOucString message = "error: father process died - aborting!";
          AddLog( message );
          myexit( ERROR_ABORT );
        }

        if ( nread < txf.buffersize ) {
          // end of file, we can stop now
          break;
        }

        if ( canceled )
          break;
      }

      if ( txf.progress ) {
        gettimeofday( &txf.abs_stop_time, &txf.tz );
        UpdateProgress();
      }

      if ( txf.computemd5 ) {
        MD5_Final( txf.md5string, &( txf.md5ctx ) );
      }

      Summary();
      // close the local file
      close( lfd );

      // if we are canceled we truncate the file
      if ( canceled ) {
        XrdOucString message = "cacneled: truncating destination";
        AddLog( message );
        newclient->Truncate( 0 );
      }

      // close the remote file
      newclient->Close();

      if ( canceled )
        myexit( ERROR_CANCEL );
      else
        myexit( 0 );
    } else {
      newclient->Close();
      // that didn't work
      close( lfd );
      XrdOucString message = "error: cannot open destination file";
      AddLog( message );
      myexit( ERROR_OPEN_TARGET );
    }
  }

  if ( newclient )
    delete newclient;
}
