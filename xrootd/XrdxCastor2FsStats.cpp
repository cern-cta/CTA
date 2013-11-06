/*******************************************************************************
 *                      XrdxCastor2FsStats.hh
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
#include "XrdxCastor2FsStats.hpp" 
#include "XrdxCastor2Fs.hpp"
/*-----------------------------------------------------------------------------*/
#include "XrdSys/XrdSysTimer.hh"
#include "XrdOuc/XrdOucTrace.hh"
/*-----------------------------------------------------------------------------*/

extern XrdxCastor2Fs* XrdxCastor2FS; ///< defined in XrdxCastor2Fs.cpp

//------------------------------------------------------------------------------
// Function that starts the stats thread
//------------------------------------------------------------------------------
void* 
XrdxCastor2FsStatsStart( void* pp )
{
  XrdxCastor2FsStats* stats = ( XrdxCastor2FsStats* ) pp;
  stats->UpdateLoop();
  return ( void* ) 0;
}


//--------------------------------------------------------------------------
// Constructor
//--------------------------------------------------------------------------
XrdxCastor2FsStats::XrdxCastor2FsStats( XrdxCastor2Proc* proc ) 
{
  Lock();
  readrate1s = readrate60s = readrate300s = 0;
  writerate1s = writerate60s = writerate300s = 0;
  statrate1s = statrate60s = statrate300s = 0;
  readdrate1s = readdrate60s = readdrate300s = 0;
  rmrate1s = rmrate60s = rmrate300s = 0;
  cmdrate1s = cmdrate60s = cmdrate300s = 0;
  memset( read300s, 0, sizeof( read300s ) );
  memset( write300s, 0, sizeof( write300s ) );
  memset( stat300s, 0, sizeof( stat300s ) );
  memset( readd300s, 0, sizeof( readd300s ) );
  memset( rm300s, 0, sizeof( rm300s ) );
  memset( cmd300s, 0, sizeof( cmd300s ) );
  ServerTable = new XrdOucTable<XrdOucString> ( XCASTOR2FS_MAXFILESYSTEMS );
  UserTable   = new XrdOucTable<XrdOucString> ( XCASTOR2FS_MAXDISTINCTUSERS );
  Proc = proc;
  UnLock();
}


//--------------------------------------------------------------------------
// Destructor
//--------------------------------------------------------------------------
XrdxCastor2FsStats::~XrdxCastor2FsStats() 
{
  if ( ServerTable ) delete ServerTable;
  if ( UserTable )   delete UserTable;
}


//--------------------------------------------------------------------------
// Set the proc object
//--------------------------------------------------------------------------
void 
XrdxCastor2FsStats::SetProc( XrdxCastor2Proc* proc ) {
  Proc = proc;
}


//--------------------------------------------------------------------------
// Increment number of read or write operations
//--------------------------------------------------------------------------
void 
XrdxCastor2FsStats::IncRdWr(bool isRW)
{
  if (isRW)
    IncWrite();
  else 
    IncRead();
}


//--------------------------------------------------------------------------
// Increment reads
//--------------------------------------------------------------------------
void
XrdxCastor2FsStats::IncRead() 
{
  time_t now = time( NULL );
  statmutex.Lock();
  read300s[( now + 1 ) % 300] = 0;
  read300s[now % 300]++;
  IncCmd( false );
  statmutex.UnLock();
}


//--------------------------------------------------------------------------
// Increment writes
//--------------------------------------------------------------------------
void 
XrdxCastor2FsStats::IncWrite() 
{
  time_t now = time( NULL );
  statmutex.Lock();
  write300s[( now + 1 ) % 300] = 0;
  write300s[now % 300]++;
  IncCmd( false );
  statmutex.UnLock();
}


//--------------------------------------------------------------------------
// Increment stats
//--------------------------------------------------------------------------
void 
XrdxCastor2FsStats::IncStat() 
{
  time_t now = time( NULL );
  statmutex.Lock();
  stat300s[( now + 1 ) % 300] = 0;
  stat300s[now % 300]++;
  IncCmd( false );
  statmutex.UnLock();
}


//--------------------------------------------------------------------------
// Increment readd
//--------------------------------------------------------------------------
void 
XrdxCastor2FsStats::IncReadd() 
{
  time_t now = time( NULL );
  statmutex.Lock();
  readd300s[( now + 1 ) % 300] = 0;
  readd300s[now % 300]++;
  IncCmd( false );
  statmutex.UnLock();
}


//--------------------------------------------------------------------------
// Increment rm
//--------------------------------------------------------------------------
void
XrdxCastor2FsStats::IncRm() 
{
  time_t now = time( NULL );
  statmutex.Lock();
  rm300s[( now + 1 ) % 300] = 0;
  rm300s[now % 300]++;
  IncCmd( false );
  statmutex.UnLock();
}


//--------------------------------------------------------------------------
// Increment number of cmds
//--------------------------------------------------------------------------
void 
XrdxCastor2FsStats::IncCmd( bool lock ) 
{
  time_t now = time( NULL );

  if ( lock ) statmutex.Lock();

  cmd300s[( now + 1 ) % 300] = 0;
  cmd300s[now % 300]++;

  if ( lock ) statmutex.UnLock();
}


//--------------------------------------------------------------------------
// Increment server read or write operations 
//--------------------------------------------------------------------------
void 
XrdxCastor2FsStats::IncServerRdWr(const char* server, bool isRW)
{
  if (isRW)
    IncServerWrite(server);
  else 
    IncServerRead(server);
}


//--------------------------------------------------------------------------
// Increment server reads
//--------------------------------------------------------------------------
void 
XrdxCastor2FsStats::IncServerRead( const char* server ) 
{
  statmutex.Lock();
  XrdxCastor2StatULongLong* rc = ServerRead.Find( server );

  if ( !rc ) {
    rc = new XrdxCastor2StatULongLong();
    rc->Inc();
    ServerRead.Add( server, rc );

    if ( !ServerTable->Find( server ) ) {
      ServerTable->Insert( new XrdOucString( server ), server );
    }
  } else {
    rc->Inc();
  }

  statmutex.UnLock();
}


//--------------------------------------------------------------------------
// Increment server writes
//--------------------------------------------------------------------------
void 
XrdxCastor2FsStats::IncServerWrite( const char* server ) 
{
  statmutex.Lock();
  XrdxCastor2StatULongLong* rc = ServerWrite.Find( server );

  if ( !rc ) {
    rc = new XrdxCastor2StatULongLong();
    rc->Inc();
    ServerWrite.Add( server, rc );

    if ( !ServerTable->Find( server ) ) {
      ServerTable->Insert( new XrdOucString( server ), server );
    }
  } else {
    rc->Inc();
  }

  statmutex.UnLock();
}


//--------------------------------------------------------------------------
// Increment user read or write operations
//--------------------------------------------------------------------------
void 
XrdxCastor2FsStats::IncUserRdWr(const char* user, bool isRW)
{
  if (isRW)
    IncUserWrite(user);
  else 
    IncUserRead(user);
}


//--------------------------------------------------------------------------
// Increment user reads
//--------------------------------------------------------------------------
void 
XrdxCastor2FsStats::IncUserRead( const char* user ) 
{
  statmutex.Lock();
  XrdxCastor2StatULongLong* rc = UserRead.Find( user );

  if ( !rc ) {
    rc = new XrdxCastor2StatULongLong();
    rc->Inc();
    UserRead.Add( user, rc );
    UserTable->Insert( new XrdOucString( user ), user );
  } else {
    rc->Inc();
  }

  statmutex.UnLock();
}


//--------------------------------------------------------------------------
// Incremenet server writes
//--------------------------------------------------------------------------
void 
XrdxCastor2FsStats::IncUserWrite( const char* user ) 
{
  statmutex.Lock();
  XrdxCastor2StatULongLong* rc = UserWrite.Find( user );

  if ( !rc ) {
    rc = new XrdxCastor2StatULongLong();
    rc->Inc();
    UserWrite.Add( user, rc );
    UserTable->Insert( new XrdOucString( user ), user );
  } else {
    rc->Inc();
  }

  statmutex.UnLock();
}


//--------------------------------------------------------------------------
// Get read rate 
//--------------------------------------------------------------------------
double 
XrdxCastor2FsStats::ReadRate( int nbins ) 
{
  if ( !nbins )
    return 0;

  time_t now = time( NULL );
  double sum = 0;

  for ( int i = 0 ; i < nbins; i++ ) {
    sum += ( read300s[( now - 1 - i ) % 300] );
  }

  sum /= nbins;
  return sum;
}


//--------------------------------------------------------------------------
// Get write rate
//--------------------------------------------------------------------------
double 
XrdxCastor2FsStats::WriteRate( int nbins ) 
{
  if ( !nbins )
    return 0;

  time_t now = time( NULL );
  double sum = 0;

  for ( int i = 0 ; i < nbins; i++ ) {
    sum += ( write300s[( now - 1 - i ) % 300] );
  }

  sum /= nbins;
  return sum;
}


//--------------------------------------------------------------------------
// Get stat rate
//--------------------------------------------------------------------------
double 
XrdxCastor2FsStats::StatRate( int nbins ) 
{
  if ( !nbins )
    return 0;

  time_t now = time( NULL );
  double sum = 0;

  for ( int i = 0 ; i < nbins; i++ ) {
    sum += ( stat300s[( now - 1 - i ) % 300] );
  }

  sum /= nbins;
  return sum;
}


//--------------------------------------------------------------------------
// Get readd rate
//--------------------------------------------------------------------------
double 
XrdxCastor2FsStats::ReaddRate( int nbins ) 
{
  if ( !nbins )
    return 0;

  time_t now = time( NULL );
  double sum = 0;

  for ( int i = 0 ; i < nbins; i++ ) {
    sum += ( readd300s[( now - 1 - i ) % 300] );
  }

  sum /= nbins;
  return sum;
}


//--------------------------------------------------------------------------
// Get rm rate
//--------------------------------------------------------------------------
double 
XrdxCastor2FsStats::RmRate( int nbins ) 
{
  if ( !nbins )
    return 0;

  time_t now = time( NULL );
  double sum = 0;

  for ( int i = 0 ; i < nbins; i++ ) {
    sum += ( rm300s[( now - 1 - i ) % 300] );
  }

  sum /= nbins;
  return sum;
}


//--------------------------------------------------------------------------
// Get cmds rate
//--------------------------------------------------------------------------
double 
XrdxCastor2FsStats::CmdRate( int nbins ) 
{
  if ( !nbins )
    return 0;

  time_t now = time( NULL );
  double sum = 0;

  for ( int i = 0 ; i < nbins; i++ ) {
    sum += ( cmd300s[( now - 1 - i ) % 300] );
  }

  sum /= nbins;
  return sum;
}


//--------------------------------------------------------------------------
// Run update loop
//--------------------------------------------------------------------------
void 
XrdxCastor2FsStats::UpdateLoop() 
{
  while ( 1 ) {
    XrdSysTimer::Wait( 490 );
    time_t now = time( NULL );
    read300s[( now + 1 ) % 300] = 0;
    write300s[( now + 1 ) % 300] = 0;
    stat300s[( now + 1 ) % 300] = 0;
    readd300s[( now + 1 ) % 300] = 0;
    rm300s[( now + 1 ) % 300] = 0;
    cmd300s[( now + 1 ) % 300] = 0;
    Update();
  }
}


//------------------------------------------------------------------------------
// Update stats
//------------------------------------------------------------------------------
void
XrdxCastor2FsStats::Update()
{
  if ( Proc ) {
    XrdxCastor2FS->Stats.Lock();
    {
      XrdxCastor2ProcFile* pf = Proc->Handle( "read1" );
      pf && pf->Write( ReadRate( 1 ), 1 );
    }
    {
      XrdxCastor2ProcFile* pf = Proc->Handle( "read60" );
      pf && pf->Write( ReadRate( 60 ), 1 );
    }
    {
      XrdxCastor2ProcFile* pf = Proc->Handle( "read300" );
      pf && pf->Write( ReadRate( 298 ), 1 );
    }
    {
      XrdxCastor2ProcFile* pf = Proc->Handle( "write1" );
      pf && pf->Write( WriteRate( 1 ), 1 );
    }
    {
      XrdxCastor2ProcFile* pf = Proc->Handle( "write60" );
      pf && pf->Write( WriteRate( 60 ), 1 );
    }
    {
      XrdxCastor2ProcFile* pf = Proc->Handle( "write300" );
      pf && pf->Write( WriteRate( 298 ), 1 );
    }
    {
      XrdxCastor2ProcFile* pf = Proc->Handle( "stat1" );
      pf && pf->Write( StatRate( 1 ), 1 );
    }
    {
      XrdxCastor2ProcFile* pf = Proc->Handle( "stat60" );
      pf && pf->Write( StatRate( 60 ), 1 );
    }
    {
      XrdxCastor2ProcFile* pf = Proc->Handle( "stat300" );
      pf && pf->Write( StatRate( 298 ), 1 );
    }
    {
      XrdxCastor2ProcFile* pf = Proc->Handle( "readd1" );
      pf && pf->Write( ReaddRate( 1 ), 1 );
    }
    {
      XrdxCastor2ProcFile* pf = Proc->Handle( "readd60" );
      pf && pf->Write( ReaddRate( 60 ), 1 );
    }
    {
      XrdxCastor2ProcFile* pf = Proc->Handle( "readd300" );
      pf && pf->Write( ReaddRate( 298 ), 1 );
    }
    {
      XrdxCastor2ProcFile* pf = Proc->Handle( "rm1" );
      pf && pf->Write( RmRate( 1 ), 1 );
    }
    {
      XrdxCastor2ProcFile* pf = Proc->Handle( "rm60" );
      pf && pf->Write( RmRate( 60 ), 1 );
    }
    {
      XrdxCastor2ProcFile* pf = Proc->Handle( "rm300" );
      pf && pf->Write( RmRate( 298 ), 1 );
    }
    {
      XrdxCastor2ProcFile* pf = Proc->Handle( "cmd1" );
      pf && pf->Write( CmdRate( 1 ), 1 );
    }
    {
      XrdxCastor2ProcFile* pf = Proc->Handle( "cmd60" );
      pf && pf->Write( CmdRate( 60 ), 1 );
    }
    {
      XrdxCastor2ProcFile* pf = Proc->Handle( "cmd300" );
      pf && pf->Write( CmdRate( 298 ), 1 );
    }
    {
      XrdxCastor2ProcFile* pf = Proc->Handle( "delayread" );

      if ( pf ) {
        XrdxCastor2FS->xCastor2FsDelayRead  = pf->Read();
      }
    }
    {
      XrdxCastor2ProcFile* pf = Proc->Handle( "delaywrite" );

      if ( pf ) {
        XrdxCastor2FS->xCastor2FsDelayWrite = pf->Read();
      }
    }
    {
      XrdxCastor2ProcFile* pf = Proc->Handle( "trace" );

      if ( pf ) {
        long int log_level = 0;
        XrdOucString slog_level;
        if (pf->Read(slog_level))
        {
          log_level = Logging::GetPriorityByString(slog_level.c_str());

          if (log_level == -1)
          {
            // Maybe the log level is specified as an int from 0 to 7
            char* end;
            errno = 0;
            log_level = (int) strtol(slog_level.c_str(), &end, 10);
            
            if (!(errno == ERANGE && ((log_level == LONG_MIN) || (log_level == LONG_MAX))) &&
                !((errno != 0) && (log_level == 0)))
            {
              if (end != slog_level.c_str())
              {
                // Conversion successful - the log level was an int
                if ((log_level >= 0) && (log_level <= 7))
                  XrdxCastor2FS->SetLogLevel(log_level);
              }
            }
          }
          else
          {
            // Log level was a string
            XrdxCastor2FS->SetLogLevel(log_level);
          }
        }
      }
    }

    XrdxCastor2FS->Stats.UnLock();
    {
      XrdxCastor2ProcFile* pf = Proc->Handle( "serverread" );

      if ( pf ) {
        XrdxCastor2FS->Stats.Lock();
        // Loop over the ServerTable and write keyval pairs for each server
        int cursor = 0;
        bool first = true;

        do {
          cursor = XrdxCastor2FS->Stats.ServerTable->Next( cursor );

          if ( cursor >= 0 ) {
            XrdOucString* name = XrdxCastor2FS->Stats.ServerTable->Item( cursor );

            if ( !name ) {
              cursor++;
              continue;
            }

            XrdxCastor2StatULongLong* sval;

            if ( ( sval = XrdxCastor2FS->Stats.ServerRead.Find( name->c_str() ) ) ) {
              // If we don't write in this time bin, we just stop the loop - 
              // or if there is an error writing
              if ( !pf->WriteKeyVal( name->c_str(), sval->Get(), 2, first ) )
                break;

              first = false;
            }

            cursor++;
          }
        } while ( cursor >= 0 );

        XrdxCastor2FS->Stats.UnLock();
      }
    }
    {
      XrdxCastor2ProcFile* pf = Proc->Handle( "serverwrite" );

      if ( pf ) {
        // Loop over the ServerTable and write keyval pairs for each server
        XrdxCastor2FS->Stats.Lock();
        int cursor = 0;
        bool first = true;

        do {
          cursor = XrdxCastor2FS->Stats.ServerTable->Next( cursor );

          if ( cursor >= 0 ) {
            XrdOucString* name = XrdxCastor2FS->Stats.ServerTable->Item( cursor );

            if ( !name ) {
              cursor++;
              continue;
            }

            XrdxCastor2StatULongLong* sval;

            if ( ( sval = XrdxCastor2FS->Stats.ServerWrite.Find( name->c_str() ) ) ) {
              // If we don't write in this time bin, we just stop the loop - 
              // or if there is an error writing
              if ( !pf->WriteKeyVal( name->c_str(), sval->Get(), 2, first ) )
                break;

              first = false;
            }

            cursor++;
          }
        } while ( cursor >= 0 );

        XrdxCastor2FS->Stats.UnLock();
      }
    }
    {
      XrdxCastor2ProcFile* pf = Proc->Handle( "userread" );

      if ( pf ) {
        XrdxCastor2FS->Stats.Lock();
        // Loop over the UserTable and write keyval pairs for each server
        int cursor = 0;
        bool first = true;

        do {
          cursor = XrdxCastor2FS->Stats.UserTable->Next( cursor );

          if ( cursor >= 0 ) {
            XrdOucString* name = XrdxCastor2FS->Stats.UserTable->Item( cursor );

            if ( !name ) {
              cursor++;
              continue;
            }

            XrdxCastor2StatULongLong* sval;

            if ( ( sval = XrdxCastor2FS->Stats.UserRead.Find( name->c_str() ) ) ) {
              // If we don't write in this time bin, we just stop the loop - 
              // or if there is an error writing
              if ( !pf->WriteKeyVal( name->c_str(), sval->Get(), 2, first ) )
                break;

              first = false;
            }

            cursor++;
          }
        } while ( cursor >= 0 );

        XrdxCastor2FS->Stats.UnLock();
      }
    }
    {
      XrdxCastor2ProcFile* pf = Proc->Handle( "userwrite" );

      if ( pf ) {
        // Loop over the UserTable and write keyval pairs for each serve
        XrdxCastor2FS->Stats.Lock();
        int cursor = 0;
        bool first = true;

        do {
          cursor = XrdxCastor2FS->Stats.UserTable->Next( cursor );

          if ( cursor >= 0 ) {
            XrdOucString* name = XrdxCastor2FS->Stats.UserTable->Item( cursor );

            if ( !name ) {
              cursor++;
              continue;
            }

            XrdxCastor2StatULongLong* sval;

            if ( ( sval = XrdxCastor2FS->Stats.UserWrite.Find( name->c_str() ) ) ) {
              // If we don't write in this time bin, we just stop the loop - 
              // or if there is an error writing
              if ( !pf->WriteKeyVal( name->c_str(), sval->Get(), 2, first ) )
                break;

              first = false;
            }

            cursor++;
          }
        } while ( cursor >= 0 );

        XrdxCastor2FS->Stats.UnLock();
      }
    }
  }
}
