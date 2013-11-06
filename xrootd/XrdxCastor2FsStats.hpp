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

#ifndef __XCASTOR_FSSTATS_HH__
#define __XCASTOR_FSSTATS_HH__

/*-----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucHash.hh"
#include "XrdOuc/XrdOucTable.hh"
#include "XrdSys/XrdSysPthread.hh"
/*-----------------------------------------------------------------------------*/
#include "XrdxCastor2Proc.hpp"
#include "XrdxCastor2FsConstants.hpp"
/*-----------------------------------------------------------------------------*/


//------------------------------------------------------------------------------
//! Class XrdxCastor2StatULongLong - helping class for the stats
//------------------------------------------------------------------------------
class XrdxCastor2StatULongLong
{

  private:
    unsigned long long cnt;

  public:

    XrdxCastor2StatULongLong() {
      Reset();
    };

    virtual ~XrdxCastor2StatULongLong() {};

    void Inc() {
      cnt++;
    }

    unsigned long long Get() {
      return cnt;
    }

    void Reset() {
      cnt = 0;
    }
};


//------------------------------------------------------------------------------
//! Class XrdxCastor2FsStats
//------------------------------------------------------------------------------
class XrdxCastor2FsStats
{

  private:

    long long read300s[300];
    long long write300s[300];
    long long stat300s[300];
    long long readd300s[300];
    long long rm300s[300];
    long long cmd300s[300];

    double readrate1s;
    double readrate60s;
    double readrate300s;

    double writerate1s;
    double writerate60s;
    double writerate300s;

    double statrate1s;
    double statrate60s;
    double statrate300s;

    double readdrate1s;
    double readdrate60s;
    double readdrate300s;

    double rmrate1s;
    double rmrate60s;
    double rmrate300s;

    double cmdrate1s;
    double cmdrate60s;
    double cmdrate300s;

    XrdOucHash<XrdxCastor2StatULongLong> ServerRead;
    XrdOucHash<XrdxCastor2StatULongLong> ServerWrite;
    XrdOucTable<XrdOucString>* ServerTable;

    XrdOucHash<XrdxCastor2StatULongLong> UserRead;
    XrdOucHash<XrdxCastor2StatULongLong> UserWrite;
    XrdOucTable<XrdOucString>* UserTable;

    XrdSysMutex statmutex;
    XrdxCastor2Proc* Proc;

  public:

    //--------------------------------------------------------------------------
    //! Constructor
    //--------------------------------------------------------------------------
    XrdxCastor2FsStats( XrdxCastor2Proc* proc = NULL );


    //--------------------------------------------------------------------------
    //! Destructor
    //--------------------------------------------------------------------------
    virtual ~XrdxCastor2FsStats();

    void SetProc( XrdxCastor2Proc* proc ) ;
    void IncRdWr(bool isRW);
    void IncRead() ;
    void IncWrite();
    void IncStat();
    void IncReadd();
    void IncRm();
    void IncCmd( bool lock = true );
    void IncServerRdWr(const char* server, bool isRW);
    void IncServerRead( const char* server );
    void IncServerWrite( const char* server );
    void IncUserRdWr(const char* user, bool isRW);
    void IncUserRead( const char* user );
    void IncUserWrite( const char* user );
    double ReadRate( int nbins );
    double WriteRate( int nbins );
    double StatRate( int nbins );
    double ReaddRate( int nbins ); 
    double RmRate( int nbins );
    double CmdRate( int nbins );
    void Update();
    void UpdateLoop();

    inline void Lock() {
      statmutex.Lock();
    }

    inline void UnLock() {
      statmutex.UnLock();
    }
};

extern void* XrdxCastor2FsStatsStart( void* pp );

#endif // __XCASTOR_FSSTATS_HH__
