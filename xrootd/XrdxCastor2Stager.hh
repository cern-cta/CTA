/*******************************************************************************
 *                      XrdxCastor2Stager.hh
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

#ifndef __XCASTOR_STAGER_HH__
#define __XCASTOR_STAGER_HH__

/*-----------------------------------------------------------------------------*/
#include <XrdOuc/XrdOucHash.hh>
#include <XrdOuc/XrdOucErrInfo.hh>
#include <XrdOuc/XrdOucString.hh>
/*-----------------------------------------------------------------------------*/
#include "XrdxCastorLogging.hh"
/*-----------------------------------------------------------------------------*/


//------------------------------------------------------------------------------
//! Class XrdxCastor2Stager
//------------------------------------------------------------------------------
class XrdxCastor2Stager : public LogId
{
  public:
    static XrdOucHash<XrdOucString>* prepare2getstore;
    static XrdOucHash<XrdOucString>* getstore;
    static XrdOucHash<XrdOucString>* putstore;
    static XrdOucHash<XrdOucString>* delaystore;

    static int GetDelayValue( const char* tag );

    //----------------------------------------------------------------------------
    //! Constructor
    //----------------------------------------------------------------------------
    XrdxCastor2Stager(): LogId() {};

    //----------------------------------------------------------------------------
    //! Destructor
    //----------------------------------------------------------------------------
    ~XrdxCastor2Stager() {};


    static bool Prepare2Get( XrdOucErrInfo& error,
                             uid_t uid, gid_t gid,
                             const char* path,
                             const char* stagehost,
                             const char* serviceclass,
                             XrdOucString& redirectionhost,
                             XrdOucString& redirectionpfn,
                             XrdOucString& stagestatus ) ;

    static bool Get( XrdOucErrInfo& error,
                     uid_t uid, gid_t gid,
                     const char* path,
                     const char* stagehost,
                     const char* serviceclass,
                     XrdOucString& redirectionhost,
                     XrdOucString& redirectionpfn,
                     XrdOucString& redirectionpfn2,
                     XrdOucString& stagestatus );

    static bool Put( XrdOucErrInfo& error,
                     uid_t uid, gid_t gid,
                     const char* path,
                     const char* stagehost,
                     const char* serviceclass,
                     XrdOucString& redirectionhost,
                     XrdOucString& redirectionpfn,
                     XrdOucString& redirectionpfn2,
                     XrdOucString& stagestatus );

    static bool Rm( XrdOucErrInfo& error,
                    uid_t uid, gid_t gid,
                    const char* path,
                    const char* stagehost,
                    const char* serviceclass );

    static bool Update( XrdOucErrInfo& error,
                        uid_t uid, gid_t gid,
                        const char* path,
                        const char* stagehost,
                        const char* serviceclass,
                        XrdOucString& redirectionhost,
                        XrdOucString& redirectionpfn,
                        XrdOucString& redirectionpfn2,
                        XrdOucString& stagestatus );

    static bool StagerQuery( XrdOucErrInfo& error,
                             uid_t uid, gid_t gid,
                             const char* path,
                             const char* stagehost,
                             const char* serviceclass,
                             XrdOucString& stagestatus );

    static bool UpdateDone( XrdOucErrInfo& error,
                            const char* path,
                            const char* reqid,
                            const char* fileid,
                            const char* nameserver,
                            const char* stagehost,
                            const char* serviceclass );

    static bool FirstWrite( XrdOucErrInfo& error,
                            const char* path,
                            const char* reqid,
                            const char* fileid,
                            const char* nameserver,
                            const char* stagehost,
                            const char* serviceclass );

    static bool PutFailed( XrdOucErrInfo& error,
                           const char* path,
                           const char* reqid,
                           const char* fileid,
                           const char* nameserver,
                           const char* stagehost,
                           const char* serviceclass );
};

#endif // __XCASTOR_STAGER_HH__
