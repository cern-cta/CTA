/*******************************************************************************
 *                      XrdxCastor2ClientAdmin.hh
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

#ifndef __XCASTOR_CLIENTADMIN_HH__
#define __XCASTOR_CLIENTADMIN_HH__

//------------------------------------------------------------------------------
//! Class XrdxCastor2ClientAdmin
//------------------------------------------------------------------------------
class XrdxCastor2ClientAdmin
{
    XrdSysMutex lock;
    XrdClientAdmin* Admin;

  public:

    XrdxCastor2ClientAdmin( const char* url ) {
      Admin = new XrdClientAdmin( url );
    };

    ~XrdxCastor2ClientAdmin() {
      if ( Admin ) delete Admin;
    };

    void Lock() {
      lock.Lock();
    };

    void UnLock() {
      lock.UnLock();
    };

    XrdClientAdmin* GetAdmin() {
      return Admin;
    };
};

#endif // __XCASTOR_CLIENTADMIN_HH__
