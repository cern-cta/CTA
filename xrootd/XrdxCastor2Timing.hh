/*******************************************************************************
 *                      XrdxCastor2Timing.hh
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

#ifndef __XCASTOR2FS__TIMING__HH
#define __XCASTOR2FS__TIMING__HH

/*-----------------------------------------------------------------------------*/
#include <sys/time.h>
/*-----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucTrace.hh"
/*-----------------------------------------------------------------------------*/


//------------------------------------------------------------------------------
//! Class XrdxCastor2Timing
//------------------------------------------------------------------------------
class XrdxCastor2Timing
{
  public:
    struct timeval tv;
    XrdOucString tag;
    XrdOucString maintag;
    XrdxCastor2Timing* next;
    XrdxCastor2Timing* ptr;

    //--------------------------------------------------------------------------
    //! Constructor
    //--------------------------------------------------------------------------
    XrdxCastor2Timing( const char* name, struct timeval& i_tv ) {
      memcpy( &tv, &i_tv, sizeof( struct timeval ) );
      tag = name;
      next = NULL;
      ptr  = this;
    }

    //--------------------------------------------------------------------------
    //! Constructor
    //--------------------------------------------------------------------------
    XrdxCastor2Timing( const char* i_maintag ) {
      tag = "BEGIN";
      next = NULL;
      ptr  = this;
      maintag = i_maintag;
    }

    //--------------------------------------------------------------------------
    //! Print function
    //--------------------------------------------------------------------------
    void Print( XrdOucTrace& trace ) {
      char msg[512];

      if ( !( trace.What & 0x8000 ) )
        return;

      XrdxCastor2Timing* p = this->next;
      XrdxCastor2Timing* n;
      trace.Beg( "Timing" );
      cerr << std::endl;

      while ( ( n = p->next ) ) {
        sprintf( msg, "                                        [%12s] %12s<=>%-12s : %.03f\n", maintag.c_str(), p->tag.c_str(), n->tag.c_str(), ( float )( ( n->tv.tv_sec - p->tv.tv_sec ) * 1000000 + ( n->tv.tv_usec - p->tv.tv_usec ) ) / 1000.0 );
        cerr << msg;
        p = n;
      }

      n = p;
      p = this->next;
      sprintf( msg, "                                        =%12s= %12s<=>%-12s : %.03f\n", maintag.c_str(), p->tag.c_str(), n->tag.c_str(), ( float )( ( n->tv.tv_sec - p->tv.tv_sec ) * 1000000 + ( n->tv.tv_usec - p->tv.tv_usec ) ) / 1000.0 );
      cerr << msg;
      trace.End();
    }

    //--------------------------------------------------------------------------
    //! Destructor
    //--------------------------------------------------------------------------
    virtual ~XrdxCastor2Timing() {
      XrdxCastor2Timing* n = next;
      if ( n ) delete n;
    };
};

#define TIMING(__trace__, __ID__,__LIST__)                              \
if (__trace__.What & TRACE_debug)                                       \
do {                                                                    \
     struct timeval tp;                                                 \
     struct timezone tz;                                                \
     gettimeofday(&tp, &tz);                                            \
     (__LIST__)->ptr->next=new XrdxCastor2Timing(__ID__,tp);            \
     (__LIST__)->ptr = (__LIST__)->ptr->next;                           \
} while(0);                                                             \
 
#endif // __XCASTOR2FS__TIMING__HH
