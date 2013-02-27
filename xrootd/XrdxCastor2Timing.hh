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

#ifndef __XCASTOR_TIMING_HH__
#define __XCASTOR_TIMING_HH__

/*----------------------------------------------------------------------------*/
#include <sys/time.h>
/*----------------------------------------------------------------------------*/
#include "XrdxCastorNamespace.hh"
/*----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucString.hh"
#include "XrdOuc/XrdOucTrace.hh"
/*----------------------------------------------------------------------------*/

XCASTORNAMESPACE_BEGIN

/*----------------------------------------------------------------------------*/
//! Class implementing comfortable time measurements through methods/functions
//! 
//! Example
//! Timing tm("Test");
//! COMMONTIMING("START",&tm);
//! ...
//! COMMONTIMING("CHECKPOINT1",&tm);
//! ...
//! COMMONTIMING("CHECKPOINT2",&tm);
//! ...
//! COMMONTIMING("STOP", &tm);
//! tm.Print();
//! fprintf(stdout,"realtime = %.02f", tm.RealTime());
/*----------------------------------------------------------------------------*/
class Timing {
public:
  struct timeval tv;
  XrdOucString tag;
  XrdOucString maintag;
  Timing* next;
  Timing* ptr;

  //----------------------------------------------------------------------------
  //! Constructor - used only internally
  //----------------------------------------------------------------------------
  Timing(const char* name, struct timeval &i_tv) {
    memcpy(&tv, &i_tv, sizeof(struct timeval));
    tag = name;
    next = 0;
    ptr  = this;
  }

  //----------------------------------------------------------------------------
  //! Constructor - tag is used as the name for the measurement in Print
  //----------------------------------------------------------------------------
  Timing(const char* i_maintag) {
    tag = "BEGIN";
    next = 0;
    ptr  = this;
    maintag = i_maintag;
  }

  //----------------------------------------------------------------------------
  //! Print method to display measurements on STDERR
  //----------------------------------------------------------------------------
  void Print() {
    char msg[512];
    Timing* p = this->next;
    Timing* n; 
    cerr << std::endl;
    while ((n =p->next)) {

      sprintf(msg,"                                        [%12s] %12s<=>%-12s : %.03f\n",maintag.c_str(),p->tag.c_str(),n->tag.c_str(), (float)((n->tv.tv_sec - p->tv.tv_sec) *1000000 + (n->tv.tv_usec - p->tv.tv_usec))/1000.0);
      cerr << msg;
      p = n;
    }
    n = p;
    p = this->next;
    sprintf(msg,"                                        =%12s= %12s<=>%-12s : %.03f\n",maintag.c_str(),p->tag.c_str(), n->tag.c_str(), (float)((n->tv.tv_sec - p->tv.tv_sec) *1000000 + (n->tv.tv_usec - p->tv.tv_usec))/1000.0);
    cerr << msg;
  }

  //----------------------------------------------------------------------------
  //! Return total Realtime
  //----------------------------------------------------------------------------
  double RealTime() {
    Timing* p = this->next;
    Timing* n; 
    while ((n =p->next)) {
      p = n;
    }
    n = p;
    p = this->next;
    return (double) ((n->tv.tv_sec - p->tv.tv_sec) *1000000 + (n->tv.tv_usec - p->tv.tv_usec))/1000.0;
  }

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~Timing(){Timing* n = next; if (n) delete n;}


  //----------------------------------------------------------------------------
  //! Wrapper Function to hide difference between Apple and Linux
  //----------------------------------------------------------------------------
  static void GetTimeSpec(struct timespec &ts) {
#ifdef __APPLE__
    struct timeval tv;
    gettimeofday(&tv, 0);
    ts.tv_sec = tv.tv_sec;
    ts.tv_nsec = tv.tv_usec * 1000;
#else
    clock_gettime(CLOCK_REALTIME, &ts);
#endif
  }    
};


//----------------------------------------------------------------------------
//! Macro to place a measurement throughout the code
//----------------------------------------------------------------------------
#define TIMING(__ID__, __LIST__)                \
  do {                                          \
    struct timeval tp;                          \
    struct timezone tz;                         \
    gettimeofday(&tp, &tz);                     \
    (__LIST__)->ptr->next=new xcastor::Timing(__ID__,tp);       \
    (__LIST__)->ptr = (__LIST__)->ptr->next;    \
  } while(0);

XCASTORNAMESPACE_END

#endif // __XCASTOR_TIMING_HH__


