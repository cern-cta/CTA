/*******************************************************************************
 *                      XrdxCastorTiming.cc
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

/*----------------------------------------------------------------------------*/
#include "XrdxCastorTiming.hh"
/*----------------------------------------------------------------------------*/

XCASTORNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Constructor - used only internally
//------------------------------------------------------------------------------
Timing::Timing(const char* name, struct timeval &i_tv) 
{
  memcpy(&tv, &i_tv, sizeof(struct timeval));
  tag = name;
  next = 0;
  ptr  = this;
}


//------------------------------------------------------------------------------
// Constructor - tag is used as the name for the measurement in Print
//------------------------------------------------------------------------------
Timing::Timing(const char* i_maintag) 
{
  tag = "BEGIN";
  next = 0;
  ptr  = this;
  maintag = i_maintag;
}


//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
Timing::~Timing()
{
  Timing* n = next; 
  if (n) delete n;
}


//------------------------------------------------------------------------------
// Print method to display measurements on STDERR
//------------------------------------------------------------------------------
void 
Timing::Print() 
{
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


//------------------------------------------------------------------------------
// Return total Realtime
//------------------------------------------------------------------------------
double 
Timing::RealTime() 
{
  Timing* p = this->next;
  Timing* n; 
  while ((n =p->next)) {
    p = n;
  }
  n = p;
  p = this->next;
  return (double) ((n->tv.tv_sec - p->tv.tv_sec) *1000000 + (n->tv.tv_usec - p->tv.tv_usec))/1000.0;
}


//------------------------------------------------------------------------------
// Wrapper Function to hide difference between Apple and Linux
//------------------------------------------------------------------------------
void 
Timing::GetTimeSpec(struct timespec &ts) 
{
#ifdef __APPLE__
  struct timeval tv;
  gettimeofday(&tv, 0);
  ts.tv_sec = tv.tv_sec;
  ts.tv_nsec = tv.tv_usec * 1000;
#else
  clock_gettime(CLOCK_REALTIME, &ts);
#endif
}    

XCASTORNAMESPACE_END
