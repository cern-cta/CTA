/*******************************************************************************
 *                      XrdxCastor2OfsProc.cc
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
#include "XrdOuc/XrdOucTrace.hh"
/*-----------------------------------------------------------------------------*/

extern XrdOucTrace OfsTrace;


//------------------------------------------------------------------------------
// Read value from file 
//------------------------------------------------------------------------------
#define READLONGLONGFROMFILE(_name_,_val_)  \
  do {                                      \
    FILE* fp = fopen(_name_,"r");           \
    if (!fp)                                \
      _val_=-1;                             \
    else {                                  \
      char line[4096];                      \
      if ((fscanf(fp,"%s",line)) != 1)      \
  _val_=-1;                                 \
      _val_=strtoll(line,(char**)NULL,0);   \
      fclose(fp);                           \
    }                                       \
  } while (0);


//------------------------------------------------------------------------------
// Write value to file
//------------------------------------------------------------------------------
#define WRITELONGLONGTOFILE(_name_,_val_)   \
  do {                                      \
    FILE* fp = fopen(_name_,"w+");          \
    if (!fp)                                \
      _val_=-1;                             \
    else {                                  \
      _val_ = (int) fprintf(fp,"%lld\n",(long long)_val_);\
       fclose(fp);                          \
    }                                       \
  } while (0);



//------------------------------------------------------------------------------
// Write values to proc file
//------------------------------------------------------------------------------
bool
XrdxCastor2Ofs::Write2ProcFile( const char* name, long long val )
{
  XrdOucString procname;
  procname = Procfilesystem;
  procname += "proc/";
  procname += name;
  //  printf("Writing %s => %lld\n",procname.c_str(),val);
  WRITELONGLONGTOFILE( procname.c_str(), val );
  return ( val > 0 );
}


//------------------------------------------------------------------------------
// Update values in proc file
//------------------------------------------------------------------------------
bool
XrdxCastor2Ofs::UpdateProc( const char* inname )
{
  XrdOucString modname = inname;
  modname.assign( modname, modname.find( "/proc" ) );
  const char* name = modname.c_str();

  if ( ThirdPartyCopy ) {
    if ( !strcmp( name, "/proc/thirdpartycopyslots" ) ) {
      return Write2ProcFile( "thirdpartycopyslots", ( long long )ThirdPartyCopySlots );
    }

    if ( !strcmp( name, "/proc/thirdpartycopyslotrate" ) ) {
      return Write2ProcFile( "thirdpartycopyslotrate", ( long long )ThirdPartyCopySlotRate );
    }
  }

  if ( !strcmp( name, "/proc/trace" ) ) {
    return true;
  }

  if ( !strcmp( name, "*" ) ) {
    bool result = true;

    if ( ThirdPartyCopy ) {
      result *= Write2ProcFile( "thirdpartycopyslots", ThirdPartyCopySlots );
      result *= Write2ProcFile( "thirdpartycopyslotrate", ThirdPartyCopySlotRate );
    }

    return result;
  }

  return false;
}


//------------------------------------------------------------------------------
// Read all values from proc
//------------------------------------------------------------------------------
bool
XrdxCastor2Ofs::ReadAllProc()
{
  bool result = true;

  if ( ThirdPartyCopy ) {
    if ( !ReadFromProc( "thirdpartycopyslots" ) ) result = false;
    if ( !ReadFromProc( "thirdpartycopyslotrate" ) ) result = false;
  }

  ReadFromProc( "trace" );
  return result;
}


//------------------------------------------------------------------------------
// Read a specific value from proc
//------------------------------------------------------------------------------
bool
XrdxCastor2Ofs::ReadFromProc( const char* entryname )
{
  XrdOucString procname = Procfilesystem;
  XrdOucString oucentry = entryname;
  long long val = 0;
  procname += "proc/";
  procname += entryname;
  READLONGLONGFROMFILE( procname.c_str(), val );

  //  printf("Reading %s => %lld\n",procname.c_str(),val);
  if ( val == -1 )
    return false;

  if ( entryname == "thirdpartycopyslots" ) {
    ThirdPartyCopySlots = ( unsigned int )val;
    return true;
  }

  if ( entryname == "thirdpartycopyslotrate" ) {
    ThirdPartyCopySlotRate = ( unsigned int ) val;
    return true;
  }

  if ( entryname == "trace" ) {
    OfsTrace.What = val;
    return true;
  }

  return false;
}

