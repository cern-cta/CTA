/*******************************************************************************
 *                      XrdxCastor2Proc.cc
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
#include "XrdxCastor2Proc.hh"
/*-----------------------------------------------------------------------------*/


/*****************************************************************************/
/*                         X r d x C a s t o r 2 P r o c                     */
/*****************************************************************************/

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
XrdxCastor2Proc:: XrdxCastor2Proc( const char* procdir, bool syncit )
{
  procdirectory = procdir;
  procsync = syncit;
}


//------------------------------------------------------------------------------
// Create proc directory
//------------------------------------------------------------------------------
bool
XrdxCastor2Proc::Open()
{
  XrdOucString doit = "mkdir -p ";
  doit += procdirectory;
  system( doit.c_str() );
  DIR* pd = opendir( procdirectory.c_str() );

  if ( !pd ) {
    return false;
  } else {
    closedir( pd );
    return true;
  }
}


//------------------------------------------------------------------------------
// Get handle to proc file
//------------------------------------------------------------------------------
XrdxCastor2ProcFile*
XrdxCastor2Proc::Handle( const char* name )
{
  XrdxCastor2ProcFile* phandle = 0;

  if ( ( phandle = files.Find( name ) ) ) {
    return phandle;
  } else {
    XrdOucString pfname = procdirectory;
    pfname += "/";
    pfname += name;
    phandle = new XrdxCastor2ProcFile( pfname.c_str() );

    if ( phandle && phandle->Open() ) {
      files.Add( name, phandle );
      return phandle;
    }
  }

  return NULL;
}


/*****************************************************************************/
/*                 X r d x C a s t o r 2 P r o c F i l e                     */
/*****************************************************************************/

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
XrdxCastor2ProcFile::XrdxCastor2ProcFile( const char* name, bool syncit )
{
  fname = name;
  fd = 0;
  procsync = syncit;
  lastwrite = 0;
}


//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
XrdxCastor2ProcFile::~XrdxCastor2ProcFile() 
{
  Close();
}


//------------------------------------------------------------------------------
// Open proc file
//------------------------------------------------------------------------------
bool
XrdxCastor2ProcFile::Open()
{
  if ( procsync ) {
    fd = open( fname.c_str(), O_CREAT | O_SYNC | O_RDWR, S_IRWXU | S_IROTH | S_IRGRP );
  } else {
    fd = open( fname.c_str(), O_CREAT | O_RDWR, S_IRWXU | S_IROTH | S_IRGRP );
  }

  if ( fd < 0 ) {
    return false;
  }

  return true;
}


//------------------------------------------------------------------------------
// Close proc file
//------------------------------------------------------------------------------
bool
XrdxCastor2ProcFile::Close()
{
  if ( fd >= 0 ) close( fd );

  return true;
}


//------------------------------------------------------------------------------
// Write long long to proc file
//------------------------------------------------------------------------------
bool
XrdxCastor2ProcFile::Write( long long val, int writedelay )
{
  char pbuf[1024];
  sprintf( pbuf, "%lld\n", val );
  return Write( pbuf, writedelay );
}


//------------------------------------------------------------------------------
// Write double to proc file
//------------------------------------------------------------------------------
bool
XrdxCastor2ProcFile::Write( double val, int writedelay )
{
  char pbuf[1024];
  sprintf( pbuf, "%.02f\n", val );
  return Write( pbuf, writedelay );
}


//------------------------------------------------------------------------------
// Write string to proc file
//------------------------------------------------------------------------------
bool
XrdxCastor2ProcFile::Write( const char* pbuf, int writedelay )
{
  time_t now = time( NULL );

  if ( writedelay ) {
    if ( now - lastwrite < writedelay ) {
      return true;
    }
  }

  int result;
  lseek( fd, 0, SEEK_SET );

  while ( ( result =::ftruncate( fd, 0 ) ) && ( errno == EINTR ) ) {}

  lastwrite = now;

  if ( ( write( fd, pbuf, strlen( pbuf ) ) ) == ( ssize_t )( strlen( pbuf ) ) ) {
    return true;
  } else {
    return false;
  }
}


//------------------------------------------------------------------------------
// Write key - value with an option to truncate
//------------------------------------------------------------------------------
bool
XrdxCastor2ProcFile::WriteKeyVal( const char*        key,
                                  unsigned long long value,
                                  int                writedelay,
                                  bool               dotruncate )
{
  if ( dotruncate ) {
    time_t now = time( NULL );

    if ( writedelay ) {
      if ( now - lastwrite < writedelay ) {
        return false;
      }
    }

    // printf("Truncating FD %d for %s\n",fd,key);
    lseek( fd, 0, SEEK_SET );

    while ( ( ::ftruncate( fd, 0 ) ) && ( errno == EINTR ) ) {}

    lastwrite = now;
  }

  char pbuf[1024];
  sprintf( pbuf, "%lu %-32s %lld\n", ( unsigned long )time( NULL ), key, value );

  if ( ( write( fd, pbuf, strlen( pbuf ) ) ) == ( ssize_t )( strlen( pbuf ) ) ) {
    return true;
  } else {
    return false;
  }
}


//------------------------------------------------------------------------------
// Read from proc file
//------------------------------------------------------------------------------
long long
XrdxCastor2ProcFile::Read()
{
  char pbuf[1024];
  lseek( fd, 0, SEEK_SET );
  ssize_t rb = read( fd, pbuf, sizeof( pbuf ) );

  if ( rb <= 0 )
    return -1;

  return strtoll( pbuf, ( char** )NULL, 10 );
}


//------------------------------------------------------------------------------
// Read from proc file
//------------------------------------------------------------------------------
bool
XrdxCastor2ProcFile::Read( XrdOucString& str )
{
  char pbuf[1024];
  pbuf[0] = 0;
  lseek( fd, 0, SEEK_SET );
  ssize_t rb = read( fd, pbuf, sizeof( pbuf ) );
  str = pbuf;

  if ( rb <= 0 )
    return false;
  else
    return true;
}



