/*******************************************************************************
 *                      XrdxCastor2Proc.hh
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
 * Foundation, Inc., 59 Temple Place - 3B3BSuite 330, Boston, MA 02111-1307, USA.
 *
 *
 * @author Elvin Sindrilaru & Andreas Peters - CERN
 * 
 ******************************************************************************/

#ifndef __XCASTOR2_PROC__
#define __XCASTOR2_PROC__

/*-----------------------------------------------------------------------------*/
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <dirent.h>
/*-----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucString.hh"
#include "XrdOuc/XrdOucHash.hh"
/*-----------------------------------------------------------------------------*/


//------------------------------------------------------------------------------
// Class XrdxCastor2ProcFile
//------------------------------------------------------------------------------
class XrdxCastor2ProcFile
{
  public:

    //----------------------------------------------------------------------------
    //! Constructor
    //!
    //! @param name name of the file
    //! @param sycnit mark if do sync
    //!
    //----------------------------------------------------------------------------
    XrdxCastor2ProcFile( const char* name, bool syncit = false );


    //----------------------------------------------------------------------------
    //! Destructor
    //----------------------------------------------------------------------------
    virtual ~XrdxCastor2ProcFile();


    //----------------------------------------------------------------------------
    //! Open file
    //----------------------------------------------------------------------------
    bool Open();


    //----------------------------------------------------------------------------
    //! Close file
    //----------------------------------------------------------------------------
    bool Close();


    //----------------------------------------------------------------------------
    //! Write long long to file
    //----------------------------------------------------------------------------
    bool Write( long long val, int writedelay = 0 );


    //----------------------------------------------------------------------------
    //! Write double to file
    //----------------------------------------------------------------------------
    bool Write( double val, int writedelay = 0 );


    //----------------------------------------------------------------------------
    //! Write string to file
    //----------------------------------------------------------------------------
    bool Write( const char* str, int writedelay = 0 );


    //----------------------------------------------------------------------------
    //! Write key-value pair to file
    //----------------------------------------------------------------------------
    bool WriteKeyVal( const char*        key,
                      unsigned long long value,
                      int                writedelay,
                      bool               truncate = 0 );


    //----------------------------------------------------------------------------
    //! Read and convert value to long long
    //----------------------------------------------------------------------------
    long long Read();


    //----------------------------------------------------------------------------
    //! Read into string
    //----------------------------------------------------------------------------
    bool Read( XrdOucString& str );

  private:

    int fd;             ///< file descriptor value
    XrdOucString fname; ///< file name
    bool procsync;      ///< mark if do sync
    time_t lastwrite;   ///< last write offset value
};


//------------------------------------------------------------------------------
// Class XrdxCastor2Proc
//------------------------------------------------------------------------------
class XrdxCastor2Proc
{
  public:


    //----------------------------------------------------------------------------
    //! Constructor
    //!
    //! @param procdir proc directory name
    //! @param syncit if true sync, else don't
    //!
    //----------------------------------------------------------------------------
    XrdxCastor2Proc( const char* procdir, bool syncit );

    //----------------------------------------------------------------------------
    //! Destructor
    //----------------------------------------------------------------------------
    virtual ~XrdxCastor2Proc() {};

    //----------------------------------------------------------------------------
    //! Create proc directory
    //!
    //! @return true if successful, otherwise false
    //!
    //----------------------------------------------------------------------------
    bool Open();

    //----------------------------------------------------------------------------
    //! Get handle to a file
    //!
    //! @param name name of the file
    //!
    //! @return proc file pointer, or NULL
    //
    //----------------------------------------------------------------------------
    XrdxCastor2ProcFile* Handle( const char* name );

  private:

    bool procsync;                         ///< mark if do sync
    XrdOucString procdirectory;            ///< proc directory name
    XrdOucHash<XrdxCastor2ProcFile> files; ///< hash containing the files in the dir
};

#endif // __XCASTOR2_PROC__

