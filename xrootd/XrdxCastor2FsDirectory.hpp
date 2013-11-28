/*******************************************************************************
 *                      XrdxCastor2FsDirectory.hpp
 *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2013  CERN
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
 * @author Andreas Peters <apeters@cern.ch>
 * @author Elvin Sindrilaru <esindril@cern.ch>
 *
 ******************************************************************************/

#ifndef __XCASTOR_FSDIRECTORY_HH__
#define __XCASTOR_FSDIRECTORY_HH__

/*-----------------------------------------------------------------------------*/
#include "XrdSfs/XrdSfsInterface.hh"
#include "XrdxCastorLogging.hpp"
/*-----------------------------------------------------------------------------*/
#include <dirent.h>
/*-----------------------------------------------------------------------------*/

//------------------------------------------------------------------------------
//! Class XrdxCastor2FsDirectory
//------------------------------------------------------------------------------
class XrdxCastor2FsDirectory : public XrdSfsDirectory, public LogId
{
  public:

    //--------------------------------------------------------------------------
    //! Constructor
    //--------------------------------------------------------------------------
    XrdxCastor2FsDirectory(const char* user=0, int MonID=0);


    //--------------------------------------------------------------------------
    //! Destructor
    //--------------------------------------------------------------------------
    virtual ~XrdxCastor2FsDirectory();


    //--------------------------------------------------------------------------
    //! Open the directory `path' and prepare for reading
    //!
    //! @param path fully qualified name of the directory to open.
    //! @param cred authentication credentials, if any
    //! @param info opaque information, if any
    //!
    //! @return SFS_OK upon success, otherwise SFS_ERROR
    //!
    //--------------------------------------------------------------------------
    int open( const char*              dirName,
              const XrdSecEntity*  client = 0,
              const char*              opaque = 0 );


    //--------------------------------------------------------------------------
    //! Read the next directory entry
    //!
    //! @return Upon success, returns the contents of the next directory entry as
    //!         a null terminated string. Returns a null pointer upon EOF or an
    //!         error. To differentiate the two cases, getErrorInfo will return
    //!         0 upon EOF and an actual error code (i.e., not 0) on error.
    //!
    //--------------------------------------------------------------------------
    const char* nextEntry();


    //--------------------------------------------------------------------------
    //! Close the directory object
    //!
    //! @param cred authentication credentials, if any
    //!
    //! @return SFS_OK upon success and SFS_ERROR upon failure
    //!
    //--------------------------------------------------------------------------
    int close();


    //--------------------------------------------------------------------------
    //! Get directory name 
    //--------------------------------------------------------------------------
    const char* FName() {
      return ( const char* )fname;
    }

  private:

    DIR*           dh;    ///< directory stream handle
    char           ateof; ///< mark the end of dir entries
    char*          fname; ///< directory name 
    XrdOucString   entry; ///<

    struct {
      struct dirent d_entry; ///<
      char   pad[MAXNAMLEN]; ///< This is only required for Solaris!
    } dirent_full;

    struct dirent* d_pnt; ///<

};

#endif // __XCASTOR_FSDIRECTORY_HH__
