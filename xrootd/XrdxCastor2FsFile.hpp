/*******************************************************************************
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
 * @author Castor Dev team, castor-dev@cern.ch
 *
 ******************************************************************************/

#pragma once

/*----------------------------------------------------------------------------*/
#include "XrdSfs/XrdSfsInterface.hh"
/*----------------------------------------------------------------------------*/
#include "XrdxCastorLogging.hpp"
/*----------------------------------------------------------------------------*/

//! Forward declaration
class XrdSfsAio;
class XrdSecEntity;


//------------------------------------------------------------------------------
//! Class XrdxCastor2FsFile
//------------------------------------------------------------------------------
class XrdxCastor2FsFile : public XrdSfsFile, public LogId
{
public:

  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  XrdxCastor2FsFile(const char* user = 0, int MonID = 0);


  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  ~XrdxCastor2FsFile();


  //----------------------------------------------------------------------------
  //! Open the file `path' in the mode indicated by `open_mode'
  //!
  //! @param path fully qualified name of the file to open.
  //! @param open_mode one of the following flag values:
  //!                    SFS_O_RDONLY - Open file for reading.
  //!                    SFS_O_WRONLY - Open file for writing.
  //!                    SFS_O_RDWR   - Open file for update
  //!                    SFS_O_CREAT  - Create the file open in RDWR mode
  //!                    SFS_O_TRUNC  - Trunc  the file open in RDWR mode
  //! @param Mode Posix access mode bits to be assigned to the file.
  //!              These bits correspond to the standard Unix permission
  //!              bits (e.g., 744 == "rwxr--r--"). Mode may also conatin
  //!              SFS_O_MKPTH is the full path is to be created. The
  //!              agument is ignored unless open_mode = SFS_O_CREAT.
  //! @param client authentication credentials, if any
  //! @param info opaque information to be used as seen fit
  //!
  //! @return OOSS_OK upon success, otherwise SFS_ERROR is returned
  //----------------------------------------------------------------------------
  int open(const char* fileName,
           XrdSfsFileOpenMode openMode,
           mode_t createMode,
           const XrdSecEntity* client = 0,
           const char* opaque = 0);


  //----------------------------------------------------------------------------
  //! Close the file object
  //!
  //! @return SFS_OK upon success and SFS_ERROR upon failure
  //----------------------------------------------------------------------------
  int close();


  //----------------------------------------------------------------------------
  //! Get file name
  //----------------------------------------------------------------------------
  const char* FName()
  {
    return fname;
  }


  //----------------------------------------------------------------------------
  //! XRootD interface
  //----------------------------------------------------------------------------
  int getMmap(void** Addr, off_t& Size)
  {
    if (Addr) Addr = 0;

    Size = 0;
    return SFS_OK;
  }


  //----------------------------------------------------------------------------
  //! Preread from file - disabled
  //----------------------------------------------------------------------------
  int read(XrdSfsFileOffset fileOffset,
           XrdSfsXferSize preread_sz)
  {
    return SFS_OK;
  }


  //----------------------------------------------------------------------------
  //! Read `blen' bytes at `offset' into 'buff' and return the actual
  //!        number of bytes read.
  //!
  //! @param offset absolute byte offset at which to start the read
  //! @param buff address of the buffer in which to place the data
  //! @param blen size of the buffer. This is the maximum number
  //!             of bytes that will be read from 'fd'
  //!
  //! @return number of bytes read upon success and SFS_ERROR o/w
  //----------------------------------------------------------------------------
  XrdSfsXferSize read(XrdSfsFileOffset offset,
                      char* buff,
                      XrdSfsXferSize blen);


  //----------------------------------------------------------------------------
  //! Read from file asynchronous
  //----------------------------------------------------------------------------
  int read(XrdSfsAio* aioparm);


  //----------------------------------------------------------------------------
  //! Write `blen' bytes at `offset' from 'buff' and return the actual
  //! number of bytes written.
  //!
  //! @param offset absolute byte offset at which to start the write.
  //! @param buff address of the buffer from which to get the data.
  //! @param blen size of the buffer. This is the maximum number
  //!             of bytes that will be written to 'fd'.
  //!
  //! @return  number of bytes written upon success and SFS_ERROR o/w.
  //!
  //!  Notes: An error return may be delayed until the next write(), close(),
  //!         or sync() call.
  //----------------------------------------------------------------------------
  XrdSfsXferSize write(XrdSfsFileOffset offset,
                       const char* buff,
                       XrdSfsXferSize blen);


  //----------------------------------------------------------------------------
  //! Write to file - asynchrnous
  //----------------------------------------------------------------------------
  int write(XrdSfsAio* aioparm);


  //----------------------------------------------------------------------------
  //! Commit all unwritten bytes to physical media
  //!
  //! @return SFS_OK upon success and SFS_ERROR upon failure
  //----------------------------------------------------------------------------
  int sync();


  //----------------------------------------------------------------------------
  //! Sync file - async mode
  //----------------------------------------------------------------------------
  int sync(XrdSfsAio* aiop);


  //----------------------------------------------------------------------------
  //! Function: Return file status information
  //!
  //! @param buf stat structure to hold the results
  //!
  //! @return SFS_OK upon success and SFS_ERROR upon failure
  //----------------------------------------------------------------------------
  int stat(struct stat* buf);


  //----------------------------------------------------------------------------
  //! Set the length of the file object to 'flen' bytes
  //!
  //! @param flen new size of the file.
  //!
  //! @return SFS_OK upon success and SFS_ERROR upon failure.
  //!
  //!  Notes: If 'flen' is smaller than the current size of the file, the file
  //!         is made smaller and the data past 'flen' is discarded. If 'flen'
  //!         is larger than the current size of the file, a hole is created
  //!         (i.e., the file is logically extended by filling the extra bytes
  //!         with zeroes).
  //----------------------------------------------------------------------------
  int truncate(XrdSfsFileOffset fileOffset);


  //----------------------------------------------------------------------------
  //!
  //----------------------------------------------------------------------------
  int getCXinfo(char* cxtype, int& cxrsz)
  {
    return cxrsz = 0;
  }


  //----------------------------------------------------------------------------
  //! Implementation specific command
  //----------------------------------------------------------------------------
  int fctl(int, const char*, XrdOucErrInfo&)
  {
    return 0;
  }


private:

  int oh; ///< file handler
  char* fname; ///< file name
};
