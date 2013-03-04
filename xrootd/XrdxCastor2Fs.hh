/*******************************************************************************
 *                      XrdxCastor2Fs.hh
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

#ifndef __XCASTOR_FS_HH__
#define __XCASTOR_FS_HH__

/*-----------------------------------------------------------------------------*/
#include <sys/types.h>
#include <unistd.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <memory.h>
#include <string.h>
#include <stdio.h>
#include <iostream>
#include <stdlib.h>
#include <sys/param.h>
#include <sys/stat.h>
#include <attr/xattr.h>
#include <utime.h>
#include <pwd.h>
/*-----------------------------------------------------------------------------*/
#include "serrno.h"
#include "Cns_api.h"
/*-----------------------------------------------------------------------------*/
#include "XrdAcc/XrdAccAuthorize.hh"
#include "XrdSfs/XrdSfsInterface.hh"
#include "XrdOuc/XrdOucTrace.hh"
#include "XrdOfs/XrdOfsEvr.hh"
#include "XrdSec/XrdSecEntity.hh"
/*-----------------------------------------------------------------------------*/
#include "XrdxCastorLogging.hh"
#include "XrdxCastor2FsStats.hh"
#include "XrdxCastor2ServerAcc.hh"
#include "XrdxCastor2Proc.hh"
/*-----------------------------------------------------------------------------*/
#define RFIO_NOREDEFINE

//------------------------------------------------------------------------------
//! Get authorization plugin object 
//------------------------------------------------------------------------------
extern "C" XrdAccAuthorize* XrdAccAuthorizeObject( XrdSysLogger* lp,
                                                   const char*   cfn,
                                                   const char*   parm );

//! Forward declarations
class XrdSysError;
class XrdSysLogger;
class XrdSfsAio;

/******************************************************************************/
/*                    X r d x C a s t o r 2 F s U F S                         */
/******************************************************************************/

//------------------------------------------------------------------------------
//! Class XrdxCastor2UFS - Unix File System Interface - this is the interface
//! with the Castor name space
//------------------------------------------------------------------------------
class XrdxCastor2FsUFS
{
  public:

    static int Chmod( const char* fn, mode_t mode ) {
      return Cns_chmod( fn, mode );
    }

    static int Chown( const char* fn, uid_t owner, gid_t group ) {
      return Cns_chown( fn, owner, group );
    }

    static int Lchown( const char* fn, uid_t owner, gid_t group ) {
      return Cns_lchown( fn, owner, group );
    }

    static int Close( int fd ) {
      return close( fd );
    }

    static int Mkdir( const char* fn, mode_t mode ) {
      return Cns_mkdir( fn, mode );
    }

    static int Open( const char* path, int oflag, mode_t omode ) {
      return open( path, oflag, omode );
    }

    static int Creat( const char* path, mode_t omode ) {
      return Cns_creat( path, omode );
    }

    static int Rem( const char* fn ) {
      return Cns_unlink( fn );
    }

    static int Remdir( const char* fn ) {
      return Cns_rmdir( fn );
    }

    static int Rename( const char* ofn, const char* nfn ) {
      return Cns_rename( ofn, nfn );
    }

    static int SetFileSize( const char* fn, unsigned long long size, time_t mtime ) {
      return Cns_setfsize( fn, NULL, size, mtime, 0 );
    }

    static int Statfd( int fd, struct stat* buf ) {
      return fstat( fd, buf );
    }

    static int Statfn( const char* fn, struct Cns_filestatcs* buf ) {
      return Cns_statcs( fn, buf );
    }

    static int Lstatfn( const char* fn, struct Cns_filestat* buf ) {
      return Cns_lstat( fn, buf );
    }

    static int Readlink( const char* fn, char* buf, size_t bufsize ) {
      return Cns_readlink( fn, buf, bufsize );
    }

    static int SetId( uid_t uid, gid_t gid ) {
      return Cns_setid( uid, gid );
    }

    static int Symlink( const char* oldn, const char* newn ) {
      return Cns_symlink( oldn, newn );
    }

    static int Unlink( const char* fn ) {
      return Cns_unlink( fn );
    }

    static int Access( const char* fn, int mode ) {
      return Cns_access( fn, mode );
    }

    static int Utimes( const char* /*fn*/, struct timeval* /*tvp*/ ) {
      return 0; /* return Cns_utimes(fn,tvp);*/
    }

    static DIR* OpenDir( const char* dn ) {
      return ( DIR* )Cns_opendir( dn );
    }
    static struct dirent*  ReadDir( DIR* dp ) {
      return Cns_readdir( ( Cns_DIR* ) dp );
    }
    static int  CloseDir( DIR* dp ) {
      return Cns_closedir( ( Cns_DIR* ) dp );
    }
};


/******************************************************************************/
/*               X r d x C a s t o r 2 F s G r o u p I n f o                  */
/******************************************************************************/

//------------------------------------------------------------------------------
//! Class XrdxCastor2FsGroupInfo
//------------------------------------------------------------------------------
class XrdxCastor2FsGroupInfo
{
  public:

  XrdOucString DefaultGroup; ///<
    XrdOucString AllGroups;  ///<
    struct passwd Passwd;    ///<
    int Lifetime;            ///<


    //--------------------------------------------------------------------------
    //! Constructor
    //--------------------------------------------------------------------------
    XrdxCastor2FsGroupInfo( const char*    defgroup = "", 
                            const char*    allgrps = "", 
                            struct passwd* pw = 0, 
                            int            lf = 60 ) {
      DefaultGroup = defgroup;
      AllGroups = allgrps;

      if ( pw ) memcpy( &Passwd, pw, sizeof( struct passwd ) );

      Lifetime = lf;
    }

    //--------------------------------------------------------------------------
    //! Destructor
    //--------------------------------------------------------------------------
    virtual ~XrdxCastor2FsGroupInfo() {}
};


/******************************************************************************/
/*              X r d x C a s t o r 2 F s D i r e c t o r y                   */
/******************************************************************************/

//------------------------------------------------------------------------------
//! Class XrdxCastor2FsDirectory
//------------------------------------------------------------------------------
class XrdxCastor2FsDirectory : public XrdSfsDirectory, public LogId
{
  public:

    //--------------------------------------------------------------------------
    //! Constructor
    //--------------------------------------------------------------------------
    XrdxCastor2FsDirectory( char* user = 0, int MonID = 0 );


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
              const XrdSecClientName*  client = 0,
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


/******************************************************************************/
/*                  X r d x C a s t o r 2 F s F i l e                         */
/******************************************************************************/

//------------------------------------------------------------------------------
//! Class XrdxCastor2FsFile
//------------------------------------------------------------------------------
class XrdxCastor2FsFile : public XrdSfsFile, public LogId
{
  public:

    //--------------------------------------------------------------------------
    //! Constructor
    //--------------------------------------------------------------------------
    XrdxCastor2FsFile( char* user = 0, int MonID = 0 );


    //--------------------------------------------------------------------------
    //! Destructor
    //--------------------------------------------------------------------------
    ~XrdxCastor2FsFile(); 

    //--------------------------------------------------------------------------
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
    //!
    //--------------------------------------------------------------------------
    int open( const char*         fileName,
              XrdSfsFileOpenMode  openMode,
              mode_t              createMode,
              const XrdSecEntity* client = 0,
              const char*         opaque = 0 );


    //--------------------------------------------------------------------------
    //! Close the file object
    //!
    //! @return SFS_OK upon success and SFS_ERROR upon failure
    //!
    //--------------------------------------------------------------------------
    int close();


    //--------------------------------------------------------------------------
    //! Get file name
    //--------------------------------------------------------------------------
    const char* FName() {
      return fname;
    }


    //--------------------------------------------------------------------------
    //!
    //--------------------------------------------------------------------------
    int Fscmd( const char*         /*path*/,
               const char*         /*path2*/,
               const char*         /*orgipath*/,
               const XrdSecEntity* /*client*/,
               XrdOucErrInfo&      /*error*/,
               const char*         /*info*/ ) {
      return SFS_OK;
    }


    //--------------------------------------------------------------------------
    //!
    //--------------------------------------------------------------------------
    int getMmap( void** Addr, off_t& Size ) {
      if ( Addr ) Addr = 0;
      Size = 0;
      return SFS_OK;
    }


    //--------------------------------------------------------------------------
    //! Preread from file - disabled
    //--------------------------------------------------------------------------
    int read( XrdSfsFileOffset /*fileOffset*/,
              XrdSfsXferSize   /*preread_sz*/ ) {
      return SFS_OK;
    }

    //--------------------------------------------------------------------------
    //! Read `blen' bytes at `offset' into 'buff' and return the actual
    //!        number of bytes read.
    //!
    //! @param offset absolute byte offset at which to start the read
    //! @param buff address of the buffer in which to place the data
    //! @param blen size of the buffer. This is the maximum number
    //!             of bytes that will be read from 'fd'
    //!
    //! @return number of bytes read upon success and SFS_ERROR o/w
    //!
    //--------------------------------------------------------------------------
    XrdSfsXferSize read( XrdSfsFileOffset offset,
                         char*            buff,
                         XrdSfsXferSize   blen );


    //--------------------------------------------------------------------------
    //! Read from file asynchronous
    //--------------------------------------------------------------------------
    int read( XrdSfsAio* aioparm );


    //--------------------------------------------------------------------------
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
    //!
    //--------------------------------------------------------------------------
    XrdSfsXferSize write( XrdSfsFileOffset offset,
                          const char*      buff,
                          XrdSfsXferSize   blen );


    //--------------------------------------------------------------------------
    //! Write to file - asynchrnous
    //--------------------------------------------------------------------------
    int write( XrdSfsAio* aioparm );


    //--------------------------------------------------------------------------
    //! Commit all unwritten bytes to physical media
    //!
    //! @return SFS_OK upon success and SFS_ERROR upon failure
    //!
    //--------------------------------------------------------------------------
    int sync();


    //--------------------------------------------------------------------------
    //! Sync file - async mode
    //--------------------------------------------------------------------------
    int sync( XrdSfsAio* aiop );


    //--------------------------------------------------------------------------
    //! Function: Return file status information
    //!
    //! @param buf stat structure to hold the results
    //!
    //! @return SFS_OK upon success and SFS_ERROR upon failure
    //!
    //--------------------------------------------------------------------------
    int stat( struct stat* buf );


    //--------------------------------------------------------------------------
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
    //!
    //--------------------------------------------------------------------------
    int truncate( XrdSfsFileOffset   fileOffset );


    //--------------------------------------------------------------------------
    //!
    //--------------------------------------------------------------------------
    int getCXinfo( char* /*cxtype[4]*/, int& cxrsz ) {
      return cxrsz = 0;
    }


    //--------------------------------------------------------------------------
    //!
    //--------------------------------------------------------------------------
    int fctl( int, const char*, XrdOucErrInfo& ) {
      return 0;
    }

  private:

    int   oh;
    FILE* ohp;             ///< file pointer to fs command pipe
    FILE* ohperror;        ///< file pointer to fs command stderr file
    int   ohperror_cnt;    ///< cnt to find stderr file
    XrdSysMutex fscmdLock; ///<
    char* fname;           ///<
};


/******************************************************************************/
/*                           X r d x C a s t o r 2 F s                        */
/******************************************************************************/

//------------------------------------------------------------------------------
//! Class XrdxCastor2Fs
//------------------------------------------------------------------------------
class XrdxCastor2Fs : public XrdSfsFileSystem, public LogId
{
    friend class XrdxCastor2FsFile;
    friend class XrdxCastor2FsDirectory;
    friend class XrdxCastor2FsStats;

  public:

    enum eFSCTL { kFsctlxCastor2FsOffset = 40000, kFsctlSetMeta = 40001 };

    //--------------------------------------------------------------------------
    //! Constructor
    //--------------------------------------------------------------------------
    XrdxCastor2Fs( XrdSysError* lp );


    //--------------------------------------------------------------------------
    //! Destructor
    //--------------------------------------------------------------------------
    virtual ~XrdxCastor2Fs() {}


    //--------------------------------------------------------------------------
    //! Configure plugin
    //--------------------------------------------------------------------------
    virtual int Configure( XrdSysError& );


    //--------------------------------------------------------------------------
    //! Initialisation
    //--------------------------------------------------------------------------
    virtual bool Init();


    //--------------------------------------------------------------------------
    //! Create new directory
    //--------------------------------------------------------------------------
    XrdSfsDirectory* newDir( char* user = 0, int MonID = 0 ) {
      return ( XrdSfsDirectory* )new XrdxCastor2FsDirectory( user, MonID );
    }


    //--------------------------------------------------------------------------
    //! Create new file
    //--------------------------------------------------------------------------
    XrdSfsFile* newFile( char* user = 0, int MonID = 0 ) {
      return ( XrdSfsFile* )new XrdxCastor2FsFile( user, MonID );
    }


    //--------------------------------------------------------------------------
    //! Change the mode on a file or directory
    //!
    //! @param path fully qualified name of the file tob created
    //! @param Mode POSIX mode
    //! @param out_error error information object to hold error details
    //! @param client authentication credentials, if any
    //! @param info opaque information, if any
    //!
    //! @return SFS_OK upon success and SFS_ERROR upon failure
    //!
    //--------------------------------------------------------------------------
    int chmod( const char*         Name,
               XrdSfsMode          Mode,
               XrdOucErrInfo&      out_error,
               const XrdSecEntity* client = 0,
               const char*         opaque = 0 );


    //--------------------------------------------------------------------------
    //! Determine if file 'path' actually exists
    //!
    //! @param path fully qualified name of the file to be tested
    //! @param file_exists address of the variable to hold the status of
    //!             'path' when success is returned. The values may be:
    //!              XrdSfsFileExistsIsDirectory - file not found but path is valid
    //!              XrdSfsFileExistsIsFile      - file found
    //!              XrdSfsFileExistsIsNo        - neither file nor directory
    //! @param error error information object holding the details
    //! @param client authentication credentials, if any
    //! @param info opaque information, if any
    //!
    //! @return SFS_OK upon success and SFS_ERROR upon failure
    //! Notes:    When failure occurs, 'file_exists' is not modified.
    //!
    //--------------------------------------------------------------------------
    int exists( const char*          path,
                XrdSfsFileExistence& exists_flag,
                XrdOucErrInfo&       error,
                const XrdSecEntity*  client = 0,
                const char*          info = 0 );


    //--------------------------------------------------------------------------
    //! Determine if file 'path' actually exists - low level
    //--------------------------------------------------------------------------
    int _exists( const char*          path,
                 XrdSfsFileExistence& exists_flag,
                 XrdOucErrInfo&       error,
                 const XrdSecEntity*  client = 0,
                 const char*          info = 0 );


    //--------------------------------------------------------------------------
    //!
    //--------------------------------------------------------------------------
    int fsctl( const int               cmd,
               const char*             args,
               XrdOucErrInfo&    out_error,
               const XrdSecEntity* client = 0 );


    //--------------------------------------------------------------------------
    //!
    //--------------------------------------------------------------------------
    int getStats( char* /*buff*/, int /*blen*/ ) {
      return 0;
    }


    //--------------------------------------------------------------------------
    //! Get version of the plugin
    //--------------------------------------------------------------------------
    const char* getVersion();


    //--------------------------------------------------------------------------
    //! Create a directory entry
    //!
    //! @param path fully qualified name of the file tob created
    //! @param Mode POSIX mode setting for the directory. If the
    //!        mode contains SFS_O_MKPTH, the full path is created
    //! @param out_error error information object to hold error details
    //! @param client authentication credentials, if any
    //! @param info opaque information, if any
    //!
    //! @return SFS_OK upon success and SFS_ERROR upon failure
    //!
    //--------------------------------------------------------------------------
    int mkdir( const char*             dirName,
               XrdSfsMode              Mode,
               XrdOucErrInfo&          out_error,
               const XrdSecClientName* client = 0,
               const char*             opaque = 0 );


    //--------------------------------------------------------------------------
    //! Create a directory entry - same as above
    //--------------------------------------------------------------------------
    int _mkdir( const char*             dirName,
                XrdSfsMode              Mode,
                XrdOucErrInfo&          out_error,
                const XrdSecClientName* client = 0,
                const char*             opaque = 0 );


    //--------------------------------------------------------------------------
    //!
    //--------------------------------------------------------------------------
    int stageprepare( const char*         path,
                      XrdOucErrInfo&      error,
                      const XrdSecEntity* client,
                      const char*         info );


    //--------------------------------------------------------------------------
    //!
    //--------------------------------------------------------------------------
    int prepare( XrdSfsPrep&         pargs,
                 XrdOucErrInfo&      out_error,
                 const XrdSecEntity* client = 0 );


    //--------------------------------------------------------------------------
    //! Delete a file - stager_rm
    //!
    //! @param path fully qualified name of the file to be removed
    //! @param einfo error information object to hold error details
    //! @param client authentication credentials, if any
    //! @param info opaque information, if any
    //!
    //! @return SFS_OK upon success and SFS_ERROR upon failure
    //! 
    //--------------------------------------------------------------------------
    int rem( const char*         path,
             XrdOucErrInfo&      out_error,
             const XrdSecEntity* client = 0,
             const char*         opaque = 0 );


    //--------------------------------------------------------------------------
    //! Delete a file from the namespace
    //!
    //! @param path fully qualified name of the file to be removed
    //! @param einfo error information object to hold error details
    //! @param client authentication credentials, if any
    //! @param info opaque information, if any
    //!
    //! @return SFS_OK upon success and SFS_ERROR upon failure
    //! 
    //--------------------------------------------------------------------------
    int _rem( const char*         path,
              XrdOucErrInfo&      out_error,
              const XrdSecEntity* client = 0,
              const char*         opaque = 0 );


    //--------------------------------------------------------------------------
    //! Delete a directory from the namespace
    //!
    //! @param fully qualified name of the dir to be removed
    //! @param einfo error information object to hold error details
    //! @param client authentication credentials, if any
    //! @param info opaque information, if any
    //!
    //! @return SFS_OK upon success and SFS_ERROR upon failure
    //!
    //--------------------------------------------------------------------------
    int remdir( const char*         path,
                XrdOucErrInfo&      out_error,
                const XrdSecEntity* client = 0,
                const char*         info = 0 );


    //--------------------------------------------------------------------------
    //! Delete a directory from the namespace
    //!
    //! @param fully qualified name of the dir to be removed
    //! @param einfo error information object to hold error details
    //! @param client authentication credentials, if any
    //! @param info opaque information, if any
    //!
    //! @return SFS_OK upon success and SFS_ERROR upon failure
    //!
    //--------------------------------------------------------------------------
    int _remdir( const char*         path,
                 XrdOucErrInfo&      out_error,
                 const XrdSecEntity* client = 0,
                 const char*         opaque = 0 );


    //--------------------------------------------------------------------------
    //! Renames a file/directory with name 'old_name' to 'new_name'
    //!
    //! @param old_name fully qualified name of the file to be renamed
    //! @param new_name fully qualified name that the file is to have
    //! @param error error information structure, if an error occurs
    //! @param client authentication credentials, if any
    //! @param infoO old_name opaque information, if any
    //! @param infoN new_name opaque information, if any
    //!
    //! @return SFS_OK upon success and SFS_ERROR upon failure
    //!
    //--------------------------------------------------------------------------
    int rename( const char*         old_name,
                const char*         new_name,
                XrdOucErrInfo&      error,
                const XrdSecEntity* client = 0,
                const char*         infoO = 0,
                const char*         infoN = 0 );


    //--------------------------------------------------------------------------
    //!  Get info on 'path' - all
    //!
    //! @param path fully qualified name of the file to be tested
    //! @param buf stat structure to hold the results
    //! @param error error information object holding the details
    //! @param client authentication credentials, if any
    //! @param info opaque information, if any
    //!
    //! @return SFS_OK upon success and SFS_ERROR upon failure
    //!
    //--------------------------------------------------------------------------
    int stat( const char*         path,
              struct stat*        buf,
              XrdOucErrInfo&      error,
              const XrdSecEntity* client = 0,
              const char*         info = 0 );


    //--------------------------------------------------------------------------
    //!  Get info on 'path' - mode
    //!
    //! @param path fully qualified name of the file to be tested
    //! @param buf stat structure to hold the results
    //! @param error error information object holding the details
    //! @param client authentication credentials, if any
    //! @param info opaque information, if any
    //!
    //! @return SFS_OK upon success and SFS_ERROR upon failure
    //!
    //--------------------------------------------------------------------------
    int stat( const char*          Name,
              mode_t&              mode,
              XrdOucErrInfo&       out_error,
              const XrdSecEntity*  client = 0,
              const char*          opaque = 0 );
  

    //--------------------------------------------------------------------------
    //! Get info on 'path'
    //!
    //! @param path fully qualified name of the file to be tested
    //! @param buf stat structiure to hold the results
    //! @param error error information object holding the details
    //! @param client authentication credentials, if any
    //! @param info opaque information, if any
    //!
    //! @return SFS_OK upon success and SFS_ERROR upon failure
    //!
    //--------------------------------------------------------------------------
    int lstat( const char*         path,
               struct stat*        buf,
               XrdOucErrInfo&      error,
               const XrdSecEntity* client = 0,
               const char*         info = 0 );


    //--------------------------------------------------------------------------
    //! Truncate 
    //--------------------------------------------------------------------------
    int truncate( const char*, XrdSfsFileOffset, XrdOucErrInfo&, 
                  const XrdSecEntity*, const char* );

    //--------------------------------------------------------------------------
    //! Symbolic link
    //--------------------------------------------------------------------------
    int symlink( const char*, const char*, XrdOucErrInfo&, 
                 const XrdSecEntity*, const char* );

    //--------------------------------------------------------------------------
    //! Read link
    //--------------------------------------------------------------------------
    int readlink( const char*, XrdOucString& ,  XrdOucErrInfo&, 
                  const XrdSecEntity*, const char* );

    //--------------------------------------------------------------------------
    //! Access
    //--------------------------------------------------------------------------
    int access( const char*, int mode, XrdOucErrInfo&, const XrdSecEntity*, 
                const char* );

    //--------------------------------------------------------------------------
    //! Utimes
    //--------------------------------------------------------------------------
    int utimes( const char*, struct timeval* tvp, XrdOucErrInfo&, 
                const XrdSecEntity*, const char* );


   //---------------------------------------------------------------------------
   //! Create a directory path
   //!
   //! @param path fully qualified name of the new path.
   //! @param mode new mode that each new directory is to have.
   //! @param info opaque information, if any
   //!
   //! @return 0 upon success and -errno upon failure
   //!
   //---------------------------------------------------------------------------
   static int Mkpath( const char*    path, 
                      mode_t         mode,
                      const char*    info = 0, 
                      XrdSecEntity*  client = NULL, 
                      XrdOucErrInfo* error = NULL );

    //--------------------------------------------------------------------------
    //! Compose error message
    //!
    //! @param pfx message prefix value
    //! @param einfo place to put text and error code 
    //! @param ecode the error code 
    //! @param op operation beegin performed
    //! @param tager the taget (e.g. file name )
    //!
    //--------------------------------------------------------------------------
    static int Emsg( const char*    pfx, 
                     XrdOucErrInfo& einfo, 
                     int            ecode , 
                     const char*    op,
                     const char*    tagert = "" );


    //--------------------------------------------------------------------------
    //! Stall message
    //!
    //! @param error error text and code 
    //! @param stime seconds to stall 
    //! @param msg message to give
    //!
    //! @return number of seconds to stall
    //!
    //--------------------------------------------------------------------------
    int Stall( XrdOucErrInfo& error, int stime, const char* msg );


    //--------------------------------------------------------------------------
    //! Get stage variables
    //--------------------------------------------------------------------------
    bool GetStageVariables( const char*   Path, 
                            const char*   Opaque, 
                            XrdOucString& stagevariables, 
                            uid_t         client_uid, 
                            gid_t         client_gid, 
                            const char*   tident );


    //--------------------------------------------------------------------------
    //! Set stage variables
    //--------------------------------------------------------------------------
    int SetStageVariables( const char*   Path, 
                           const char*   Opaque, 
                           XrdOucString  stagevariables, 
                           XrdOucString& stagehost, 
                           XrdOucString& serviceclass, 
                           int           n, 
                           const char*   tident );


    //--------------------------------------------------------------------------
    //! Map input path according to policy
    //!
    //! @param input the initial input path
    //!
    //! @return the mapped path according to the directives in the xrd.cf file
    //!
    //--------------------------------------------------------------------------
    static XrdOucString NsMapping( XrdOucString input );


    //--------------------------------------------------------------------------
    //! Reload grid map file
    //--------------------------------------------------------------------------
    void ReloadGridMapFile();


    //--------------------------------------------------------------------------
    //! Reload VOMS map file 
    //--------------------------------------------------------------------------
    void ReloadVomsMapFile();


    //--------------------------------------------------------------------------
    //! VOMS map groups
    //--------------------------------------------------------------------------
    bool VomsMapGroups( const XrdSecEntity* client, 
                        XrdSecEntity&       mappedclient, 
                        XrdOucString&       allgroups, 
                        XrdOucString&       defaultgroup );

    bool MapCernCertificates;
    XrdOucString GridMapFile;
    XrdOucString VomsMapFile;
    XrdOucString LocationCacheDir;
    char* ConfigFN;  ///< path to config file 

    // we must check if all these things need to be thread safed with a mutex ...
    static  XrdOucHash<XrdOucString>* filesystemhosttable;
    static  XrdSysMutex               filesystemhosttablelock;
    static  XrdOucHash<XrdOucString>* nsMap;
    static  XrdOucHash<XrdOucString>* stagertable;
    static  XrdOucHash<XrdOucString>* stagerpolicy;
    static  XrdOucHash<XrdOucString>* roletable;
    static  XrdOucHash<XrdOucString>* stringstore;
    static  XrdOucHash<XrdSecEntity>* secentitystore;
    static  XrdOucHash<struct passwd>* passwdstore;
    static  XrdOucHash<XrdOucString>*  gridmapstore;
    static  XrdOucHash<XrdOucString>*  vomsmapstore;
    static  XrdOucHash<XrdxCastor2FsGroupInfo>*  groupinfocache;

    static  int TokenLockTime;  ///< specifies the grace period for client to show 
                                ///< up on a disk server in seconds before the token expires

    XrdxCastor2ServerAcc* ServerAcc;     ///< authorization module for token encryption/decryption
    XrdSysMutex encodeLock;              ///<
    XrdSysMutex locatorLock;             ///< 
    XrdSysMutex PermissionMutex;         ///< protecting the permission hash
    XrdSysMutex StoreMutex;              ///< protecting the string+pwd store hash
    XrdSysMutex MapMutex;                ///< protecting all ROLEMAP, GETID etc. functions
    XrdSysMutex SecEntityMutex;          ///< protecting the sec entity hash
    XrdSysMutex GridMapMutex;            ///< protecting the gridmap store hash
    XrdSysMutex VomsMapMutex;            ///< protecting the vomsmap store hash
    XrdOucString xCastor2FsName;         ///< mount point of the catalog fs
    XrdOucString xCastor2FsTargetPort;   ///< xrootd port where redirections go on the OSTs -default is 1094
    XrdOucString xCastor2FsLocatePolicy; ///< can be configured to read only from the primary FS, from the twin FS or balanced between both

    long long xCastor2FsDelayRead;   ///< if true, all reads get a default delay to come back later
    long long xCastor2FsDelayWrite;  ///< if true, all writes get a default dealy to come back later
    XrdOucString Procfilesystem;     ///< location of the proc file system directory
    bool ProcfilesystemSync;         ///< sync every access on disk
    XrdOucString DeletionDirectory;  ///< directory where deletion spool files are stored
    XrdOucString zeroProc;           ///< path to a 0-byte file in the proc filesystem

    XrdOucString AuthLib;            ///< path to a possible authorizationn library
    bool authorize;                  ///< determins if the autorization should be applied or not
    XrdAccAuthorize* Authorization;  ///< authorization service

  protected:

    char*        HostName;        ///< our hostname as derived in XrdOfs
    char*        HostPref;        ///< our hostname as derived in XrdOfs without domain
    XrdOucString ManagerId;       ///< manager id in <host>:<port> format
    XrdOucString ManagerLocation; ///< [::<IP>]:<port>
    bool         IssueCapability; ///< attach an opaque capability for verification on the disk server
    XrdxCastor2Proc* Proc;        ///< proc handling object
    XrdxCastor2FsStats Stats;     ///<
    XrdOfsEvr evrObject;          ///< Event receiver

  private:

    int mLogLevel; ///< log level from config file
    static  XrdSysError* eDest;
};

#endif // __XCASTOR_FS_HH__
