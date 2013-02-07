/*******************************************************************************
 *                      XrdxCastor2Ofs.hh
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

#ifndef __XCASTOR2OFS_H__
#define __XCASTOR2OFS_H__

/*-----------------------------------------------------------------------------*/
#include "pwd.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <zlib.h>
/*-----------------------------------------------------------------------------*/
#include "XrdNet/XrdNetSocket.hh"
#include "XrdOuc/XrdOucEnv.hh"
#include "XrdOuc/XrdOucString.hh"
#include "XrdOuc/XrdOucTrace.hh"
#include "XrdOfs/XrdOfs.hh"
#include "XrdSfs/XrdSfsInterface.hh"
#include "XrdSys/XrdSysTimer.hh"
#include "XrdSys/XrdSysPthread.hh"
#include "XrdClient/XrdClientAdmin.hh"
#include "XrdClient/XrdClient.hh"
#include "XrdxCastor2ClientAdmin.hh"
/*-----------------------------------------------------------------------------*/
#include "XrdTransferManager.hh"
/*-----------------------------------------------------------------------------*/


//------------------------------------------------------------------------------
//! Class XrdxCastor2Ofs2StagerJob
//------------------------------------------------------------------------------
class XrdxCastor2Ofs2StagerJob
{
  public:

    int  ErrCode;         ///< error code
    XrdOucString ErrMsg;  ///< error message

    //--------------------------------------------------------------------------
    //! Constructor
    //!
    //! @param sjobuuid stager job identifier
    //! @param port port
    //!
    //--------------------------------------------------------------------------
    XrdxCastor2Ofs2StagerJob( const char* sjobuuid, int port );


    //--------------------------------------------------------------------------
    //! Destructor
    //--------------------------------------------------------------------------
    ~XrdxCastor2Ofs2StagerJob();


    //--------------------------------------------------------------------------
    //! Open new connection to the StagerJob
    //--------------------------------------------------------------------------
    bool Open();


    //--------------------------------------------------------------------------
    //! Close connection
    //!
    //! @param ok ?
    //! @param update mark that writes were made
    //!
    //! @return true if succesful, otherwise false
    //!
    //--------------------------------------------------------------------------
    bool Close( bool ok, bool update );


    //--------------------------------------------------------------------------
    //! Test if connection still alive
    //--------------------------------------------------------------------------
    bool StillConnected();


    //--------------------------------------------------------------------------
    //! Get connection status
    //--------------------------------------------------------------------------
    inline bool Connected() {
      return IsConnected;
    }

  private:

    int Port;              ///<
    bool IsConnected;      ///<
    XrdOucString SjobUuid; ///<
    XrdNetSocket* Socket;  ///<
    struct timeval to;     ///<
    fd_set check_set;      ///<
};


//------------------------------------------------------------------------------
//! Class XrdxCastor2OfsFile
//------------------------------------------------------------------------------
class XrdxCastor2OfsFile : public XrdOfsFile
{
  public:

    enum eProcRequest {kProcNone, kProcRead, kProcWrite};


    //--------------------------------------------------------------------------
    //! Constuctor
    //--------------------------------------------------------------------------
    XrdxCastor2OfsFile( const char* user, int MonID = 0 );


    //--------------------------------------------------------------------------
    //! Destructor
    //--------------------------------------------------------------------------
    virtual ~XrdxCastor2OfsFile();


    //--------------------------------------------------------------------------
    //! Open file
    //--------------------------------------------------------------------------
    int open( const char*         fileName,
              XrdSfsFileOpenMode  openMode,
              mode_t              createMode,
              const XrdSecEntity* client,
              const char*         opaque = 0 );
  

    //--------------------------------------------------------------------------
    //! Close file
    //--------------------------------------------------------------------------
    int close();


    //--------------------------------------------------------------------------
    //! Read from file
    //--------------------------------------------------------------------------
    int read( XrdSfsFileOffset fileOffset,  // Preread only
              XrdSfsXferSize   amount );


    //--------------------------------------------------------------------------
    //! Read from file 
    //--------------------------------------------------------------------------
    XrdSfsXferSize read( XrdSfsFileOffset fileOffset,
                         char*            buffer,
                         XrdSfsXferSize   buffer_size );


    //--------------------------------------------------------------------------
    //! Read from file
    //--------------------------------------------------------------------------
    int read( XrdSfsAio* aioparm );


    //--------------------------------------------------------------------------
    //! Write to file 
    //--------------------------------------------------------------------------
    XrdSfsXferSize write( XrdSfsFileOffset fileOffset,
                          const char*      buffer,
                          XrdSfsXferSize   buffer_size );


    //--------------------------------------------------------------------------
    //! Write to file 
    //--------------------------------------------------------------------------
    int write( XrdSfsAio* aioparm );


    //--------------------------------------------------------------------------
    //! Sync file
    //--------------------------------------------------------------------------
    int sync();


    //--------------------------------------------------------------------------
    //! Sync file - async 
    //--------------------------------------------------------------------------
    int sync( XrdSfsAio* aiop );


    //--------------------------------------------------------------------------
    //! Truncate file 
    //--------------------------------------------------------------------------
    int truncate( XrdSfsFileOffset fileOffset );


    //--------------------------------------------------------------------------
    //! Update meta data - used to inform the manager node that the first byte 
    //! was written
    //--------------------------------------------------------------------------
    int UpdateMeta();


    //--------------------------------------------------------------------------
    //! Unlink file 
    //--------------------------------------------------------------------------
    int Unlink();


    //--------------------------------------------------------------------------
    //! Verify checksum 
    //--------------------------------------------------------------------------
    bool VerifyChecksum();


    //--------------------------------------------------------------------------
    //! Do a third party transfer
    //--------------------------------------------------------------------------
    int ThirdPartyTransfer( const char*         fileName,
                            XrdSfsFileOpenMode  openMode,
                            mode_t              createMode,
                            const XrdSecEntity* client,
                            const char*         opaque );
  
    //--------------------------------------------------------------------------
    //! Close third party transfer
    //--------------------------------------------------------------------------
    int ThirdPartyTransferClose( const char* uuidstring );


    XrdxCastor2Ofs2StagerJob* StagerJob; ///< interface to communicate with stagerjob
    XrdTransfer* Transfer;               ///<
    int          StagerJobPid;           ///< pid of an assigned stagerjob

    bool         IsAdminStream;          ///<
    bool         IsThirdPartyStream;     ///<
    bool         IsThirdPartyStreamCopy; ///<
    bool         IsClosed;               ///<
    time_t       LastMdsUpdate;          ///<

  private:
  
    int              procRequest;    ///<
    XrdOucEnv*       envOpaque;      ///<
    bool             isRW;           ///<
    bool             isTruncate;     ///<
    bool             hasWrite;       ///<
    bool             firstWrite;     ///<
    bool             isOpen;         ///<
    bool             viaDestructor;  ///<
    XrdOucString     reqid;          ///< 
    XrdOucString     stagehost;      ///<
    XrdOucString     serviceclass;   ///<
    int              stagerjobport;  ///<
    XrdOucString     SjobUuid;       ///<
    XrdOucString     castorlfn;      ///<
    unsigned int     adler;          ///<
    bool             hasadler;       ///<
    bool             hasadlererror;  ///<
    XrdSfsFileOffset adleroffset;    ///<
    struct stat      statinfo;       ///<
    XrdOucString     DiskChecksum;   ///<
    XrdOucString     DiskChecksumAlgorithm; ///<
    bool             verifyChecksum; ///<

};


//------------------------------------------------------------------------------
//! Class XrdxCastor2OfsDirectory
//------------------------------------------------------------------------------
class XrdxCastor2OfsDirectory : public XrdOfsDirectory
{
  public:

    //--------------------------------------------------------------------------
    //! Constructor
    //--------------------------------------------------------------------------
    XrdxCastor2OfsDirectory( const char* user, int MonID = 0 ) :
      XrdOfsDirectory( user, MonID )  {};

    //--------------------------------------------------------------------------
    //! Destructor
    //--------------------------------------------------------------------------
    virtual ~XrdxCastor2OfsDirectory() {};
};


//------------------------------------------------------------------------------
//! Class XrdxCastor2Ofs
//------------------------------------------------------------------------------
class XrdxCastor2Ofs : public XrdOfs
{
    friend class XrdxCastor2OfsDirectory;
    friend class XrdxCastor2OfsFile;
  
  public:

    //--------------------------------------------------------------------------
    //! Constructor
    //--------------------------------------------------------------------------
    XrdxCastor2Ofs() {};


    //--------------------------------------------------------------------------
    //! Destructor
    //--------------------------------------------------------------------------
    virtual ~XrdxCastor2Ofs() {};


    //--------------------------------------------------------------------------
    //! Configure function
    //--------------------------------------------------------------------------
    int Configure( XrdSysError& error );


    //--------------------------------------------------------------------------
    //! Create new directory object
    //--------------------------------------------------------------------------
    XrdSfsDirectory* newDir( char* user = 0, int MonID = 0 ) {
      return ( XrdSfsDirectory* ) new XrdxCastor2OfsDirectory( user, MonID );
    }


    //--------------------------------------------------------------------------
    //! Create new file object 
    //--------------------------------------------------------------------------
    XrdSfsFile* newFile( char* user = 0, int MonID = 0 ) {
      return ( XrdSfsFile* ) new XrdxCastor2OfsFile( user, MonID );
    };


    //--------------------------------------------------------------------------
    //!
    //--------------------------------------------------------------------------
    bool UpdateProc( const char* name );


    // *** Here we mask all illegal operations ***
    //--------------------------------------------------------------------------
    //!
    //--------------------------------------------------------------------------
    int chmod( const char*         /*Name*/,
               XrdSfsMode          /*Mode*/,
               XrdOucErrInfo&      /*out_error*/,
               const XrdSecEntity* /*client*/,
               const char*         /*opaque = 0*/ ) { return SFS_OK; }


    //--------------------------------------------------------------------------
    //!
    //--------------------------------------------------------------------------
    int exists( const char*          /*fileName*/,
                XrdSfsFileExistence& /*exists_flag*/,
                XrdOucErrInfo&       /*out_error*/,
                const XrdSecEntity*  /*client*/,
                const char*          /*opaque = 0*/ ) { return SFS_OK; }


    //--------------------------------------------------------------------------
    //!
    //--------------------------------------------------------------------------
    int fsctl( const int           /*cmd*/,
               const char*         /*args*/,
               XrdOucErrInfo&      /*out_error*/,
               const XrdSecEntity* /*client*/ ) { return SFS_OK; }


    //--------------------------------------------------------------------------
    //!
    //--------------------------------------------------------------------------
    int mkdir( const char*         /*dirName*/,
               XrdSfsMode          /*Mode*/,
               XrdOucErrInfo&      /*out_error*/,
               const XrdSecEntity* /*client*/,
               const char*         /*opaque = 0*/ ) { return SFS_OK; }


    //--------------------------------------------------------------------------
    //!
    //--------------------------------------------------------------------------
    int prepare( XrdSfsPrep&         /*pargs*/,
                 XrdOucErrInfo&      /*out_error*/,
                 const XrdSecEntity* /*client = 0*/ ) { return SFS_OK; }


    //--------------------------------------------------------------------------
    //!
    //--------------------------------------------------------------------------
    int remdir( const char*         /*dirName*/,
                XrdOucErrInfo&      /*out_error*/,
                const XrdSecEntity* /*client*/,
                const char*         /*info = 0*/ ) { return SFS_OK; }


    //--------------------------------------------------------------------------
    //!
    //--------------------------------------------------------------------------
    int rename( const char*         /*oldFileName*/,
                const char*         /*newFileName*/,
                XrdOucErrInfo&      /*out_error*/,
                const XrdSecEntity* /*client*/,
                const char*         /*infoO = 0*/,
                const char*         /*infoN = 0*/ ) { return SFS_OK; }


    //--------------------------------------------------------------------------
    //!
    //--------------------------------------------------------------------------
    int rem( const char*         path,
             XrdOucErrInfo&      out_error,
             const XrdSecEntity* client,
             const char*         info = 0 );


    //--------------------------------------------------------------------------
    //! This function we needs to work without authorization
    //--------------------------------------------------------------------------
    int stat( const char*         Name,
              struct stat*        buf,
              XrdOucErrInfo&      out_error,
              const XrdSecEntity* client,
              const char*         opaque = 0 );



    //--------------------------------------------------------------------------
    //! This function deals with plugin calls
    //--------------------------------------------------------------------------
    int FSctl( int, XrdSfsFSctl&, XrdOucErrInfo&, const XrdSecEntity* );


    //--------------------------------------------------------------------------
    //!
    //--------------------------------------------------------------------------
    void ThirdPartySetup( const char* transferdirectory, int slots, int rate );


    //--------------------------------------------------------------------------
    //!
    //--------------------------------------------------------------------------
    void ThirdPartyCleanupOnRestart();


    XrdSysMutex         MetaMutex;     ///< mutex to protect the UpdateMeta function
    XrdSysMutex         PasswdMutex;   ///< mutex to protect the passwd expiry hash

    static XrdOucHash<XrdxCastor2ClientAdmin>* clientadmintable; ///<
    static XrdOucHash<struct passwd>* passwdstore;               ///<

    XrdOucHash<XrdOucString> FileNameMap;    ///< keeping the mapping LFN->PFN for open files
    XrdSysMutex         FileNameMapLock;     ///< protecting the MAP hash for LFN->PFN

    XrdOucString        Procfilesystem;      ///< location of the proc file system directory
    bool                ProcfilesystemSync;  ///< sync every access on disk

    XrdSysError*        Eroute;
    const char*         LocalRoot;           ///< we need this to write /proc variables directly to the FS

    bool                doChecksumStreaming; ///<
    bool                doChecksumUpdates;   ///<
    bool                doPOSC;              ///<

    bool                ThirdPartyCopy;               ///< default from Configure
    int                 ThirdPartyCopySlots;          ///< default from Configure
    int                 ThirdPartyCopySlotRate;       ///< default from Configure
    XrdOucString        ThirdPartyCopyStateDirectory; ///< default from Configure

  private:

    vecString procUsers;   ///< vector of users ( reduced tidents <user>@host 
                           ///< which can write into /proc )

    //--------------------------------------------------------------------------
    //! Write values to the /proc file system - source in XrdxCastor2OfsProc.cc
    //--------------------------------------------------------------------------
    bool Write2ProcFile( const char* name, long long value );


    //--------------------------------------------------------------------------
    //! Read values out of the /proc file system
    //--------------------------------------------------------------------------
    bool ReadAllProc();


    //--------------------------------------------------------------------------
    //! Read values out of the /proc file system
    //--------------------------------------------------------------------------
    bool ReadFromProc( const char* entryname );
};

extern XrdxCastor2Ofs XrdxCastor2OfsFS;  ///< global instance of the Ofs subsystem

#endif
