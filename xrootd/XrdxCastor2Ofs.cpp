/*******************************************************************************
 *                      XrdxCastor2Ofs.cc
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
#include <sys/types.h>
#include <sys/stat.h>
#include <grp.h>
#include <attr/xattr.h>
#include "h/Cns_api.h"
#include "h/serrno.h"
#include <string.h>
/*----------------------------------------------------------------------------*/
#include "XrdAcc/XrdAccAuthorize.hh"
#include "XrdOfs/XrdOfsTrace.hh"
#include "XrdOfs/XrdOfs.hh"
#include "XrdSfs/XrdSfsAio.hh"
#include "XrdSys/XrdSysDNS.hh"
#include "XrdOss/XrdOssApi.hh"
#include "XrdOuc/XrdOucTrace.hh"
#include "XrdOuc/XrdOucHash.hh"
#include "XrdNet/XrdNetOpts.hh"
#include "XrdCl/XrdClFileSystem.hh"
/*----------------------------------------------------------------------------*/
#include "XrdxCastor2Ofs.hpp"
#include "XrdxCastor2FsConstants.hpp"
#include "XrdxCastorTiming.hpp"
/*----------------------------------------------------------------------------*/

XrdxCastor2Ofs XrdxCastor2OfsFS;

// extern symbols
extern XrdOfs*     XrdOfsFS;
extern XrdSysError OfsEroute;
extern XrdOssSys*  XrdOfsOss;
extern XrdOss*     XrdOssGetSS( XrdSysLogger*, const char*, const char* );
extern XrdOucTrace OfsTrace;

// global singletons
XrdOucHash<struct passwd>* XrdxCastor2Ofs::passwdstore;


//------------------------------------------------------------------------------
// SfsGetFileSystem
//------------------------------------------------------------------------------
extern "C"
{
  XrdSfsFileSystem* XrdSfsGetFileSystem( XrdSfsFileSystem* /*native_fs*/,
                                         XrdSysLogger*     lp,
                                         const char*       configfn )
  {
    // Do the herald thing
    OfsEroute.SetPrefix( "castor2ofs_" );
    OfsEroute.logger( lp );
    OfsEroute.Say( "++++++ (c) 2008 CERN/IT-DM-SMD ",
                   "xCastor2Ofs (extended Castor2 File System) v 1.0" );

    // Initialize the subsystems
    XrdxCastor2OfsFS.ConfigFN = ( configfn && *configfn ? strdup( configfn ) : 0 );
    XrdxCastor2OfsFS.passwdstore = new XrdOucHash<struct passwd> ();

    if ( XrdxCastor2OfsFS.Configure( OfsEroute ) ) return 0;

    // All done, we can return the callout vector to these routines
    XrdOfsFS = &XrdxCastor2OfsFS;
    return &XrdxCastor2OfsFS;
  }
}


/******************************************************************************/
/*                         x C a s t o r O f s                                */
/******************************************************************************/

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
XrdxCastor2Ofs::XrdxCastor2Ofs(): 
  LogId() 
{
  mLogLevel = LOG_ERR; // log everything above err
}


//------------------------------------------------------------------------------
// Configure
//------------------------------------------------------------------------------
int XrdxCastor2Ofs::Configure( XrdSysError& Eroute )
{
  char* var;
  const char* val;
  int  cfgFD;

  // Extract the manager from the config file
  XrdOucStream config_stream( &Eroute, getenv( "XRDINSTANCE" ) );
  XrdxCastor2OfsFS.doChecksumStreaming = true;
  XrdxCastor2OfsFS.doChecksumUpdates   = false;
  XrdxCastor2OfsFS.doPOSC = false;
  XrdxCastor2OfsFS.ThirdPartyCopy = true;
  XrdxCastor2OfsFS.ThirdPartyCopySlots = 5;
  XrdxCastor2OfsFS.ThirdPartyCopySlotRate = 25;
  XrdxCastor2OfsFS.ThirdPartyCopyStateDirectory = "/var/log/xrootd/server/transfer";
  Procfilesystem = "/tmp/xcastor2-ofs/";
  ProcfilesystemSync = false;

  if ( !ConfigFN || !*ConfigFN ) {
    // This error will be reported by XrdxCastor2OfsFS.Configure
  } else {
    // Try to open the configuration file.
    if ( ( cfgFD = open( ConfigFN, O_RDONLY, 0 ) ) < 0 ) {
      return Eroute.Emsg( "Config", errno, "open config file fn=", ConfigFN );
    }

    config_stream.Attach( cfgFD );
   
    // Now start reading records until eof.
    while ( ( var = config_stream.GetMyFirstWord() ) ) {
      if ( !strncmp( var, "xcastor2.", 9 ) ) {
        var += 9;

        // Get the debug level
        if ( !strcmp( "debuglevel", var ) ) {
          if ( !( val = config_stream.GetWord() ) ) {
            Eroute.Emsg( "Config", "argument for debug level invalid set to ERR." );
            mLogLevel = LOG_ERR;
          } else {
            std::string str_val(val);
            if (isdigit(str_val[0])) {
              // The level is given as a number 
              mLogLevel = atoi(val);
            }
            else {
              // The level is given as a string
              mLogLevel = Logging::GetPriorityByString(val);
            }
            Logging::SetLogPriority(mLogLevel);
            Eroute.Say( "=====> xcastor2.debuglevel: ", 
                        Logging::GetPriorityString(mLogLevel), "" );
          }
        }

        // Get any debug filter name 
        if ( !strcmp( "debugfilter", var ) ) {
          if ( !( val = config_stream.GetWord() ) ) {
            Eroute.Emsg( "Config", "argument for debug filter invalid set to none." );
          } else {
            Logging::SetFilter(val);
            Eroute.Say( "=====> xcastor2.debugfileter: ", val, "" );
          }
        }

        if ( !strcmp( "procuser", var ) ) {
          if ( ( val = config_stream.GetWord() ) ) {
            XrdOucString sval = val;
            procUsers.Push_back( sval );
          }
        }

        if ( !strcmp( "posc", var ) ) {
          if ( ( val = config_stream.GetWord() ) ) {
            XrdOucString sval = val;

            if ( ( sval == "true" ) || ( sval == "1" ) || ( sval == "yes" ) ) {
              XrdxCastor2OfsFS.doPOSC = true;
            }
          }
        }

        if ( !strcmp( "checksum", var ) ) {
          if ( ( val = config_stream.GetWord() ) ) {
            XrdOucString sval = val;

            if ( sval == "always" ) {
              XrdxCastor2OfsFS.doChecksumStreaming = true;
              XrdxCastor2OfsFS.doChecksumUpdates   = true;
            } 
            else if ( sval == "never" ) {
              XrdxCastor2OfsFS.doChecksumStreaming = false;
              XrdxCastor2OfsFS.doChecksumUpdates   = false;
            } 
            else if ( sval == "streaming" ) {
              XrdxCastor2OfsFS.doChecksumStreaming = true;
              XrdxCastor2OfsFS.doChecksumUpdates   = false;
            } else {
              Eroute.Emsg( "Config", "argument for checksum invalid. Specify"
                           " xcastor2.checksum [always|never|streaming]" );
              exit( -1 );
            }
          }
        }

        if ( !strcmp( "proc", var ) )
          if ( !( val = config_stream.GetWord() ) ) {
            Eroute.Emsg( "Config", "argument for proc invalid." );
            exit( -1 );
          } else {
            Procfilesystem = val;

            if ( ( val = config_stream.GetWord() ) ) {
              if ( ( !strcmp( val, "sync" ) ) || ( !strcmp( val, "async" ) ) ) {
                if ( !strcmp( val, "sync" ) ) ProcfilesystemSync = true;
                if ( !strcmp( val, "async" ) ) ProcfilesystemSync = false;
              } else {
                Eroute.Emsg( "Config", "argument for proc invalid. Specify xcastor2.proc <path> [sync|async]" );
                exit( -1 );
              }
            }

            while ( Procfilesystem.replace( "//", "/" ) ) {}

            if ( ProcfilesystemSync ) {
              Eroute.Say( "=====> xcastor2.proc: ", Procfilesystem.c_str(), " sync" );
            }
            else {
              Eroute.Say( "=====> xcastor2.proc: ", Procfilesystem.c_str(), " async" );
            }
          }

        if ( !strcmp( "thirdparty", var ) ) {
          if ( ( val = config_stream.GetWord() ) ) {
            if ( ( !strcmp( val, "1" ) ) || 
                 ( !strcmp( val, "yes" ) ) || 
                 ( !strcmp( val, "true" ) ) ) 
            {
              ThirdPartyCopy = true;
            }  else {
              ThirdPartyCopy = false;
            }
          }
        }

        if ( !strcmp( "thirdparty.slots", var ) ) {
          if ( ( val = config_stream.GetWord() ) ) {
            int slots = atoi( val );
            if ( slots < 1 )   slots = 1;
            if ( slots > 128 ) slots = 128;
            ThirdPartyCopySlots = slots;
          }
        }

        if ( !strcmp( "thirdparty.slotrate", var ) ) {
          if ( ( val = config_stream.GetWord() ) ) {
            int rate = atoi( val );
            if ( rate < 1 ) rate = 1;
            ThirdPartyCopySlotRate = rate;
          }
        }

        if ( !strcmp( "thirdparty.statedirectory", var ) ) {
          if ( ( val = config_stream.GetWord() ) ) {
            ThirdPartyCopyStateDirectory = val;
          }
        }
      }
    }

    config_stream.Close();
  }

  XrdOucString makeProc;
  makeProc = "mkdir -p ";
  makeProc += Procfilesystem;
  makeProc += "/proc";
#ifdef XFS_SUPPORT
  makeProc += "/xfs";
#endif
  makeProc += "; chown stage.st ";
  makeProc += Procfilesystem;
  makeProc += "/proc";
  system( makeProc.c_str() );

  if ( access( Procfilesystem.c_str(), R_OK | W_OK ) ) {
    Eroute.Emsg( "Config", "Cannot use given third proc directory ", Procfilesystem.c_str() );
    exit( -1 );
  }

  if ( XrdxCastor2OfsFS.ThirdPartyCopy ) {
    XrdOucString makeStateDirectory;
    makeStateDirectory = "mkdir -p ";
    makeStateDirectory += ThirdPartyCopyStateDirectory.c_str();
    makeStateDirectory += "; chown stage.st ";
    makeStateDirectory += ThirdPartyCopyStateDirectory.c_str();
    system( makeStateDirectory.c_str() );

    if ( access( ThirdPartyCopyStateDirectory.c_str(), R_OK | W_OK ) ) {
      Eroute.Emsg( "Config", "Cannot use given third party state directory ", 
                   ThirdPartyCopyStateDirectory.c_str() );
      exit( -1 );
    }

    // Set the configuration values
    XrdxCastor2OfsFS.ThirdPartySetup( XrdxCastor2OfsFS.ThirdPartyCopyStateDirectory.c_str(), 
                                      ThirdPartyCopySlots, ThirdPartyCopySlotRate );
    XrdxCastor2Ofs::ThirdPartyCleanupOnRestart();
  }

  for ( int i = 0; i < procUsers.GetSize(); i++ ) {
    Eroute.Say( "=====> xcastor2.procuser: ", procUsers[i].c_str() );
  }

  if ( doPOSC ) {
    Eroute.Say( "=====> xcastor2.posc: yes", "", "" );
  } else {
    Eroute.Say( "=====> xcastor2.posc: no ", "", "" );
  }

  if ( ThirdPartyCopy ) {
    Eroute.Say( "=====> xcastor2.thirdparty: yes", "", "" );
    XrdOucString slots = "";
    slots += ThirdPartyCopySlots;
    Eroute.Say( "=====> xcastor2.thirdparty.slots: ", slots.c_str(), "" );
    XrdOucString rate = "";
    rate += ThirdPartyCopySlotRate;
    rate += " Mb/s";
    Eroute.Say( "=====> xcastor2.thirdparty.rate: ", rate.c_str(), "" );
    Eroute.Say( "=====> xcastor2.thirdparty.statedirectory: ", ThirdPartyCopyStateDirectory.c_str() );
  } else {
    Eroute.Say( "=====> xcastor2.thirdparty: no ", "", "" );
  }

  // Setup the circular in-memory logging buffer  
  XrdOucString unit = "rdr@";
  unit += XrdSysDNS::getHostName();
  unit += ":1094";

  Logging::Init();
  Logging::SetLogPriority( mLogLevel );
  Logging::SetUnit( unit.c_str() );
  xcastor_info( "info=\"logging configured\" " );

  // Read in /proc variables at startup time to reset to previous values
  ReadAllProc();
  UpdateProc( "*" );
  ReadAllProc();
  int rc = XrdOfs::Configure( Eroute );
  
  // We need to set the effective user for all the XrdClient's used to issue 
  // 'prepares' to redirectors or third-party transfers
  setenv( "XrdClientEUSER", "stage", 1 );
  return rc;
}


//------------------------------------------------------------------------------
// Remove
//------------------------------------------------------------------------------
int
XrdxCastor2Ofs::rem( const char*         path,
                     XrdOucErrInfo&      error,
                     const XrdSecEntity* /*client*/,
                     const char*         /*info*/ )
{
  const char* tident = error.getErrUser();
  bool allowed = false;
  XrdOucString reducedTident, user, stident;
  reducedTident = "";
  user = "";
  stident = tident;
  int dotpos = stident.find( "." );
  reducedTident.assign( tident, 0, dotpos - 1 );
  reducedTident += "@";
  int atpos  = stident.find( "@" );
  user.assign( tident, atpos + 1 );
  reducedTident += user;

  for ( int i = 0 ; i < XrdxCastor2OfsFS.procUsers.GetSize(); i++ ) {
    if ( XrdxCastor2OfsFS.procUsers[i] == reducedTident ) {
      allowed = true;
      break;
    }
  }

  if ( !allowed ) {
    return XrdxCastor2OfsFS.Emsg( "rem", error, EPERM, "allow access to unlink", path );
  }

  if ( unlink( path ) ) {
    // We don't report deletion of a not existing file as error
    if ( errno != ENOENT ) {
      return XrdxCastor2OfsFS.Emsg( "rem", error, errno, "remove", path );
    }
  }

  XrdOfsHandle::Hide( path );
  return SFS_OK;
}


//------------------------------------------------------------------------------
// Stat
//------------------------------------------------------------------------------
int
XrdxCastor2Ofs::stat( const char*         Name,
                      struct stat*        buf,
                      XrdOucErrInfo&      /*out_error*/,
                      const XrdSecEntity* /*client*/,
                      const char*         /*opaque*/ )
{
  int fd = ::open( "/etc/castor/status", O_RDONLY );

  if ( fd ) {
    char buffer[4096];
    int nread = ::read( fd, buffer, 4095 );

    if ( nread > 0 ) {
      buffer[nread] = 0;
      XrdOucString status = buffer;

      if ( ( status.find( "DiskServerStatus=DISKSERVER_PRODUCTION" ) ) == STR_NPOS ) {
        close( fd );
        return SFS_ERROR;
      }

      int fpos = 1;
      XrdOucString name = Name;

      while ( ( fpos = name.find( "/", fpos ) ) != STR_NPOS ) {
        XrdOucString fsprod;
        fsprod.assign( name, 0, fpos - 1 );
        XrdOucString tag1 = fsprod;
        tag1 += "=FILESYSTEM_PRODUCTION";
        XrdOucString tag2 = fsprod;
        tag2 += "/";
        tag2 += "=FILESYSTEM_PRODUCTION";

        if ( ( status.find( tag1 ) != STR_NPOS ) || ( status.find( tag2 ) != STR_NPOS ) ) {
          // yes in production
          break;
        }

        fpos++;
      }

      if ( fpos == STR_NPOS ) {
        close( fd );
        return SFS_ERROR;
      }
    }

    close( fd );
  }

  if ( !::stat( Name, buf ) ) return SFS_OK;
  else return SFS_ERROR;
}


/******************************************************************************/
/*                         x C a s t o r O f s F i l e                        */
/******************************************************************************/


//------------------------------------------------------------------------------
// Constuctor
//------------------------------------------------------------------------------
XrdxCastor2OfsFile::XrdxCastor2OfsFile( const char* user, int MonID ) :
  XrdOfsFile( user, MonID )
{
  envOpaque = NULL;
  IsAdminStream = false;
  IsThirdPartyStream = false;
  IsThirdPartyStreamCopy = false;
  firstWrite = true;
  hasWrite = false;
  stagehost = "none";
  serviceclass = "none";
  reqid = "0";
  hasadler = 1;
  adler = adler32( 0L, Z_NULL, 0 );
  adleroffset = 0;
  DiskChecksum = "";
  DiskChecksumAlgorithm = "";
  verifyChecksum = true;
  hasadlererror = false;
  castorlfn = "";
  Transfer = 0;
  StagerJobPid = 0;
  IsClosed = false;
  StagerJob = 0;
  SjobUuid = "";
  stagerjobport = 0;
  isOpen = false;
  isRW = false;
  isTruncate = false;
  viaDestructor = false;
  mTpcKey = "";
}


//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
XrdxCastor2OfsFile::~XrdxCastor2OfsFile()
{
  viaDestructor = true;
  close();

  if ( envOpaque ) delete envOpaque;
  if ( Transfer ) XrdTransferManager::TM()->DetachTransfer( Transfer );
  if ( StagerJob ) delete StagerJob;

  envOpaque = NULL;
  Transfer = 0;
  StagerJob = 0;
}


//------------------------------------------------------------------------------
// XrdxCastor2OfsFile::open
//------------------------------------------------------------------------------
int
XrdxCastor2OfsFile::open( const char*         path,
                          XrdSfsFileOpenMode  open_mode,
                          mode_t              create_mode,
                          const XrdSecEntity* client,
                          const char*         opaque )
{
  xcastor_debug("path=%s", path);

  // The OFS open is caugth to set the access/modify time in the nameserver
  procRequest = kProcNone;
  char* val = 0;
  const char* tident = error.getErrUser();
  xcastor::Timing opentiming("ofsf::open");
  TIMING("START", &opentiming);
  XrdOucString spath = path;
  isRW = false;
  isTruncate = false;
  castorlfn = path;
  xcastor_debug("castorlfn=%s, opaque=%s", castorlfn.c_str(), opaque);
  XrdOucString newpath = "";
  XrdOucString newopaque = opaque;

  // If there is explicit user opaque information we find two ?, 
  // so we just replace it with a seperator.
  newopaque.replace( "?", "&" );
  newopaque.replace( "&&", "&" );

  // Check if there are several redirection tokens
  int firstpos = 0;
  int lastpos = 0;
  int newpos = 0;
  firstpos = newopaque.find( "castor2fs.sfn", 0 );
  lastpos = firstpos + 1;

  while ( ( newpos = newopaque.find( "castor2fs.sfn", lastpos ) ) != STR_NPOS ) {
    lastpos = newpos + 1;
  }

  // Erase from the beginning to the last token start
  if ( lastpos > ( firstpos + 1 ) ) {
    newopaque.erase( firstpos, lastpos - 2 - firstpos );
  }

  // Erase the tried parameter from the opaque information
  int tried_pos = newopaque.find( "tried=" );
  int amp_pos = newopaque.find ( '&', tried_pos );

  if ( tried_pos != STR_NPOS ) {
    newopaque.erase( tried_pos, amp_pos - tried_pos );
  }

  if ( newopaque.find( "castor2fs.signature=" ) == STR_NPOS ) {
    // This is a backdoor for the tape to update files
    firstWrite = false;
  }

  // This prevents 'clever' users from faking internal opaque information
  newopaque.replace( "ofsgranted=", "notgranted=" );
  newopaque.replace( "source=", "nosource=" );
  envOpaque = new XrdOucEnv( newopaque.c_str() );
  newpath = envOpaque->Get( "castor2fs.pfn1" );

  if ( newopaque.endswith( "&" ) ) {
    newopaque += "source=";
  } else {
    newopaque += "&source=";
  }

  newopaque += newpath.c_str();

  if ( !envOpaque->Get( "target" ) ) {
    if ( newopaque.endswith( "&" ) ) {
      newopaque += "target=";
    } else {
      newopaque += "&target=";
    }

    newopaque += newpath.c_str();
  }

  // Store opaque information
  if ( envOpaque->Get( "castor2ofsproc" ) ) {
    // that is strange ;-) ....
    newopaque.replace( "castor2ofsproc", "castor2ofsfake" );
  }

  XrdOucString verifytag = envOpaque->Get( "verifychecksum" );

  if ( ( verifytag == "false" ) ||
       ( verifytag == "0" ) ||
       ( verifytag == "off" ) ) {
    verifyChecksum = false;
  }

  open_mode |= SFS_O_MKPTH;
  create_mode |= SFS_O_MKPTH;

  if ( ( open_mode & ( SFS_O_RDONLY | SFS_O_WRONLY | SFS_O_RDWR |
                       SFS_O_CREAT  | SFS_O_TRUNC ) ) != 0 ) {
    isRW = true;
  }

  if ( open_mode & SFS_O_TRUNC )
    isTruncate = true;

  if ( spath.beginswith( "/proc/" ) ) {
    procRequest = kProcRead;

    if ( isRW ) {
      procRequest = kProcWrite;
    }

    // Declare this as a proc path to the authorization plugin
    newopaque += "&";
    newopaque += "castor2ofsproc=true";
  } else {
    // Deny this as a proc path to the authorization plugin
    newopaque += "&";
    newopaque += "castor2ofsproc=false";
  }

  if ( !procRequest ) {
    // Send an acc/mod update to the server
    switch ( open_mode & ( SFS_O_RDONLY | SFS_O_WRONLY | SFS_O_RDWR |
                           SFS_O_CREAT  | SFS_O_TRUNC ) ) {
    case SFS_O_RDONLY:
      break;
      break;

    default:
      break;
    }
  } else {
    // Reroot the /proc location
    XrdOucString prependedspath = XrdxCastor2OfsFS.Procfilesystem;
    prependedspath += "/";
    prependedspath += spath;

    while ( prependedspath.replace( "//", "/" ) ) {};

    // check the allowed proc users
    // this is not a high performance implementation but the usage of /proc is very rare anyway
    //
    // clients are allowed to read/modify /proc, when they appear in the configuration file f.e. as
    // -> xcastor2.procUsers xrootd@pcitmeta.cern.ch
    // where the user name in front of @ is the UID name on the client side
    // and the host name after the @ is the hostname without domain
    XrdOucString reducedTident, user, stident;
    reducedTident = "";
    user = "";
    stident = tident;
    int dotpos = stident.find( "." );
    reducedTident.assign( tident, 0, dotpos - 1 );
    reducedTident += "@";
    int adpos  = stident.find( "@" );
    user.assign( tident, adpos + 1 );
    reducedTident += user;
    bool allowed = false;

    for ( int i = 0 ; i < XrdxCastor2OfsFS.procUsers.GetSize(); i++ ) {
      if ( XrdxCastor2OfsFS.procUsers[i] == reducedTident ) {
        allowed = true;
        break;
      }
    }

    if ( !allowed ) {
      return XrdxCastor2OfsFS.Emsg( "open", error, EPERM, "allow access to /proc/ fs in xrootd", path );
    }

    if ( procRequest == kProcWrite ) {
      open_mode = SFS_O_RDWR | SFS_O_TRUNC;
      prependedspath += ".__proc_update__";
    } else {
      // Depending on the desired file, we update the information in that file
      if ( !XrdxCastor2OfsFS.UpdateProc( prependedspath.c_str() ) )
        return XrdxCastor2OfsFS.Emsg( "open", error, EIO, "update /proc information", path );
    }

    // We have to open the rerooted proc path!
    int rc = XrdOfsFile::open( prependedspath.c_str(), open_mode, create_mode, client, newopaque.c_str() );
    return rc;
  }

  // Deal with native TPC transfers which are passed directly to the OFS layer 
  if (newopaque.find("tpc.dst") != STR_NPOS)
  {
    // This is a source TPC file and we just received the second open reuqest from the initiator.
    // We save the mapping between the tpc.key and the castor lfn for future open requests from 
    // the destination of the TPC transfer.
    if ((val = envOpaque->Get("tpc.key")))
    {    
      mTpcKey = val;
      struct TpcInfo transfer = (struct TpcInfo) { std::string(newpath.c_str()), time(NULL) };
      XrdxCastor2OfsFS.mTpcMapMutex.Lock();     // -->
      std::pair< std::map<std::string, struct TpcInfo>::iterator, bool> pair =
        XrdxCastor2OfsFS.mTpcMap.insert(std::make_pair(mTpcKey, transfer));
      
      if (pair.second == false)
      {
        xcastor_err("tpc.key:%s is already in the map", mTpcKey.c_str());
        XrdxCastor2OfsFS.mTpcMap.erase(pair.first);
        XrdxCastor2OfsFS.mTpcMapMutex.UnLock(); // <--
        return XrdxCastor2OfsFS.Emsg( "open", error, EINVAL, 
                                      "tpc.key already in the map for file:  ",
                                      newpath.c_str() );
      }
      
      XrdxCastor2OfsFS.mTpcMapMutex.UnLock();   // <--
      int rc = XrdOfsFile::open( newpath.c_str(), open_mode, create_mode, client, newopaque.c_str() );
      return rc;
    }

    xcastor_err("no tpc.key in the opaque information");
    return XrdxCastor2OfsFS.Emsg( "open", error, ENOENT, 
                                  "no tpc.key in the opaque info for file: ",
                                  newpath.c_str() );
  }
  else if (newopaque.find("tpc.org") != STR_NPOS)
  {
    // This is a source TPC file and we just received the third open request which
    // comes directly from the destination of the transfer. Here we need to retrieve
    // the castor lfn from the map which we saved previously as the destination has 
    // no knowledge of the castor mapping. 
    if ((val = envOpaque->Get("tpc.key")))
    {
      std::string key = val;   
      XrdxCastor2OfsFS.mTpcMapMutex.Lock();     // -->
      std::map<std::string, struct TpcInfo>::iterator iter = XrdxCastor2OfsFS.mTpcMap.find(key);

      if (iter != XrdxCastor2OfsFS.mTpcMap.end())
      { 
        std::string saved_lfn = iter->second.path;
        XrdxCastor2OfsFS.mTpcMapMutex.UnLock(); // <--
        int rc = XrdOfsFile::open(saved_lfn.c_str(), open_mode, create_mode, 
                                  client, newopaque.c_str() );
        return rc;
      }

      XrdxCastor2OfsFS.mTpcMapMutex.UnLock(); // <--
      return XrdxCastor2OfsFS.Emsg( "open", error, EINVAL, 
                                    "can not find tpc.key in map for file: ",
                                    newpath.c_str() );
    }
   
    xcastor_err("no tpc.key in the opaque information");
    return XrdxCastor2OfsFS.Emsg( "open", error, ENOENT, 
                                  "no tpc.key in the opaque info for file: ",
                                  newpath.c_str() );
  }

  TIMING("PROCBLOCK", &opentiming);

  //////////////////////////////////////////////////////////////////////////////
  //............................................................................
  // Section to deal with third party transfer streams
  //............................................................................
  if ( XrdxCastor2OfsFS.ThirdPartyCopy && ( val = envOpaque->Get( "xferuuid" ) ) ) {
    IsThirdPartyStream = true;
    // look at third party transfers
    // do some checks to allow to bypass the authorization module
    val = envOpaque->Get( "xfercmd" );
    XrdOucString xcmd = val;

    if ( xcmd == "copy" ) {
      IsThirdPartyStreamCopy = true;
      // we have to get the physical mapping from the Map
      XrdOucString* mappedpath = 0;
      XrdxCastor2OfsFS.FileNameMapLock.Lock();
      mappedpath = XrdxCastor2OfsFS.FileNameMap.Find( path );
      XrdxCastor2OfsFS.FileNameMapLock.UnLock();

      if ( !mappedpath ) {
        // the user has not anymore the file open, for the moment we forbid this!
        return XrdxCastor2OfsFS.Emsg( "open", error, EPERM, "open transfer stream - "
                                      "destination file is not open anymore", newpath.c_str() );
      } else {
        newpath = mappedpath->c_str();
        // place it for the close statement
        envOpaque->Put( "castor2fs.pfn1", newpath.c_str() );
      }

      // determine if the file exists and has size 0
      if ( !( XrdOfsOss->Stat( newpath.c_str(), &statinfo ) ) ) {
        // this must be a copy write stream, and the file has to exist with size 0
        if ( isRW && ( !statinfo.st_size ) ) {
          newopaque += "&ofsgranted=yes";
        } // else: this will file latest during the open authorization
      } // else: this will file latest during the open authorization
    } else {
      // store a mapping for the third party copy who doesn't know the PFN
      XrdxCastor2OfsFS.FileNameMapLock.Lock();
      XrdxCastor2OfsFS.FileNameMap.Add( path, new XrdOucString( newpath.c_str() ) );
      XrdxCastor2OfsFS.FileNameMapLock.UnLock();
    }
  }
  //////////////////////////////////////////////////////////////////////////////

  int retc;
  char* v = 0;

  if ( ( v = envOpaque->Get( "createifnotexist" ) ) ) {
    if ( atoi( v ) ) {
      if ( !isRW ) {
        if ( ( retc = XrdOfsOss->Stat( newpath.c_str(), &statinfo ) ) ) {
          // If this does not work we get the error later in any case!
          creat( newpath.c_str(), S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH );
        }
      }
    }
  }

  if ( ( retc = XrdOfsOss->Stat( newpath.c_str(), &statinfo ) ) ) {
    // File does not exist, keep the create flag (unless this is 
    // an update but the file didn't exist on the disk server
    if ( envOpaque->Get( "castor2fs.accessop" ) && 
         ( atoi( envOpaque->Get( "castor2fs.accessop" ) ) == AOP_Update ) ) 
      {
        firstWrite = true;
      } else {
      firstWrite = false;
    }
  } else {
    if ( open_mode & SFS_O_CREAT )
      open_mode -= SFS_O_CREAT;

    // the file exists and it is in write mode = update
    // hmm... not this one
    // try to get a checksum from the filesystem
    char diskckSumbuf[32 + 1];
    char diskckSumalg[32];
    diskckSumbuf[0] = diskckSumalg[0] = 0;
    // Get existing checksum - we don't check errors here
    int nattr = 0;
    nattr = getxattr( newpath.c_str(), "user.castor.checksum.type", diskckSumalg, sizeof( diskckSumalg ) );

    if ( nattr ) diskckSumalg[nattr] = 0;

    nattr = getxattr( newpath.c_str(), "user.castor.checksum.value", diskckSumbuf, sizeof( diskckSumbuf ) );

    if ( nattr ) diskckSumbuf[nattr] = 0;

    DiskChecksum = diskckSumbuf;
    DiskChecksumAlgorithm = diskckSumalg;
    xcastor_debug("checksum algorithm=%s, checksum=%s", DiskChecksumAlgorithm.c_str(),
                  DiskChecksum.c_str() );
  }

  uid_t sec_uid = 0;
  uid_t sec_gid = 0;

  if ( envOpaque->Get( "castor2fs.client_sec_uid" ) ) {
    sec_uid = atoi( envOpaque->Get( "castor2fs.client_sec_uid" ) );
  }

  if ( envOpaque->Get( "castor2fs.client_sec_gid" ) ) {
    sec_gid = atoi( envOpaque->Get( "castor2fs.client_sec_gid" ) );
  }

  // Extract the stager information
  val = envOpaque->Get( "castor2fs.pfn2" );

  if ( !( XrdxCastor2OfsFS.ThirdPartyCopy ) || ( !IsThirdPartyStreamCopy ) ) {
    if ( !( val ) || ( !strlen( val ) ) ) {
      return XrdxCastor2OfsFS.Emsg( "open", error, ESHUTDOWN, "reqid/pfn2 is missing", FName() );
    }
  } else {
    // Third party streams don't have any req id
    val = "";
  }

  XrdOucString reqtag = val;
  reqid = "0";
  stagehost = "none";
  serviceclass = "none";
  stagerjobport = 0;
  SjobUuid = "";
  int pos1;
  int pos2;
  int pos3;
  int pos4;

  // Syntax is reqid: <reqid:stagerhost:serviceclass:stagerjobport:stagerjobuuid>
  if ( ( pos1 = reqtag.find( ":" ) ) != STR_NPOS ) {
    reqid.assign( reqtag, 0, pos1 - 1 );

    if ( ( pos2 = reqtag.find( ":", pos1 + 1 ) ) != STR_NPOS )  {
      stagehost.assign( reqtag, pos1 + 1, pos2 - 1 );

      if ( ( pos3 = reqtag.find( ":", pos2 + 1 ) ) != STR_NPOS ) {
        serviceclass.assign( reqtag, pos2 + 1, pos3 - 1 );

        if ( ( pos4 = reqtag.find( ":", pos3 + 1 ) ) != STR_NPOS ) {
          XrdOucString sport;
          sport.assign( reqtag, pos3 + 1, pos4 - 1 );
          stagerjobport = atoi( sport.c_str() );
          SjobUuid.assign( reqtag, pos4 + 1 );
        }
      }
    }
  }

  xcastor_debug("StagerJob uuid=%s, port=%i ", SjobUuid.c_str(), stagerjobport );
  StagerJob = new XrdxCastor2Ofs2StagerJob( SjobUuid.c_str(), stagerjobport );

  if ( !IsThirdPartyStreamCopy ) {
    // Send the sigusr1 to the local xcastor2job to signal the open
    if ( !StagerJob->Open() ) {
      // The open failed
      xcastor_err("error: open => couldn't run the Open towards StagerJob: "
                  "reqid=%s, stagerjobport=%i, stagerjobuuid=%s",
                  reqid.c_str(), stagerjobport, SjobUuid.c_str() );
      return XrdxCastor2OfsFS.Emsg( "open", error, EIO, "signal open to the corresponding stagerjob", "" );
    }

    if ( IsThirdPartyStream ) {
      if ( isRW ) {
        hasWrite = true;

        // Here we send the update for the 1st byte written
        if ( UpdateMeta() != SFS_OK ) {
          return SFS_ERROR;
        }

        firstWrite = false;
      }
    }
  }

  TIMING("FILEOPEN", &opentiming);
  int rc = XrdOfsFile::open( newpath.c_str(), open_mode, create_mode, client, newopaque.c_str() );

  //////////////////////////////////////////////////////////////////////////////
  //............................................................................
  // Section to deal with third party transfer streams
  //............................................................................
  if ( rc == SFS_OK ) {
    isOpen = true;

    if ( IsThirdPartyStream ) {
      TIMING("THIRDPARTY", &opentiming);
      xcastor_debug("copy slots scheduled: %i, available: %i", 
                    XrdTransferManager::TM()->ScheduledTransfers.Num(), 
                    XrdxCastor2OfsFS.ThirdPartyCopySlots );
      // evt. change the thread parameters defined on the /proc state
      XrdxCastor2OfsFS.ThirdPartySetup( XrdxCastor2OfsFS.ThirdPartyCopyStateDirectory.c_str(), 
                                        XrdxCastor2OfsFS.ThirdPartyCopySlots, 
                                        XrdxCastor2OfsFS.ThirdPartyCopySlotRate );

      // execute the third party managing function
      rc = ThirdPartyTransfer( path, open_mode, create_mode, client, newopaque.c_str() );

      if ( rc > 0 ) {
        // send a stall to the client, the transfer is already detached ....
        XrdOfsFile::close();
        return retc;
      }
    }

    if ( isTruncate ) {
      truncate( 0 );
    }
  }

  //////////////////////////////////////////////////////////////////////////////
  if ( IsThirdPartyStreamCopy ) {
    // retrieve the req id from the opaque info

    // we wait for the stagerJob PID from x2castorjob
    // try to read the pid file
    if ( !Transfer ) {
      // uups this is fatal and strange
      return XrdxCastor2OfsFS.Emsg( "open", error, EINVAL, 
                                    "open attach any transfer as a copy stream; ", newpath.c_str() );
    }

    XrdOucString reqtag = Transfer->Options->Get( "castor2fs.pfn2" );
    reqid = "0";
    int pos1;

    if ( ( pos1 = reqtag.find( ":" ) ) != STR_NPOS ) {
      reqid.assign( reqtag, 0, pos1 - 1 );
    }
  }

  TIMING("DONE", &opentiming);
  opentiming.Print();
  return rc;
}


//------------------------------------------------------------------------------
// XrdxCasto2OfsFile::close
//------------------------------------------------------------------------------
int
XrdxCastor2OfsFile::close()
{
  int rc = SFS_OK;

  if ( IsClosed )
    return true;

  // If this is a TPC transfer then we can drop the key from the map
  if (mTpcKey != "")
  {
    xcastor_debug("drop from map tpc.key:%s", mTpcKey.c_str());
    XrdSysMutexHelper lock(XrdxCastor2OfsFS.mTpcMapMutex);
    XrdxCastor2OfsFS.mTpcMap.erase(mTpcKey);
    mTpcKey = "";

    // Remove keys which are older than one hour
    std::map<std::string, struct TpcInfo>::iterator iter = XrdxCastor2OfsFS.mTpcMap.begin();
    time_t now = time(NULL);
    
    while (iter != XrdxCastor2OfsFS.mTpcMap.end())
    {
      if (now - iter->second.expire > 3600)
      {
        xcastor_info("expire tpc.key:%s", iter->first.c_str());
        XrdxCastor2OfsFS.mTpcMap.erase(iter++);
      }
      else 
      {
        ++iter;
      }
    }
  }

  IsClosed = true;
  XrdOucString spath = FName();

  if ( procRequest == kProcWrite ) {
    sync();
    XrdOucString newpath = spath;
    newpath.erase( ".__proc_update__" );
    XrdxCastor2OfsFS.rem( newpath.c_str(), error, 0, "" );
    int rc =  ::rename( spath.c_str(), newpath.c_str() ); 
    //    printf("rc is %d %s->%s\n",rc, spath.c_str(),newpath.c_str());
    XrdxCastor2OfsFS.rem( spath.c_str(), error, 0, "" );
    XrdxCastor2OfsFS.ReadAllProc();
    XrdxCastor2OfsFS.UpdateProc( newpath.c_str() );
    XrdOfsFile::close();
    return rc;
  } else {
    XrdxCastor2OfsFS.ReadFromProc( "trace" );
  }

  char ckSumbuf[32 + 1];
  sprintf( ckSumbuf, "%x", adler );
  char* ckSumalg = "ADLER32";
  XrdOucString newpath = "";
  newpath = envOpaque->Get( "castor2fs.pfn1" );

  if ( hasWrite ) {
    if ( XrdxCastor2OfsFS.doChecksumUpdates && ( !hasadler ) ) {
      xcastor::Timing checksumtiming( "ofsf::checksum" );
      TIMING("START", &checksumtiming);
      char chcksumbuf[64 * 1024];
      // We just quickly rescan the file and recompute the checksum
      adler = adler32( 0L, Z_NULL, 0 );
      adleroffset = 0;
      XrdSfsFileOffset checkoffset = 0;
      XrdSfsXferSize checksize = 0;
      XrdSfsFileOffset checklength = 0;

      while ( ( checksize = read( checkoffset, chcksumbuf, sizeof( chcksumbuf ) ) ) > 0 ) {
        adler = adler32( adler, ( const Bytef* )chcksumbuf, checksize );
        checklength += checksize;
        checkoffset += checksize;
      }

      TIMING("STOP", &checksumtiming);
      checksumtiming.Print();
      sprintf( ckSumbuf, "%x", adler );
      xcastor_debug("recalculated checksum [ length=%u bytes ] is=%s", checklength, ckSumbuf);
      hasadler = true;
    } else {
      if ( ( XrdxCastor2OfsFS.doChecksumStreaming || XrdxCastor2OfsFS.doChecksumUpdates ) && hasadler ) {
        sprintf( ckSumbuf, "%x", adler );
        xcastor_debug("streaming checksum=%s", ckSumbuf);
      }
    }

    if ( ( XrdxCastor2OfsFS.doChecksumStreaming || XrdxCastor2OfsFS.doChecksumUpdates ) && hasadler ) {
      // Only the real copy stream set's checksums, not the fake open
      if ( ( !IsThirdPartyStream ) || ( IsThirdPartyStream && IsThirdPartyStreamCopy ) ) {
        if ( setxattr( newpath.c_str(), "user.castor.checksum.type", ckSumalg, strlen( ckSumalg ), 0 ) ) {
          XrdxCastor2OfsFS.Emsg( "close", error, EIO, "set checksum type", "" );
          rc = SFS_ERROR;
          // Anyway this is not returned to the client
        } else {
          if ( setxattr( newpath.c_str(), "user.castor.checksum.value", ckSumbuf, strlen( ckSumbuf ), 0 ) ) {
            XrdxCastor2OfsFS.Emsg( "close", error, EIO, "set checksum", "" );
            rc = SFS_ERROR;
            // Anyway this is not returned to the client
          }
        }
      }
    } else {
      if ( ( !XrdxCastor2OfsFS.doChecksumUpdates ) && ( !hasadler ) ) {
        // Remove checksum - we don't check errors here
        removexattr( newpath.c_str(), "user.castor.checksum.type" );
        removexattr( newpath.c_str(), "user.castor.checksum.value" );
      }
    }
  }

  if ( XrdxCastor2OfsFS.ThirdPartyCopy ) {
    if ( IsThirdPartyStream ) {
      // Terminate the transfer to set the final state and wait for the termination
      rc = ThirdPartyTransferClose( envOpaque->Get( "xferuuid" ) );

      if ( !IsThirdPartyStreamCopy ) {
        // Remove mappings for non third party streams
        XrdxCastor2OfsFS.FileNameMapLock.Lock();
        XrdxCastor2OfsFS.FileNameMap.Del( castorlfn.c_str() );
        XrdxCastor2OfsFS.FileNameMapLock.UnLock();
        // Comment: if there is no mapping defined, no 3rd party copy can be run towards this file
      }
    }

    XrdOfsFile::close();
  }

  // Inform the StagerJob
  if ( !IsThirdPartyStreamCopy ) {
    if ( viaDestructor ) {
      // This means the file was not properly closed
      Unlink();
    }

    if ( StagerJob && ( !StagerJob->Close( true, hasWrite ) ) ) {
      // For the moment, we cannot send this back, but soon we will
      xcastor_debug("StagerJob close faied, got rc=%i and msg=%s", 
                    StagerJob->ErrCode, StagerJob->ErrMsg.c_str());
      XrdxCastor2OfsFS.Emsg( "close", error, StagerJob->ErrCode, StagerJob->ErrMsg.c_str(), "" );
      rc = SFS_ERROR;
    }
  }

  return rc;
}


//------------------------------------------------------------------------------
// XrdxCastor2OfsFile::VerifyChecksum
//------------------------------------------------------------------------------
bool
XrdxCastor2OfsFile::VerifyChecksum()
{
  bool rc = true;
  XrdOucString CalcChecksum;
  XrdOucString CalcChecksumAlgorithm;

  if ( ( DiskChecksum != "" ) &&
       ( DiskChecksumAlgorithm != "" ) &&
       verifyChecksum ) 
  {
    char ckSumbuf[32 + 1];
    sprintf( ckSumbuf, "%x", adler );
    CalcChecksum = ckSumbuf;
    CalcChecksumAlgorithm = "ADLER32";

    if ( CalcChecksum != DiskChecksum ) {
      XrdxCastor2OfsFS.Emsg( "VerifyChecksum", error, EIO, "verify checksum - checksum wrong!", "" );
      rc = false;
    }

    if ( CalcChecksumAlgorithm != DiskChecksumAlgorithm ) {
      XrdxCastor2OfsFS.Emsg( "VerifyChecksum", error, EIO, "verify checksum - checksum type wrong!", "" );
      rc = false;
    }

    if ( !rc ) {
      xcastor_err("error: checksum %s != %s with algorithms [ %s  <=> %s ]", 
                    CalcChecksum.c_str(), DiskChecksum.c_str(), 
                    CalcChecksumAlgorithm.c_str(), DiskChecksumAlgorithm.c_str());
    } else {
      xcastor_debug("checksum OK: %s", CalcChecksum.c_str());
    }
  }

  return rc;
}


//------------------------------------------------------------------------------
// Read
//------------------------------------------------------------------------------
int
XrdxCastor2OfsFile::read( XrdSfsFileOffset fileOffset,  // Preread only
                          XrdSfsXferSize   amount )
{
  int rc = XrdOfsFile::read( fileOffset, amount );
  hasadler = false;
  return rc;
}


//------------------------------------------------------------------------------
// Read
//------------------------------------------------------------------------------
XrdSfsXferSize
XrdxCastor2OfsFile::read( XrdSfsFileOffset fileOffset,
                          char*            buffer,
                          XrdSfsXferSize   buffer_size )
{
  // If we once got an adler checksum error, we fail all reads.
  if ( hasadlererror ) {
    return SFS_ERROR;
  }

  int rc = XrdOfsFile::read( fileOffset, buffer, buffer_size );

  // Computation of adler checksum - we disable it if seeks happen
  if ( fileOffset != adleroffset ) {
    hasadler = false;
  }

  if ( hasadler && XrdxCastor2OfsFS.doChecksumStreaming ) {
    adler = adler32( adler, ( const Bytef* ) buffer, rc );

    if ( rc > 0 )
      adleroffset += rc;
  }

  if ( hasadler && ( fileOffset + buffer_size >= statinfo.st_size ) ) {
    // Invoke the checksum verification
    if ( !VerifyChecksum() ) {
      hasadlererror = true;
      rc = SFS_ERROR;
    }
  }

  return rc;
}


//------------------------------------------------------------------------------
// Read
//------------------------------------------------------------------------------
int
XrdxCastor2OfsFile::read( XrdSfsAio* aioparm )
{
  int rc = XrdOfsFile::read( aioparm );
  hasadler = false;
  return rc;
}


//------------------------------------------------------------------------------
// Write
//------------------------------------------------------------------------------
XrdSfsXferSize
XrdxCastor2OfsFile::write( XrdSfsFileOffset fileOffset,
                           const char*      buffer,
                           XrdSfsXferSize   buffer_size )
{
  // If we are a transfer - check if we are still in running state,
  if ( Transfer ) {
    if ( Transfer->State != XrdTransfer::kRunning ) {
      // This transfer has been canceled, we have terminate
      XrdxCastor2OfsFS.Emsg( "write", error, ECANCELED, "write - the transfer has been canceled already ", 
                             XrdTransferStateAsString[Transfer->State] );

      // We truncate this file to 0!
      if ( !IsClosed ) {
        truncate( 0 );
        sync();
      }

      return SFS_ERROR;
    }
  }

  if ( StagerJob && StagerJob->Connected() ) {
    // If we have a stagerJob watching, check it is still alive
    if ( !StagerJob->StillConnected() ) {
      // oh oh, the stagerJob died ... reject any write now
      xcastor_err("error:write => StagerJob has been canceled");

      // We truncate this file to 0!
      if ( !IsClosed ) {
        truncate( 0 );
        sync();
      }

      if ( Transfer && ( Transfer->State == XrdTransfer::kRunning ) ) {
        // If we are a transfer we terminate the transfer and close the file ;-)
        Transfer->SetState( XrdTransfer::kTerminate );
        close();
      }

      XrdxCastor2OfsFS.Emsg( "write", error, ECANCELED, "write - the stagerjob write slot has been canceled ", "" );
      return SFS_ERROR;
    } else {
      xcastor_debug("my StagerJob is alive");
    }
  }

  int rc = XrdOfsFile::write( fileOffset, buffer, buffer_size );

  // Computation of adler checksum - we disable it if seek happened
  if ( fileOffset != adleroffset ) {
    hasadler = false;
  }

  if ( hasadler && XrdxCastor2OfsFS.doChecksumStreaming ) {
    adler = adler32( adler, ( const Bytef* ) buffer, buffer_size );

    if ( rc > 0 ) {
      adleroffset += rc;
    }
  }

  hasWrite = true;

  if ( firstWrite ) {
    if ( UpdateMeta() != SFS_OK ) {
      return SFS_ERROR;
    }

    firstWrite = false;
  }

  return rc;
}


//------------------------------------------------------------------------------
// Write
//------------------------------------------------------------------------------
int
XrdxCastor2OfsFile::write( XrdSfsAio* aioparm )
{
  // If we are a transfer - check if we are still in running state,
  if ( Transfer ) {
    if ( Transfer->State != XrdTransfer::kRunning ) {
      // This transfer has been canceled, we have terminated
      XrdxCastor2OfsFS.Emsg( "write", error, ECANCELED, "write - the transfer has been canceld already ", 
                             XrdTransferStateAsString[Transfer->State] );

      // We truncate this file to 0!
      if ( !IsClosed ) {
        truncate( 0 );
        sync();
      }

      return SFS_ERROR;
    }
  }

  if ( StagerJob && StagerJob->Connected() ) {
    if ( !StagerJob->StillConnected() ) {
      // oh oh, the stagerJob died ... reject any write now
      xcastor_err("error:write => StagerJob has been canceled");

      // We truncate this file to 0!
      if ( !IsClosed ) {
        truncate( 0 );
        sync();
      }

      if ( Transfer && ( Transfer->State == XrdTransfer::kRunning ) ) {
        // If we are a transfer we terminate the transfer and close the file ;-)
        Transfer->SetState( XrdTransfer::kTerminate );
        close();
      }

      XrdxCastor2OfsFS.Emsg( "write", error, ECANCELED, "write - the stagerjob write slot has been canceled ", "" );
      return SFS_ERROR;
    } else {
      xcastor_err("my StagerJob is alive");
    }
  }

  int rc = XrdOfsFile::write( aioparm );
  hasWrite = true;

  if ( firstWrite ) {
    if ( UpdateMeta() != SFS_OK ) {
      return SFS_ERROR;
    }

    firstWrite = false;
  }

  return rc;
}


//------------------------------------------------------------------------------
// Sync
//------------------------------------------------------------------------------
int
XrdxCastor2OfsFile::sync()
{
  return XrdOfsFile::sync();
}



//------------------------------------------------------------------------------
// Sync
//------------------------------------------------------------------------------
int
XrdxCastor2OfsFile::sync( XrdSfsAio* /*aiop*/ )
{
  return XrdOfsFile::sync();
}


//------------------------------------------------------------------------------
// Truncate
//------------------------------------------------------------------------------
int
XrdxCastor2OfsFile::truncate( XrdSfsFileOffset fileOffset )
{
  return XrdOfsFile::truncate( fileOffset );
}


//------------------------------------------------------------------------------
// Unlink
//------------------------------------------------------------------------------
int
XrdxCastor2OfsFile::Unlink()
{
  if ( ( !XrdxCastor2OfsFS.doPOSC ) || ( !isRW ) ) {
    return SFS_OK;
  }

  if ( IsThirdPartyStreamCopy ) {
    return SFS_OK;
  }

  XrdOucString preparename = envOpaque->Get( "castor2fs.sfn" );
  XrdOucString secuid      = envOpaque->Get( "castor2fs.client_sec_uid" );
  XrdOucString secgid      = envOpaque->Get( "castor2fs.client_sec_gid" );
  xcastor_debug("unlink lfn=%s", preparename.c_str());

  // Don't deal with proc requests
  if ( procRequest != kProcNone )
    return SFS_OK;

  XrdOucString mdsaddress = "root://";
  mdsaddress += envOpaque->Get( "castor2fs.manager" );
  mdsaddress += "//dummy";
  XrdCl::URL url(mdsaddress.c_str());
  XrdCl::Buffer* buffer = 0;
  std::vector<std::string> list;
  
  if (!url.IsValid()) {
    xcastor_debug("URL is not valid: %s", mdsaddress.c_str());
    return XrdxCastor2OfsFS.Emsg( "Unlink", error, ENOENT, "URL is not valid ", "" );
  }
  
  XrdCl::FileSystem* mdsclient = new XrdCl::FileSystem(url);

  if (!mdsclient) {
    xcastor_err("Could not create XrdCl::FileSystem object.");
    return XrdxCastor2OfsFS.Emsg( "Unlink", error, EHOSTUNREACH, 
                                  "connect to the mds host (normally the manager) ", "" );
  }

  preparename += "?";
  preparename += "&unlink=true";
  preparename += "&reqid=";
  preparename += reqid;
  preparename += "&stagehost=";
  preparename += stagehost;
  preparename += "&serviceclass=";
  preparename += serviceclass;
  preparename += "&uid=";
  preparename += secuid;
  preparename += "&gid=";
  preparename += secgid;

  list.push_back(preparename.c_str());
  xcastor_debug("Informing about %s", preparename.c_str());

  XrdxCastor2OfsFS.MetaMutex.Lock();
  XrdCl::XRootDStatus status = mdsclient->Prepare(list, XrdCl::PrepareFlags::Stage, 1, buffer); 

  if (!status.IsOK()) {
    xcastor_err("Prepare response invalid.");
    XrdxCastor2OfsFS.MetaMutex.UnLock();
    delete buffer;
    return XrdxCastor2OfsFS.Emsg("Unlink", error, ESHUTDOWN, "unlink ", FName());
  }

  XrdxCastor2OfsFS.MetaMutex.UnLock();
  delete buffer;
  return SFS_OK;
}


//------------------------------------------------------------------------------
// UpdateMeta
//------------------------------------------------------------------------------
int
XrdxCastor2OfsFile::UpdateMeta()
{
  if ( IsThirdPartyStreamCopy ) {
    return SFS_OK;
  }

  XrdOucString preparename = envOpaque->Get( "castor2fs.sfn" );
  xcastor_debug("haswrite=%i, firstWrite=%i, 3rd-party=%i/%i, lfn=%s",                
                hasWrite, firstWrite, IsThirdPartyStream, IsThirdPartyStreamCopy,
                preparename.c_str());

  // Don't deal with proc requests
  if ( procRequest != kProcNone )
    return SFS_OK;

  XrdOucString mdsaddress = "root://";
  mdsaddress += envOpaque->Get( "castor2fs.manager" );
  mdsaddress += "//dummy";
  XrdCl::URL url(mdsaddress.c_str());
  XrdCl::Buffer* buffer = 0;
  std::vector<std::string> list;
  
  if (!url.IsValid()) {
    xcastor_debug("URL is not valid: %s", mdsaddress.c_str());
    return XrdxCastor2OfsFS.Emsg( "UpdateMeta", error, ENOENT, "URL is not valid ", "" );
  }
  
  XrdCl::FileSystem* mdsclient = new XrdCl::FileSystem(url);

  if (!mdsclient) {
    xcastor_err("Could not create XrdCl::FileSystem object.");
    return XrdxCastor2OfsFS.Emsg( "UpdateMeta", error, EHOSTUNREACH, 
                                  "connect to the mds host (normally the manager) ", "" );
  }

  preparename += "?";

  if ( firstWrite && hasWrite )
    preparename += "&firstbytewritten=true";
  else {
    return SFS_OK;
  }

  preparename += "&reqid=";
  preparename += reqid;
  preparename += "&stagehost=";
  preparename += stagehost;
  preparename += "&serviceclass=";
  preparename += serviceclass;
  list.push_back(preparename.c_str());
  xcastor_debug("informing about %s", preparename.c_str());

  XrdxCastor2OfsFS.MetaMutex.Lock();
  XrdCl::XRootDStatus status = mdsclient->Prepare(list, XrdCl::PrepareFlags::Stage, 1, buffer); 
  
  if (!status.IsOK()) {
    xcastor_err("Prepare response invalid.");
    XrdxCastor2OfsFS.MetaMutex.UnLock();
    delete buffer;
    return XrdxCastor2OfsFS.Emsg( "Unlink", error, ESHUTDOWN, "update mds size/modtime", FName());
  }

  XrdxCastor2OfsFS.MetaMutex.UnLock();
  delete buffer;
  return SFS_OK;
}


/******************************************************************************/
/*              xCastorOfs -  StagerJob Interface                             */
/******************************************************************************/

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
XrdxCastor2Ofs2StagerJob::XrdxCastor2Ofs2StagerJob( const char* sjobuuid, int port ):
  LogId()
{
  Port     = port;
  SjobUuid = sjobuuid;
  Socket   = 0;
  // select timeout is 0
  to.tv_sec  = 0;
  to.tv_usec = 1;
  IsConnected = false;
  ErrCode = 0;
  ErrMsg = "undef";
}


//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
XrdxCastor2Ofs2StagerJob::~XrdxCastor2Ofs2StagerJob()
{
  if ( Socket ) {
    delete Socket;
    Socket = 0;
  }

  IsConnected = false;
}


//------------------------------------------------------------------------------
// StagerJob open
//------------------------------------------------------------------------------
bool
XrdxCastor2Ofs2StagerJob::Open()
{
  // If no port is specified then we don't inform anybody
  if ( !Port )
    return true;

  Socket = new XrdNetSocket();

  if ( ( Socket->Open( "localhost", Port, 0, 0 ) ) < 0 ) {
    xcastor_debug("can not open socket");
    return false;
  }

  Socket->setOpts( Socket->SockNum(), XRDNET_KEEPALIVE | XRDNET_DELAY );
  // Build IDENT message
  XrdOucString ident = "IDENT ";
  ident += SjobUuid.c_str();
  ident += "\n";
  // Send IDENT message
  int nwrite = write( Socket->SockNum(), ident.c_str(), ident.length() );

  if ( nwrite != ident.length() ) {
    xcastor_debug("Calling XrdxCastor2Ofs2StagerJob::Open return false");
    return false;
  }

  IsConnected = true;
  return true;
}


//------------------------------------------------------------------------------
// StagerJob still connected
//------------------------------------------------------------------------------
bool
XrdxCastor2Ofs2StagerJob::StillConnected()
{
  int sval;

  if ( !IsConnected )
    return false;

  if ( !Socket )
    return false;

  // Add the socket to the select set
  FD_ZERO( &check_set );
  FD_SET( Socket->SockNum(), &check_set );

  if ( ( sval = select( Socket->SockNum() + 1, &check_set, 0, 0, &to ) ) < 0 ) {
    return false;
  }

  if ( FD_ISSET( Socket->SockNum(), &check_set ) ) {
    return false;
  }

  return true;
}


//------------------------------------------------------------------------------
// StagerJob close
//------------------------------------------------------------------------------
bool
XrdxCastor2Ofs2StagerJob::Close( bool ok, bool haswrite )
{
  if ( !Port || !Socket || !IsConnected) {
    return true;
  }

  XrdOucString ident = "CLOSE ";

  if ( ok ) {
    ident += "0 ";
  } else {
    ident += "1 ";
  }

  if ( haswrite ) {
    ident += "1 \n";
  } else {
    ident += "0 \n";
  }

  // Send CLOSE message
  xcastor_debug("Trying to send CLOSE message to StagerJob");
  int nwrite = write( Socket->SockNum(), ident.c_str(), ident.length() );

  if ( nwrite != ident.length() ) {
    ErrCode = EIO;
    ErrMsg = "Couldn't write CLOSE request to StagerJob!";
    return false;
  } else {
    xcastor_debug("Sent CLOSE message to StagerJob");
  }

  // Read CLOSE response
  char closeresp[16384];
  int  respcode;
  char respmesg[16384];
  int nread = 0;
  xcastor_debug("Trying to read CLOSE response from StagerJob");
  nread = ::read( Socket->SockNum(), closeresp, sizeof( closeresp ) );
  xcastor_debug("This read gave %i bytes and errno=%i", nread, errno);

  if ( nread > 0 ) {
    xcastor_debug("Read CLOSE response from StagerJob");

    // ok, try to parse it
    if ( ( sscanf( closeresp, "%d %[^\n]", &respcode, respmesg ) ) == 2 ) {
      ErrMsg = respmesg;
      ErrCode = respcode;

      // Check the code
      if ( ErrCode != 0 ) {
        return false;
      }

      // yes, everything worked!
    } else {
      ErrCode = EINVAL;
      ErrMsg  = "StagerJob returned illegal response: ";
      ErrMsg += ( char* )closeresp;
      return false;
    }
  } else {
    // That was an error
    ErrCode = errno;
    ErrMsg = "Couldn't read response from StagerJob!";
    return false;
  }

  return true;
}
