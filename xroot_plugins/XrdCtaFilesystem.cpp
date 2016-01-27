/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "castor/common/CastorConfiguration.hpp"
#include "common/exception/Exception.hpp"
#include "nameserver/CastorNameServer.hpp"
#include "nameserver/mockNS/MockNameServer.hpp"
#include "nameserver/NameServer.hpp"
#include "remotens/MockRemoteNS.hpp"
#include "common/admin/AdminHost.hpp"
#include "common/admin/AdminUser.hpp"
#include "common/archiveRoutes/ArchiveRoute.hpp"
#include "scheduler/ArchiveToTapeCopyRequest.hpp"
#include "scheduler/LogicalLibrary.hpp"
#include "scheduler/OStoreDB/OStoreDB.hpp"
#include "scheduler/RetrieveRequestDump.hpp"
#include "common/archiveNS/StorageClass.hpp"
#include "scheduler/SchedulerDatabase.hpp"
#include "common/archiveNS/StorageClass.hpp"
#include "common/TapePool.hpp"
#include "XrdCtaFilesystem.hpp"
#include "XrdCtaFile.hpp"
#include "XrdOuc/XrdOucString.hh"
#include "XrdSec/XrdSecEntity.hh"
#include "XrdVersion.hh"
#include "objectstore/RootEntry.hpp"
#include "objectstore/BackendFactory.hpp"

#include <memory>
#include <iostream>
#include <pwd.h>
#include <sstream>
#include <sys/types.h>

XrdVERSIONINFO(XrdSfsGetFileSystem,XrdPro)

extern "C"
{
  XrdSfsFileSystem *XrdSfsGetFileSystem (XrdSfsFileSystem* native_fs, XrdSysLogger* lp, const char* configfn)
  {
    try {
      return new cta::xrootPlugins::XrdProFilesystem();
    } catch (cta::exception::Exception &ex) {
      std::cout << "[ERROR] Could not load the CTA xroot plugin. CTA exception caught: " << ex.getMessageValue() << "\n";
      return NULL;
    } catch (std::exception &ex) {
      std::cout << "[ERROR] Could not load the CTA xroot plugin. Exception caught: " << ex.what() << "\n";
      return NULL;
    } catch (...) {
      std::cout << "[ERROR] Could not load the CTA xroot plugin. Unknown exception caught!" << "\n";
      return NULL;
    }
  }
}

namespace cta { namespace xrootPlugins {

//------------------------------------------------------------------------------
// FSctl
//------------------------------------------------------------------------------
int XrdProFilesystem::FSctl(const int cmd, XrdSfsFSctl &args, XrdOucErrInfo &eInfo, const XrdSecEntity *client)
{
  (void)cmd; (void)args; (void)eInfo; (void)client;
  eInfo.setErrInfo(ENOTSUP, "Not supported.");
  return SFS_ERROR;
}

//------------------------------------------------------------------------------
// newFile
//------------------------------------------------------------------------------
XrdSfsFile * XrdProFilesystem::newFile(char *user, int MonID)
{  
  return new cta::xrootPlugins::XrdProFile(&m_scheduler, user, MonID);
}

//------------------------------------------------------------------------------
// newDir
//------------------------------------------------------------------------------
XrdSfsDirectory * XrdProFilesystem::newDir(char *user, int MonID)
{
  (void)user; (void)MonID;
  return NULL;
}

//------------------------------------------------------------------------------
// fsctl
//------------------------------------------------------------------------------
int XrdProFilesystem::fsctl(const int cmd, const char *args, XrdOucErrInfo &eInfo, const XrdSecEntity *client)
{
  (void)cmd; (void)args; (void)eInfo; (void)client;
  eInfo.setErrInfo(ENOTSUP, "Not supported.");
  return SFS_ERROR;
}

//------------------------------------------------------------------------------
// getStats
//------------------------------------------------------------------------------
int XrdProFilesystem::getStats(char *buff, int blen)
{
  (void)buff; (void)blen;
  return SFS_OK;
}

//------------------------------------------------------------------------------
// getVersion
//------------------------------------------------------------------------------
const char * XrdProFilesystem::getVersion()
{
  return NULL;
}

//------------------------------------------------------------------------------
// exists
//------------------------------------------------------------------------------
int XrdProFilesystem::exists(const char *path, XrdSfsFileExistence &eFlag, XrdOucErrInfo &eInfo, const XrdSecEntity *client, const char *opaque)
{
  (void)path; (void)eFlag; (void)eInfo; (void)client; (void)opaque;
  eInfo.setErrInfo(ENOTSUP, "Not supported.");
  return SFS_ERROR;
}

//------------------------------------------------------------------------------
// mkdir
//------------------------------------------------------------------------------
int XrdProFilesystem::mkdir(const char *path, XrdSfsMode mode, XrdOucErrInfo &eInfo, const XrdSecEntity *client, const char *opaque)
{
  (void)path; (void)mode; (void)eInfo; (void)client; (void)opaque;
  eInfo.setErrInfo(ENOTSUP, "Not supported.");
  return SFS_ERROR;
}

//------------------------------------------------------------------------------
// prepare
//------------------------------------------------------------------------------
int XrdProFilesystem::prepare(XrdSfsPrep &pargs, XrdOucErrInfo &eInfo, const XrdSecEntity *client)
{
  (void)pargs; (void)eInfo; (void)client;
  eInfo.setErrInfo(ENOTSUP, "Not supported.");
  return SFS_ERROR;
}

//------------------------------------------------------------------------------
// rem
//------------------------------------------------------------------------------
int XrdProFilesystem::rem(const char *path, XrdOucErrInfo &eInfo, const XrdSecEntity *client, const char *opaque)
{
  (void)path; (void)eInfo; (void)client; (void)opaque;
  eInfo.setErrInfo(ENOTSUP, "Not supported.");
  return SFS_ERROR;
}

//------------------------------------------------------------------------------
// remdir
//------------------------------------------------------------------------------
int XrdProFilesystem::remdir(const char *path, XrdOucErrInfo &eInfo, const XrdSecEntity *client, const char *opaque)
{
  (void)path; (void)eInfo; (void)client; (void)opaque;
  eInfo.setErrInfo(ENOTSUP, "Not supported.");
  return SFS_ERROR;
}

//------------------------------------------------------------------------------
// rename
//------------------------------------------------------------------------------
int XrdProFilesystem::rename(const char *oPath, const char *nPath, XrdOucErrInfo &eInfo, const XrdSecEntity *client, const char *opaqueO, const char *opaqueN)
{
  (void)oPath; (void)nPath; (void)eInfo; (void)client; (void)opaqueO; (void)opaqueN;
  eInfo.setErrInfo(ENOTSUP, "Not supported.");
  return SFS_ERROR;
}

//------------------------------------------------------------------------------
// stat
//------------------------------------------------------------------------------
int XrdProFilesystem::stat(const char *Name, struct ::stat *buf, XrdOucErrInfo &eInfo, const XrdSecEntity *client,const char *opaque)
{
  (void)Name; (void)buf; (void)eInfo; (void)client; (void)opaque;
  eInfo.setErrInfo(ENOTSUP, "Not supported.");
  return SFS_ERROR;
}

//------------------------------------------------------------------------------
// stat
//------------------------------------------------------------------------------
int XrdProFilesystem::stat(const char *path, mode_t &mode, XrdOucErrInfo &eInfo, const XrdSecEntity *client, const char *opaque)
{
  (void)path; (void)mode; (void)eInfo; (void)client; (void)opaque;
  eInfo.setErrInfo(ENOTSUP, "Not supported.");
  return SFS_ERROR;
}

//------------------------------------------------------------------------------
// truncate
//------------------------------------------------------------------------------
int XrdProFilesystem::truncate(const char *path, XrdSfsFileOffset fsize, XrdOucErrInfo &eInfo, const XrdSecEntity *client, const char *opaque)
{
  (void)path; (void)fsize; (void)eInfo; (void)client; (void)opaque;
  eInfo.setErrInfo(ENOTSUP, "Not supported.");
  return SFS_ERROR;
}

//------------------------------------------------------------------------------
// chksum
//------------------------------------------------------------------------------
int XrdProFilesystem::chksum(csFunc Func, const char *csName, const char *path, XrdOucErrInfo &eInfo, const XrdSecEntity *client,const char *opaque)
{
  (void)Func; (void)csName; (void)path; (void)eInfo; (void)client; (void)opaque;
  eInfo.setErrInfo(ENOTSUP, "Not supported.");
  return SFS_ERROR;
}

//------------------------------------------------------------------------------
// chmod
//------------------------------------------------------------------------------
int XrdProFilesystem::chmod(
  const char *path,
  XrdSfsMode mode,
  XrdOucErrInfo &eInfo,
  const XrdSecEntity *client,
  const char *opaque) {
  (void)path; (void)mode; (void)eInfo; (void)client; (void)opaque;
  eInfo.setErrInfo(ENOTSUP, "Not supported.");
  return SFS_ERROR;
}

//------------------------------------------------------------------------------
// Disc
//------------------------------------------------------------------------------
void XrdProFilesystem::Disc(const XrdSecEntity *client)
{
  (void)client;
}

//------------------------------------------------------------------------------
// EnvInfo
//------------------------------------------------------------------------------
void XrdProFilesystem::EnvInfo(XrdOucEnv *envP)
{
  (void)envP;
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
XrdProFilesystem::XrdProFilesystem():
  m_ns(castor::common::CastorConfiguration::getConfig().getConfEntString("TapeServer", "MockNameServerPath")),
  m_remoteStorage(castor::common::CastorConfiguration::getConfig().getConfEntString("TapeServer", "EOSRemoteHostAndPort")), 
  m_backend(cta::objectstore::BackendFactory::createBackend(
    castor::common::CastorConfiguration::getConfig().getConfEntString("TapeServer", "ObjectStoreBackendPath"))
      .release()),
  m_backendPopulator(*m_backend),
  m_scheddb(*m_backend, m_backendPopulator.getAgent()),
  m_scheduler(m_ns, m_scheddb, m_remoteStorage)
{  
  // If the backend is a VFS, make sure we don't delete it on exit.
  // If not, nevermind.
  try {
    dynamic_cast<cta::objectstore::BackendVFS &>(*m_backend).noDeleteOnExit();
  } catch (std::bad_cast &){}
  const cta::SecurityIdentity bootstrap_requester;
  try {
    m_scheduler.createAdminUserWithoutAuthorizingRequester(bootstrap_requester, cta::UserIdentity(44446,1028), "Bootstrap operator");
  } catch (cta::exception::Exception &ex) {
    std::cout << "[WARNING] Could not create the admin user and admin host. CTA exception caught: " << ex.getMessageValue() << "\n";
  } catch (std::exception &ex) {
    std::cout << "[WARNING] Could not create the admin user and admin host. Exception caught: " << ex.what() << "\n";
  } catch (...) {
    std::cout << "[WARNING] Could not create the admin user and admin host. Unknown exception caught!" << "\n";
  }
  try {
    m_scheduler.createAdminUserWithoutAuthorizingRequester(bootstrap_requester, cta::UserIdentity(getuid(),getgid()), "Bootstrap operator");
  } catch (cta::exception::Exception &ex) {
    std::cout << "[WARNING] Could not create the admin user and admin host. CTA exception caught: " << ex.getMessageValue() << "\n";
  } catch (std::exception &ex) {
    std::cout << "[WARNING] Could not create the admin user and admin host. Exception caught: " << ex.what() << "\n";
  } catch (...) {
    std::cout << "[WARNING] Could not create the admin user and admin host. Unknown exception caught!" << "\n";
  }
  try {
    m_scheduler.createAdminHostWithoutAuthorizingRequester(bootstrap_requester, "tpsrv045.cern.ch", "Bootstrap operator host");
  } catch (cta::exception::Exception &ex) {
    std::cout << "[WARNING] Could not create the admin user and admin host. CTA exception caught: " << ex.getMessageValue() << "\n";
  } catch (std::exception &ex) {
    std::cout << "[WARNING] Could not create the admin user and admin host. Exception caught: " << ex.what() << "\n";
  } catch (...) {
    std::cout << "[WARNING] Could not create the admin user and admin host. Unknown exception caught!" << "\n";
  }
  try {
    m_scheduler.createAdminHostWithoutAuthorizingRequester(bootstrap_requester, "localhost", "Bootstrap operator host");
  } catch (cta::exception::Exception &ex) {
    std::cout << "[WARNING] Could not create the admin user and admin host. CTA exception caught: " << ex.getMessageValue() << "\n";
  } catch (std::exception &ex) {
    std::cout << "[WARNING] Could not create the admin user and admin host. Exception caught: " << ex.what() << "\n";
  } catch (...) {
    std::cout << "[WARNING] Could not create the admin user and admin host. Unknown exception caught!" << "\n";
  }
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
XrdProFilesystem::~XrdProFilesystem() {
}

}}
