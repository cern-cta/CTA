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
#include "catalogue/CatalogueFactory.hpp"
#include "common/admin/AdminHost.hpp"
#include "common/admin/AdminUser.hpp"
#include "common/archiveRoutes/ArchiveRoute.hpp"
#include "common/archiveNS/StorageClass.hpp"
#include "common/Configuration.hpp"
#include "common/exception/Exception.hpp"
#include "common/TapePool.hpp"
#include "nameserver/mockNS/MockNameServer.hpp"
#include "nameserver/NameServer.hpp"
#include "objectstore/RootEntry.hpp"
#include "objectstore/BackendFactory.hpp"
#include "remotens/MockRemoteNS.hpp"
#include "scheduler/LogicalLibrary.hpp"
#include "scheduler/OStoreDB/OStoreDB.hpp"
#include "scheduler/RetrieveRequestDump.hpp"
#include "scheduler/SchedulerDatabase.hpp"
#include "XrdOuc/XrdOucString.hh"
#include "XrdSec/XrdSecEntity.hh"
#include "XrdVersion.hh"
#include "xroot_plugins/XrdCtaFilesystem.hpp"
#include "xroot_plugins/XrdCtaFile.hpp"

#include <memory>
#include <iostream>
#include <pwd.h>
#include <sstream>
#include <sys/types.h>

XrdVERSIONINFO(XrdSfsGetFileSystem,XrdCta)

extern "C"
{
  XrdSfsFileSystem *XrdSfsGetFileSystem (XrdSfsFileSystem* native_fs, XrdSysLogger* lp, const char* configfn)
  {
    try {
      return new cta::xrootPlugins::XrdCtaFilesystem();
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
int XrdCtaFilesystem::FSctl(const int cmd, XrdSfsFSctl &args, XrdOucErrInfo &eInfo, const XrdSecEntity *client)
{
  (void)cmd; (void)args; (void)eInfo; (void)client;
  eInfo.setErrInfo(ENOTSUP, "Not supported.");
  return SFS_ERROR;
}

//------------------------------------------------------------------------------
// newFile
//------------------------------------------------------------------------------
XrdSfsFile * XrdCtaFilesystem::newFile(char *user, int MonID)
{  
  return new cta::xrootPlugins::XrdCtaFile(m_catalogue.get(), m_scheduler.get(), user, MonID);
}

//------------------------------------------------------------------------------
// newDir
//------------------------------------------------------------------------------
XrdSfsDirectory * XrdCtaFilesystem::newDir(char *user, int MonID)
{
  (void)user; (void)MonID;
  return NULL;
}

//------------------------------------------------------------------------------
// fsctl
//------------------------------------------------------------------------------
int XrdCtaFilesystem::fsctl(const int cmd, const char *args, XrdOucErrInfo &eInfo, const XrdSecEntity *client)
{
  (void)cmd; (void)args; (void)eInfo; (void)client;
  eInfo.setErrInfo(ENOTSUP, "Not supported.");
  return SFS_ERROR;
}

//------------------------------------------------------------------------------
// getStats
//------------------------------------------------------------------------------
int XrdCtaFilesystem::getStats(char *buff, int blen)
{
  (void)buff; (void)blen;
  return SFS_OK;
}

//------------------------------------------------------------------------------
// getVersion
//------------------------------------------------------------------------------
const char * XrdCtaFilesystem::getVersion()
{
  return NULL;
}

//------------------------------------------------------------------------------
// exists
//------------------------------------------------------------------------------
int XrdCtaFilesystem::exists(const char *path, XrdSfsFileExistence &eFlag, XrdOucErrInfo &eInfo, const XrdSecEntity *client, const char *opaque)
{
  (void)path; (void)eFlag; (void)eInfo; (void)client; (void)opaque;
  eInfo.setErrInfo(ENOTSUP, "Not supported.");
  return SFS_ERROR;
}

//------------------------------------------------------------------------------
// mkdir
//------------------------------------------------------------------------------
int XrdCtaFilesystem::mkdir(const char *path, XrdSfsMode mode, XrdOucErrInfo &eInfo, const XrdSecEntity *client, const char *opaque)
{
  (void)path; (void)mode; (void)eInfo; (void)client; (void)opaque;
  eInfo.setErrInfo(ENOTSUP, "Not supported.");
  return SFS_ERROR;
}

//------------------------------------------------------------------------------
// prepare
//------------------------------------------------------------------------------
int XrdCtaFilesystem::prepare(XrdSfsPrep &pargs, XrdOucErrInfo &eInfo, const XrdSecEntity *client)
{
  (void)pargs; (void)eInfo; (void)client;
  eInfo.setErrInfo(ENOTSUP, "Not supported.");
  return SFS_ERROR;
}

//------------------------------------------------------------------------------
// rem
//------------------------------------------------------------------------------
int XrdCtaFilesystem::rem(const char *path, XrdOucErrInfo &eInfo, const XrdSecEntity *client, const char *opaque)
{
  (void)path; (void)eInfo; (void)client; (void)opaque;
  eInfo.setErrInfo(ENOTSUP, "Not supported.");
  return SFS_ERROR;
}

//------------------------------------------------------------------------------
// remdir
//------------------------------------------------------------------------------
int XrdCtaFilesystem::remdir(const char *path, XrdOucErrInfo &eInfo, const XrdSecEntity *client, const char *opaque)
{
  (void)path; (void)eInfo; (void)client; (void)opaque;
  eInfo.setErrInfo(ENOTSUP, "Not supported.");
  return SFS_ERROR;
}

//------------------------------------------------------------------------------
// rename
//------------------------------------------------------------------------------
int XrdCtaFilesystem::rename(const char *oPath, const char *nPath, XrdOucErrInfo &eInfo, const XrdSecEntity *client, const char *opaqueO, const char *opaqueN)
{
  (void)oPath; (void)nPath; (void)eInfo; (void)client; (void)opaqueO; (void)opaqueN;
  eInfo.setErrInfo(ENOTSUP, "Not supported.");
  return SFS_ERROR;
}

//------------------------------------------------------------------------------
// stat
//------------------------------------------------------------------------------
int XrdCtaFilesystem::stat(const char *Name, struct ::stat *buf, XrdOucErrInfo &eInfo, const XrdSecEntity *client,const char *opaque)
{
  (void)Name; (void)buf; (void)eInfo; (void)client; (void)opaque;
  eInfo.setErrInfo(ENOTSUP, "Not supported.");
  return SFS_ERROR;
}

//------------------------------------------------------------------------------
// stat
//------------------------------------------------------------------------------
int XrdCtaFilesystem::stat(const char *path, mode_t &mode, XrdOucErrInfo &eInfo, const XrdSecEntity *client, const char *opaque)
{
  (void)path; (void)mode; (void)eInfo; (void)client; (void)opaque;
  eInfo.setErrInfo(ENOTSUP, "Not supported.");
  return SFS_ERROR;
}

//------------------------------------------------------------------------------
// truncate
//------------------------------------------------------------------------------
int XrdCtaFilesystem::truncate(const char *path, XrdSfsFileOffset fsize, XrdOucErrInfo &eInfo, const XrdSecEntity *client, const char *opaque)
{
  (void)path; (void)fsize; (void)eInfo; (void)client; (void)opaque;
  eInfo.setErrInfo(ENOTSUP, "Not supported.");
  return SFS_ERROR;
}

//------------------------------------------------------------------------------
// chksum
//------------------------------------------------------------------------------
int XrdCtaFilesystem::chksum(csFunc Func, const char *csName, const char *path, XrdOucErrInfo &eInfo, const XrdSecEntity *client,const char *opaque)
{
  (void)Func; (void)csName; (void)path; (void)eInfo; (void)client; (void)opaque;
  eInfo.setErrInfo(ENOTSUP, "Not supported.");
  return SFS_ERROR;
}

//------------------------------------------------------------------------------
// chmod
//------------------------------------------------------------------------------
int XrdCtaFilesystem::chmod(
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
void XrdCtaFilesystem::Disc(const XrdSecEntity *client)
{
  (void)client;
}

//------------------------------------------------------------------------------
// EnvInfo
//------------------------------------------------------------------------------
void XrdCtaFilesystem::EnvInfo(XrdOucEnv *envP)
{
  (void)envP;
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
XrdCtaFilesystem::XrdCtaFilesystem():
  m_ctaConf("/etc/cta/cta-frontend.conf"),
  m_backend(cta::objectstore::BackendFactory::createBackend(m_ctaConf.getConfEntString("ObjectStore", "BackendPath", NULL)).release()),
  m_backendPopulator(*m_backend),
  m_scheddb(*m_backend, m_backendPopulator.getAgent()) {
  using namespace cta;
  const catalogue::DbLogin catalogueLogin = catalogue::DbLogin::parseFile("/etc/cta/cta_catalogue_db.conf");
  m_catalogue.reset(catalogue::CatalogueFactory::create(catalogueLogin));

  m_scheduler.reset(new cta::Scheduler(*m_catalogue, m_scheddb, 5, 2*1000*1000));

  // If the backend is a VFS, make sure we don't delete it on exit.
  // If not, nevermind.
  try {
    dynamic_cast<objectstore::BackendVFS &>(*m_backend).noDeleteOnExit();
  } catch (std::bad_cast &){}
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
XrdCtaFilesystem::~XrdCtaFilesystem() {
}

}}
