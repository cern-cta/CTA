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
#include "scheduler/mockDB/MockSchedulerDatabase.hpp"
#include "scheduler/RetrieveFromTapeCopyRequest.hpp"
#include "common/archiveNS/StorageClass.hpp"
#include "scheduler/SchedulerDatabase.hpp"
#include "common/archiveNS/StorageClass.hpp"
#include "common/TapePool.hpp"
#include "XrdProFilesystem.hpp"
#include "XrdProFile.hpp"
#include "XrdOuc/XrdOucString.hh"
#include "XrdSec/XrdSecEntity.hh"
#include "XrdVersion.hh"
#include "objectstore/BackendVFS.hpp"
#include "objectstore/RootEntry.hpp"
#include "remotens/EosNS.hpp"

#include <memory>
#include <iostream>
#include <pwd.h>
#include <sstream>
#include <sys/types.h>

XrdVERSIONINFO(XrdSfsGetFileSystem,XrdPro)

cta::objectstore::BackendVFS g_backend;

class BackendPopulator {
public:
    BackendPopulator(cta::objectstore::Backend & be): m_backend(be),
      m_agent(m_backend) {
    // We need to populate the root entry before using.
    cta::objectstore::RootEntry re(m_backend);
    re.initialize();
    re.insert();
    cta::objectstore::ScopedExclusiveLock rel(re);
    re.fetch();
    m_agent.generateName("OStoreDBFactory");
    m_agent.initialize();
    cta::objectstore::CreationLog cl(cta::UserIdentity(1111, 1111), "systemhost", 
      time(NULL), "Initial creation of the  object store structures");
    re.addOrGetAgentRegisterPointerAndCommit(m_agent,cl);
    rel.release();
    m_agent.insertAndRegisterSelf();
    rel.lock(re);
    re.addOrGetDriveRegisterPointerAndCommit(m_agent, cl);
    rel.release();
  }
  
  virtual ~BackendPopulator() throw() {
    cta::objectstore::ScopedExclusiveLock agl(m_agent);
    m_agent.fetch();
    m_agent.removeAndUnregisterSelf();
  }
  
  cta::objectstore::Agent & getAgent() { return m_agent;  }
private:
  cta::objectstore::Backend & m_backend;
  cta::objectstore::Agent m_agent;
} g_backendPopulator(g_backend);

class OStoreDBWithAgent: public cta::OStoreDB {
public:
  OStoreDBWithAgent(cta::objectstore::Backend & be, cta::objectstore::Agent & ag):
  cta::OStoreDB(be) {
    cta::OStoreDB::setAgent(ag);
  }
  virtual ~OStoreDBWithAgent() throw () {
    cta::OStoreDB::setAgent(*((cta::objectstore::Agent *)NULL));
  }

} g_OStoreDB(g_backend, g_backendPopulator.getAgent());


cta::EosNS g_eosNs("localhost:1094");
cta::CastorNameServer g_castorNs;

extern "C"
{
  XrdSfsFileSystem *XrdSfsGetFileSystem (XrdSfsFileSystem* native_fs, XrdSysLogger* lp, const char* configfn)
  {
    g_eosNs.createEntry(cta::RemotePath("eos://eos/kruse/file1"), cta::RemoteFileStatus(cta::UserIdentity(getuid(), getgid()), 0777, 0));
    return new XrdProFilesystem(
      &g_castorNs,
      &g_OStoreDB,
      &g_eosNs);
  }
}

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
  return new XrdProFile(m_scheduler, user, MonID);
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
int XrdProFilesystem::stat(const char *Name, struct stat *buf, XrdOucErrInfo &eInfo, const XrdSecEntity *client,const char *opaque)
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
XrdProFilesystem::XrdProFilesystem(
  cta::NameServer *ns,
  cta::SchedulerDatabase *scheddb,
  cta::RemoteNS *remoteStorage):
  m_ns(ns),
  m_scheddb(scheddb),
  m_remoteStorage(remoteStorage),
  m_scheduler(new cta::Scheduler(*ns, *scheddb, *remoteStorage)) {
  const cta::SecurityIdentity bootstrap_requester;
  m_scheduler->createAdminUserWithoutAuthorizingRequester(bootstrap_requester,
    cta::UserIdentity(getuid(),getgid()), "Bootstrap operator");
  m_scheduler->createAdminHostWithoutAuthorizingRequester(bootstrap_requester,
    "localhost", "Bootstrap operator host");
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
XrdProFilesystem::~XrdProFilesystem() {
  delete m_scheduler;
  delete m_ns;
}
