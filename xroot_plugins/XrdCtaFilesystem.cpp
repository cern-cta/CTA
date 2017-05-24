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
#include "common/log/StdoutLogger.hpp"
#include "common/log/StringLogger.hpp"
#include "common/log/FileLogger.hpp"
#include "common/archiveRoutes/ArchiveRoute.hpp"
#include "common/Configuration.hpp"
#include "common/exception/Exception.hpp"
#include "common/TapePool.hpp"
#include "eos/messages/eos_messages.pb.h"
#include "objectstore/RootEntry.hpp"
#include "objectstore/BackendFactory.hpp"
#include "scheduler/LogicalLibrary.hpp"
#include "scheduler/OStoreDB/OStoreDB.hpp"
#include "scheduler/RetrieveRequestDump.hpp"
#include "scheduler/SchedulerDatabase.hpp"
#include "XrdOuc/XrdOucString.hh"
#include "XrdSec/XrdSecEntity.hh"
#include "XrdVersion.hh"
#include "xroot_plugins/XrdCtaFilesystem.hpp"
#include "xroot_plugins/XrdCtaFile.hpp"
#include "XrdCtaDir.hpp"
#include "version.h"

#include <memory>
#include <iostream>
#include <pwd.h>
#include <stdlib.h>
#include <sstream>
#include <string.h>
#include <sys/types.h>

XrdVERSIONINFO(XrdSfsGetFileSystem,XrdCta)

extern "C"
{
  XrdSfsFileSystem *XrdSfsGetFileSystem (XrdSfsFileSystem* native_fs, XrdSysLogger* lp, const char* configfn)
  {
    try {
      return new cta::xroot_plugins::XrdCtaFilesystem();
    } catch (cta::exception::Exception &ex) {
      std::cerr << "[ERROR] Could not load the CTA xroot plugin. CTA exception caught: " << ex.what() << "\n";
      return nullptr;
    } catch (std::exception &ex) {
      std::cerr << "[ERROR] Could not load the CTA xroot plugin. Exception caught: " << ex.what() << "\n";
      return nullptr;
    } catch (...) {
      std::cerr << "[ERROR] Could not load the CTA xroot plugin. Unknown exception caught!" << "\n";
      return nullptr;
    }
  }
}

namespace cta { namespace xroot_plugins {

//------------------------------------------------------------------------------
// FSctl
//------------------------------------------------------------------------------
int XrdCtaFilesystem::FSctl(const int cmd, XrdSfsFSctl &args, XrdOucErrInfo &eInfo, const XrdSecEntity *client) {
  std::ostringstream errMsg;

  try {
    if (SFS_FSCTL_PLUGIO != cmd) {
      eInfo.setErrInfo(ENOTSUP, "Not supported: cmd != SFS_FSCTL_PLUGIO");
      return SFS_ERROR;
    }

    {
      std::list<cta::log::Param> params;
      params.push_back({"args.Arg1Len", args.Arg1Len});
      params.push_back({"args.Arg2Len", args.Arg2Len});
      params.push_back({"client->host", client->host});
      params.push_back({"client->name", client->name});
      (*m_log)(log::INFO, "FSctl called", params);
    }

    if (args.Arg1 == nullptr || args.Arg1Len == 0) {
      throw cta::exception::Exception("Did not receive a query argument");
    }

    const std::string msgBuffer(args.Arg1, args.Arg1Len);
    eos::wfe::Wrapper msg;
    if (!msg.ParseFromString(msgBuffer)) {
      throw cta::exception::Exception("Failed to parse incoming wrapper message");
    }

    auto reply = processWrapperMsg(msg, client);

    const int replySize = reply->DataLen();
    eInfo.setErrInfo(replySize, reply.release());
    return SFS_DATA;
  } catch(cta::exception::Exception &ex) {
    errMsg << __FUNCTION__ << " failed: " << ex.getMessage().str();
  } catch(std::exception &se) {
    errMsg << __FUNCTION__ << " failed: " << se.what();
  } catch(...) {
    errMsg << __FUNCTION__ << " failed: Caught an unknown exception";
  }

  // Reaching this point means an exception was thrown and errMsg has been set
  try {
    {
      std::list<cta::log::Param> params;
      params.push_back({"errMsg", errMsg.str()});
      (*m_log)(log::ERR, "FSctl encountered an unexpected exception", params);
    }
    eos::wfe::Wrapper wrapper;
    wrapper.set_type(eos::wfe::Wrapper::ERROR);
    eos::wfe::Error *const error = wrapper.mutable_error();
    error->set_audience(eos::wfe::Error::EOSLOG);
    error->set_code(ECANCELED);
    error->set_message(errMsg.str());

    std::string replyString = wrapper.SerializeAsString();
    auto reply = make_UniqueXrdOucBuffer(replyString.size(), replyString.c_str());

    const int replySize = reply->DataLen();
    eInfo.setErrInfo(replySize, reply.release());
    return SFS_DATA;
  } catch(...) {
    eInfo.setErrInfo(ECANCELED, "Failed to create reply eos::wfe::Error message");
    return SFS_ERROR;
  }
}

//------------------------------------------------------------------------------
// processWrapperMsg
//------------------------------------------------------------------------------
XrdCtaFilesystem::UniqueXrdOucBuffer XrdCtaFilesystem::processWrapperMsg(const eos::wfe::Wrapper &msg,
  const XrdSecEntity *const client) {
  switch(msg.type()) {
  case eos::wfe::Wrapper::NONE:
    throw cta::exception::Exception("Cannot process a wrapped message of type NONE");
  case eos::wfe::Wrapper::NOTIFICATION:
    return processNotificationMsg(msg.notification(), client);
  default:
    {
      cta::exception::Exception ex;
      ex.getMessage() << "Cannot process a wrapped message with a numeric type value of " << msg.type();
      throw ex;
    }
  }
}

//------------------------------------------------------------------------------
// processNotificationMsg
//------------------------------------------------------------------------------
XrdCtaFilesystem::UniqueXrdOucBuffer XrdCtaFilesystem::processNotificationMsg(const eos::wfe::Notification &msg,
  const XrdSecEntity *const client) {
  {
    std::list<cta::log::Param> params;
    params.push_back({"wf.event", msg.wf().event()});
    params.push_back({"wf.queue",  msg.wf().queue()});
    params.push_back({"wf.wfname", msg.wf().wfname()});
    params.push_back({"fid", msg.file().fid()});
    params.push_back({"path", msg.file().lpath()});
    (*m_log)(log::INFO, "Processing notification message", params);
  }

  switch(msg.wf().event()) {
  case eos::wfe::Workflow::NONE:
    throw cta::exception::Exception("Cannot process a NONE workflow event");
  case eos::wfe::Workflow::OPENR:
    throw cta::exception::Exception("Cannot process a OPENR workflow event");
  case eos::wfe::Workflow::OPENW:
    throw cta::exception::Exception("Cannot process a OPENW workflow event");
  case eos::wfe::Workflow::CLOSER:
    throw cta::exception::Exception("Cannot process a CLOSER workflow event");
  case eos::wfe::Workflow::CLOSEW:
    return processCLOSEW(msg, client);
  case eos::wfe::Workflow::DELETE:
    throw cta::exception::Exception("Cannot process a DELETE workflow event");
  case eos::wfe::Workflow::PREPARE:
    return processPREPARE(msg, client);
  default:
    {
      cta::exception::Exception ex;
      ex.getMessage() << "Workflow events with numeric value " << msg.wf().event() << " are not supported";
      throw ex;
    }
  }
}

//------------------------------------------------------------------------------
// processCLOSEW
//------------------------------------------------------------------------------
XrdCtaFilesystem::UniqueXrdOucBuffer XrdCtaFilesystem::processCLOSEW(const eos::wfe::Notification &msg,
  const XrdSecEntity *const client) {
  if(msg.wf().wfname() == "default") {
    return processDefaultCLOSEW(msg, client);
  } else {
    cta::exception::Exception ex;
    ex.getMessage() << "Cannot process a CLOSEW event for a " << msg.wf().wfname() << " workflow";
    throw ex;
  }
}

//------------------------------------------------------------------------------
// processDefaultCLOSEW
//------------------------------------------------------------------------------
XrdCtaFilesystem::UniqueXrdOucBuffer XrdCtaFilesystem::processDefaultCLOSEW(const eos::wfe::Notification &msg,
const XrdSecEntity *const client) {
  cta::common::dataStructures::DiskFileInfo diskFileInfo;
  diskFileInfo.recoveryBlob = toJson(msg);
  diskFileInfo.group = msg.file().owner().groupname();
  diskFileInfo.owner = msg.file().owner().username();
  diskFileInfo.path = msg.file().lpath();

  cta::common::dataStructures::UserIdentity requester;
  requester.name = msg.cli().user().username();
  requester.group = msg.cli().user().groupname();

  std::ostringstream archiveReportURL;
  archiveReportURL << "eosQuery://" << msg.wf().instance().name() << "//eos/wfe/passwd?mgm.pcmd=event&mgm.fid=" <<
  std::hex << msg.file().fid() <<
    "&mgm.logid=cta&mgm.event=archived&mgm.workflow=default&mgm.path=/eos/wfe/passwd&mgm.ruid=0&mgm.rgid=0";

  cta::common::dataStructures::ArchiveRequest request;
  request.checksumType = msg.file().cks().name();
  request.checksumValue = msg.file().cks().value();
  request.diskFileInfo = diskFileInfo;
  request.diskFileID = msg.file().fid();
  request.fileSize = msg.file().size();
  request.requester = requester;
  request.srcURL = msg.turl();
  request.storageClass = getDirXattr("CTA_StorageClass", msg.directory());
  request.archiveReportURL = archiveReportURL.str();

  const std::string diskInstance = msg.wf().instance().name();
  log::LogContext lc(*m_log);
  const uint64_t archiveFileId = m_scheduler->queueArchive(diskInstance, request, lc);

  eos::wfe::Wrapper wrapper;
  wrapper.set_type(eos::wfe::Wrapper::XATTR);
  eos::wfe::Xattr *const xattr = wrapper.mutable_xattr();
  xattr->set_fid(msg.file().fid());
  xattr->set_op(eos::wfe::Xattr::ADD);
  (*xattr->mutable_xattrs())["sys.archiveFileId"] = std::to_string(archiveFileId);

  std::string replyString = wrapper.SerializeAsString();
  return make_UniqueXrdOucBuffer(replyString.size(), replyString.c_str());
}

//------------------------------------------------------------------------------
// getDirXattr
//------------------------------------------------------------------------------
std::string XrdCtaFilesystem::getDirXattr(const std::string &attributeName, const eos::wfe::Md &dir) {
  const auto itor = dir.xattr().find(attributeName);
  if(itor == dir.xattr().end()) {
    cta::exception::Exception ex;
    ex.getMessage() << "Directory " << dir.lpath() << " has no attribute named " << attributeName;
    throw ex;
  }
  return itor->second;
}

//------------------------------------------------------------------------------
// processPREPARE
//------------------------------------------------------------------------------
XrdCtaFilesystem::UniqueXrdOucBuffer XrdCtaFilesystem::processPREPARE(const eos::wfe::Notification &msg,
  const XrdSecEntity *const client) {
  if(msg.wf().wfname() == "default") {
    auto reply = processDefaultPREPARE(msg, client);
    return UniqueXrdOucBuffer(reply.release());
  } else {
    cta::exception::Exception ex;
    ex.getMessage() << "Cannot process a PREPARE event for a " << msg.wf().wfname() << " workflow";
    throw ex;
  }
}

//------------------------------------------------------------------------------
// processDefaultPREPARE
//------------------------------------------------------------------------------
XrdCtaFilesystem::UniqueXrdOucBuffer XrdCtaFilesystem::processDefaultPREPARE(const eos::wfe::Notification &msg,
  const XrdSecEntity *const client) {
  cta::common::dataStructures::DiskFileInfo diskFileInfo;
  diskFileInfo.recoveryBlob = toJson(msg);
  diskFileInfo.group = msg.file().owner().groupname();
  diskFileInfo.owner = msg.file().owner().username();
  diskFileInfo.path = msg.file().lpath();

  cta::common::dataStructures::UserIdentity requester;
  requester.name = msg.cli().user().username();
  requester.group = msg.cli().user().groupname();

  const std::string archiveFileIdStr = getFileXattr("sys.archiveFileId", msg.file());

  cta::common::dataStructures::RetrieveRequest request;
  request.diskFileInfo = diskFileInfo;
  request.archiveFileID = cta::utils::toUint64(archiveFileIdStr);
  request.requester = requester;
  request.dstURL = msg.turl();

  const std::string diskInstance = msg.wf().instance().name();
  log::LogContext lc(*m_log);
  m_scheduler->queueRetrieve(diskInstance, request, lc);

  eos::wfe::Wrapper wrapper;
  wrapper.set_type(eos::wfe::Wrapper::ERROR); // Actually success - error code will be 0
  eos::wfe::Error *const error = wrapper.mutable_error();
  error->set_audience(eos::wfe::Error::EOSLOG);
  error->set_code(0);
  error->set_message("");

  std::string replyString = wrapper.SerializeAsString();
  return make_UniqueXrdOucBuffer(replyString.size(), replyString.c_str());
}

//------------------------------------------------------------------------------
// getFileXattr
//------------------------------------------------------------------------------
std::string XrdCtaFilesystem::getFileXattr(const std::string &attributeName, const eos::wfe::Md &file) {
  const auto itor = file.xattr().find(attributeName);
  if(itor == file.xattr().end()) {
    cta::exception::Exception ex;
    ex.getMessage() << "File " << file.lpath() << " has no attribute named " << attributeName;
    throw ex;
  }
  return itor->second;
}

//------------------------------------------------------------------------------
// newFile
//------------------------------------------------------------------------------
XrdSfsFile * XrdCtaFilesystem::newFile(char *user, int MonID)
{  
  return new cta::xrootPlugins::XrdCtaFile(m_catalogue.get(), m_scheduler.get(), m_log.get(), user, MonID);
}

//------------------------------------------------------------------------------
// newDir
//------------------------------------------------------------------------------
XrdSfsDirectory * XrdCtaFilesystem::newDir(char *user, int MonID)
{
  return new cta::xrootPlugins::XrdCtaDir(m_catalogue.get(), m_log.get(), user, MonID);;
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
  return nullptr;
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
  m_xrdOucBuffPool(1024, 65536), // XrdOucBuffPool(minsz, maxsz)
  m_ctaConf("/etc/cta/cta-frontend.conf"),
  m_backend(cta::objectstore::BackendFactory::createBackend(m_ctaConf.getConfEntString("ObjectStore", "BackendPath", nullptr)).release()),
  m_backendPopulator(*m_backend, "Frontend"),
  m_scheddb(*m_backend, m_backendPopulator.getAgentReference()) {
  using namespace cta;
  
  // Try to instantiate the logging system API
  try {
    std::string loggerURL=m_ctaConf.getConfEntString("Log", "URL", "syslog:");
    if (loggerURL == "syslog:") {
      m_log.reset(new log::SyslogLogger("cta-frontend", log::DEBUG));
    } else if (loggerURL == "stdout:") {
      m_log.reset(new log::StdoutLogger("cta-frontend"));
    } else if (loggerURL.substr(0, 5) == "file:") {
      m_log.reset(new log::FileLogger("cta-frontend", loggerURL.substr(5), log::DEBUG));
    } else {
      throw cta::exception::Exception(std::string("Unknown log URL: ")+loggerURL);
    }
  } catch(exception::Exception &ex) {
    throw cta::exception::Exception(std::string("Failed to instantiate object representing CTA logging system: ")+ex.getMessage().str());
  }
  
  const rdbms::Login catalogueLogin = rdbms::Login::parseFile("/etc/cta/cta_catalogue_db.conf");
  const uint64_t nbConns = m_ctaConf.getConfEntInt<uint64_t>("Catalogue", "NumberOfConnections", nullptr);
  m_catalogue = catalogue::CatalogueFactory::create(catalogueLogin, nbConns);
  m_scheduler = cta::make_unique<cta::Scheduler>(*m_catalogue, m_scheddb, 5, 2*1000*1000);

  // If the backend is a VFS, make sure we don't delete it on exit.
  // If not, nevermind.
  try {
    dynamic_cast<objectstore::BackendVFS &>(*m_backend).noDeleteOnExit();
  } catch (std::bad_cast &){}

  const std::list<cta::log::Param> params = {cta::log::Param("version", CTA_VERSION)};
  cta::log::Logger &log= *m_log;
  log(log::INFO, std::string("cta-frontend started"), params);
  
  // Start the heartbeat thread for the agent object
  m_agentHeartbeat = cta::make_unique<objectstore::AgentHeartbeatThread> (m_backendPopulator.getAgentReference(), *m_backend, *m_log);
  m_agentHeartbeat->startThread();
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
XrdCtaFilesystem::~XrdCtaFilesystem() {
  m_agentHeartbeat->stopAndWaitThread();
}

}}
