/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2015-2021 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "cmdline/restore_files/RestoreFilesCmd.hpp"
#include "cmdline/restore_files/RestoreFilesCmdLineArgs.hpp"
#include "common/utils/utils.hpp"
#include "common/make_unique.hpp"
#include "CtaFrontendApi.hpp"

#include <XrdSsiPbLog.hpp>
#include <XrdSsiPbIStreamBuffer.hpp>

#include <grpc++/grpc++.h>
#include "Rpc.grpc.pb.h"

#include <sys/stat.h>
#include <iostream>
#include <memory>

// GLOBAL VARIABLES : used to pass information between main thread and stream handler thread

// global synchronisation flag
std::atomic<bool> isHeaderSent(false);

std::list<cta::admin::RecycleTapeFileLsItem> deletedTapeFiles;

namespace XrdSsiPb {

/*!
 * User error exception
 */
class UserException : public std::runtime_error
{
public:
  UserException(const std::string &err_msg) : std::runtime_error(err_msg) {}
}; // class UserException

/*!
 * Alert callback.
 *
 * Defines how Alert messages should be logged
 */
template<>
void RequestCallback<cta::xrd::Alert>::operator()(const cta::xrd::Alert &alert)
{
   std::cout << "AlertCallback():" << std::endl;
   Log::DumpProtobuf(Log::PROTOBUF, &alert);
}

/*!
 * Data/Stream callback.
 *
 * Defines how incoming records from the stream should be handled
 */
template<>
void IStreamBuffer<cta::xrd::Data>::DataCallback(cta::xrd::Data record) const
{
  using namespace cta::xrd;
  using namespace cta::admin;

  // Wait for primary response to be handled before allowing stream response
  while(!isHeaderSent) { std::this_thread::yield(); }

  switch(record.data_case()) {
    case Data::kRtflsItem:
      {
        auto item = record.rtfls_item();
        deletedTapeFiles.push_back(item);
        break;
      }
    case Data::kTflsItem:
      break;
    default:
      throw std::runtime_error("Received invalid stream data from CTA Frontend for the cta-restore-deleted-files command.");
   }
}

} // namespace XrdSsiPb


namespace cta{
namespace admin {

/*!
 * RestoreFilesCmdException
 */
class RestoreFilesCmdException : public std::runtime_error
{
public:
  RestoreFilesCmdException(const std::string &err_msg) : std::runtime_error(err_msg) {}
}; // class UserException



//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
RestoreFilesCmd::RestoreFilesCmd(std::istream &inStream, std::ostream &outStream,
  std::ostream &errStream, cta::log::StdoutLogger &log):
  CmdLineTool(inStream, outStream, errStream),
  m_log(log) {

  // Default layout: see EOS common/LayoutId.hh for definitions of constants
  const int kAdler         =  0x2;
  const int kReplica       = (0x1 <<  4);
  const int kStripeSize    = (0x0 <<  8); // 1 stripe
  const int kStripeWidth   = (0x0 << 16); // 4K blocks
  const int kBlockChecksum = (0x1 << 20);
  
  // Default single replica layout id should be 00100012
  m_defaultFileLayout = kReplica | kAdler | kStripeSize | kStripeWidth | kBlockChecksum;

}

//------------------------------------------------------------------------------
// exceptionThrowingMain
//------------------------------------------------------------------------------
int RestoreFilesCmd::exceptionThrowingMain(const int argc, char *const *const argv) {
  RestoreFilesCmdLineArgs cmdLineArgs(argc, argv);
  if (cmdLineArgs.m_help) {
    printUsage(m_out);
    return 0;
  }

  readAndSetConfiguration(getUsername(), cmdLineArgs);
  listDeletedFilesCta();
  for (auto &file: deletedTapeFiles) {
    /*From https://codimd.web.cern.ch/f9JQv3YzSmKJ_W_ezXN3fA#
    When a user deletes a file, there are tree scenarios for recovery:
    * The file has been deleted in the EOS namespace and the CTA catalogue 
      (normal case, must restore in both places)
    * EOS namespace entry is kept and diskFileId has not changed
      (just restore in CTA)
    * EOS namespace entry is kept and diskFileId has changed
      (restore the file with the new eos disk file id)
    */
    try {
      if (!fileExistsEos(file.disk_instance(), file.disk_file_id())) {
        uint64_t new_fid = restoreDeletedFileEos(file);
        file.set_disk_file_id(std::to_string(new_fid));    
      }
      //archive file exists in CTA, so only need to restore the file copy
      restoreDeletedFileCopyCta(file);
    } catch (RestoreFilesCmdException &e) {
      m_log(cta::log::ERR,e.what());
    }
  }
  return 0;
}

//------------------------------------------------------------------------------
// readAndSetConfiguration
//------------------------------------------------------------------------------
void RestoreFilesCmd::readAndSetConfiguration(
  const std::string &userName, 
  const RestoreFilesCmdLineArgs &cmdLineArgs) {
  
  m_vid = cmdLineArgs.m_vid;
  m_diskInstance = cmdLineArgs.m_diskInstance;
  m_eosFxids = cmdLineArgs.m_eosFxids;
  m_copyNumber = cmdLineArgs.m_copyNumber;
  m_archiveFileId = cmdLineArgs.m_archiveFileId;

  if (cmdLineArgs.m_debug) {
    m_log.setLogMask("DEBUG");
  } else {
    m_log.setLogMask("INFO");
  }

  // Set cta frontend configuration options
  const std::string cli_config_file = "/etc/cta/cta-cli.conf";
  XrdSsiPb::Config cliConfig(cli_config_file, "cta");
  cliConfig.set("resource", "/ctafrontend");
  cliConfig.set("response_bufsize", StreamBufferSize);         // default value = 1024 bytes
  cliConfig.set("request_timeout", DefaultRequestTimeout);     // default value = 10s

  // Allow environment variables to override config file
  cliConfig.getEnv("request_timeout", "XRD_REQUESTTIMEOUT");

  // If XRDDEBUG=1, switch on all logging
  if(getenv("XRDDEBUG")) {
    cliConfig.set("log", "all");
  }
  // If fine-grained control over log level is required, use XrdSsiPbLogLevel
  cliConfig.getEnv("log", "XrdSsiPbLogLevel");

  // Validate that endpoint was specified in the config file
  if(!cliConfig.getOptionValueStr("endpoint").first) {
    throw std::runtime_error("Configuration error: cta.endpoint missing from " + cli_config_file);
  }

  // If the server is down, we want an immediate failure. Set client retry to a single attempt.
  XrdSsiProviderClient->SetTimeout(XrdSsiProvider::connect_N, 1);

  m_serviceProviderPtr.reset(new XrdSsiPbServiceType(cliConfig));

  // Set cta frontend configuration options to connect to eos
  const std::string frontend_xrootd_config_file = "/etc/cta/cta-frontend-xrootd.conf";
  XrdSsiPb::Config frontendXrootdConfig(frontend_xrootd_config_file, "cta");

  // Get the endpoint for namespace queries
  auto nsConf = frontendXrootdConfig.getOptionValueStr("ns.config");
  if(nsConf.first) {
    setNamespaceMap(nsConf.second);
  } else {
    throw std::runtime_error("Configuration error: cta.ns.config missing from " + frontend_xrootd_config_file);
  }
}

void RestoreFilesCmd::setNamespaceMap(const std::string &keytab_file) {
  // Open the keytab file for reading
  std::ifstream file(keytab_file);
  if(!file) {
    throw cta::exception::UserError("Failed to open namespace keytab configuration file " + keytab_file);
  }
  ::eos::client::NamespaceMap_t namespaceMap;
  // Parse the keytab line by line
  std::string line;
  for(int lineno = 0; std::getline(file, line); ++lineno) {
    // Strip out comments
    auto pos = line.find('#');
    if(pos != std::string::npos) {
      line.resize(pos);
    }

    // Parse one line
    std::stringstream ss(line);
    std::string diskInstance;
    std::string endpoint;
    std::string token;
    std::string eol;
    ss >> diskInstance >> endpoint >> token >> eol;

    // Ignore blank lines, all other lines must have exactly 3 elements
    if(token.empty() || !eol.empty()) {
      if(diskInstance.empty() && endpoint.empty() && token.empty()) continue;
      throw cta::exception::UserError("Could not parse namespace keytab configuration file line " + std::to_string(lineno) + ": " + line);
    }
    namespaceMap.insert(std::make_pair(diskInstance, ::eos::client::Namespace(endpoint, token)));
  }
  m_endpointMapPtr = cta::make_unique<::eos::client::EndpointMap>(namespaceMap);
}

//------------------------------------------------------------------------------
// listDeletedFilesCta
//------------------------------------------------------------------------------
void RestoreFilesCmd::listDeletedFilesCta() const {
  std::list<cta::log::Param> params;
  params.push_back(cta::log::Param("userName", getUsername()));
  
  cta::xrd::Request request;

  auto &admincmd = *(request.mutable_admincmd());
   
  request.set_client_cta_version(CTA_VERSION);
  request.set_client_xrootd_ssi_protobuf_interface_version(XROOTD_SSI_PROTOBUF_INTERFACE_VERSION);
  admincmd.set_cmd(AdminCmd::CMD_RECYCLETAPEFILE);
  admincmd.set_subcmd(AdminCmd::SUBCMD_LS);

  if (m_vid) {
    params.push_back(cta::log::Param("tapeVid", m_vid.value()));
    auto key = OptionString::VID;
    auto new_opt = admincmd.add_option_str();
    new_opt->set_key(key);
    new_opt->set_value(m_vid.value());
  }
  if (m_diskInstance) {
    params.push_back(cta::log::Param("diskInstance", m_diskInstance.value()));
    auto key = OptionString::INSTANCE;
    auto new_opt = admincmd.add_option_str();
    new_opt->set_key(key);
    new_opt->set_value(m_diskInstance.value());
  }
  if (m_archiveFileId) {
    params.push_back(cta::log::Param("archiveFileId", m_archiveFileId.value()));
    auto key = OptionUInt64::ARCHIVE_FILE_ID;
    auto new_opt = admincmd.add_option_uint64();
    new_opt->set_key(key);
    new_opt->set_value(m_archiveFileId.value());
  }
  if (m_copyNumber) {
    params.push_back(cta::log::Param("copyNb", m_copyNumber.value()));
    auto key = OptionUInt64::COPY_NUMBER;
    auto new_opt = admincmd.add_option_uint64();
    new_opt->set_key(key);
    new_opt->set_value(m_copyNumber.value());
  }
  if (m_eosFxids) {
    std::stringstream ss;
    auto key = OptionStrList::FILE_ID;
    auto new_opt = admincmd.add_option_str_list();
    new_opt->set_key(key);
    for (auto &fxid: m_eosFxids.value()) {
      new_opt->add_item(fxid);
      ss << fxid << ",";
    }
    auto fids = ss.str();
    fids.pop_back(); //remove last ","
    params.push_back(cta::log::Param("diskFileId", fids));
    
  }


  m_log(cta::log::INFO, "Listing deleted file in cta catalogue", params);  

  // Send the Request to the Service and get a Response
  cta::xrd::Response response;
  auto stream_future = m_serviceProviderPtr->SendAsync(request, response);

  // Handle responses
  switch(response.type())
  {
    using namespace cta::xrd;
    using namespace cta::admin;
    case Response::RSP_SUCCESS:
      // Print message text
      std::cout << response.message_txt();
      // Allow stream processing to commence
      isHeaderSent = true;
      break;
    case Response::RSP_ERR_PROTOBUF:                     throw XrdSsiPb::PbException(response.message_txt());
    case Response::RSP_ERR_USER:                         throw XrdSsiPb::UserException(response.message_txt());
    case Response::RSP_ERR_CTA:                          throw std::runtime_error(response.message_txt());
    default:                                             throw XrdSsiPb::PbException("Invalid response type.");
  }

  // wait until the data stream has been processed before exiting
  stream_future.wait();

  params.push_back(cta::log::Param("nbFiles", deletedTapeFiles.size()));
  m_log(cta::log::INFO, "Listed deleted file in cta catalogue", params);  

}

//------------------------------------------------------------------------------
// restoreDeletedFileCopyCta
//------------------------------------------------------------------------------
void RestoreFilesCmd::restoreDeletedFileCopyCta(const RecycleTapeFileLsItem &file) const {

  std::list<cta::log::Param> params;
  params.push_back(cta::log::Param("userName", getUsername()));
  params.push_back(cta::log::Param("tapeVid", file.vid()));
  params.push_back(cta::log::Param("diskInstance", file.disk_instance()));
  params.push_back(cta::log::Param("archiveFileId", file.archive_file_id()));
  params.push_back(cta::log::Param("copyNb", file.copy_nb()));
  params.push_back(cta::log::Param("diskFileId", file.disk_file_id()));
  
  m_log(cta::log::DEBUG, "Restoring file copy in cta catalogue", params);  

  cta::xrd::Request request;

  auto &admincmd = *(request.mutable_admincmd());
   
  request.set_client_cta_version(CTA_VERSION);
  request.set_client_xrootd_ssi_protobuf_interface_version(XROOTD_SSI_PROTOBUF_INTERFACE_VERSION);
  admincmd.set_cmd(AdminCmd::CMD_RECYCLETAPEFILE);
  admincmd.set_subcmd(AdminCmd::SUBCMD_RESTORE);

  {
    auto key = OptionString::VID;
    auto new_opt = admincmd.add_option_str();
    new_opt->set_key(key);
    new_opt->set_value(file.vid());
  }
  {
    auto key = OptionString::INSTANCE;
    auto new_opt = admincmd.add_option_str();
    new_opt->set_key(key);
    new_opt->set_value(file.disk_instance());
  }
  {
    auto key = OptionUInt64::ARCHIVE_FILE_ID;
    auto new_opt = admincmd.add_option_uint64();
    new_opt->set_key(key);
    new_opt->set_value(file.archive_file_id());
  }
  {
    auto key = OptionUInt64::COPY_NUMBER;
    auto new_opt = admincmd.add_option_uint64();
    new_opt->set_key(key);
    new_opt->set_value(file.copy_nb());
  }
  {
    auto key = OptionString::FXID;
    auto new_opt = admincmd.add_option_str();
    new_opt->set_key(key);
    new_opt->set_value(file.disk_file_id());
    
  }
  // Send the Request to the Service and get a Response
  cta::xrd::Response response;
  m_serviceProviderPtr->Send(request, response);

  // Handle responses
  switch(response.type())
  {
    using namespace cta::xrd;
    using namespace cta::admin;
    case Response::RSP_SUCCESS:
      // Print message text
      std::cout << response.message_txt();
      m_log(cta::log::INFO, "Restored file copy in cta catalogue", params);  
      break;
    case Response::RSP_ERR_PROTOBUF:                     throw XrdSsiPb::PbException(response.message_txt());
    case Response::RSP_ERR_USER:                         throw XrdSsiPb::UserException(response.message_txt());
    case Response::RSP_ERR_CTA:                          throw std::runtime_error(response.message_txt());
    default:                                             throw XrdSsiPb::PbException("Invalid response type.");
  }
}

//------------------------------------------------------------------------------
// addContainerEos
//------------------------------------------------------------------------------
int RestoreFilesCmd::addContainerEos(const std::string &diskInstance, const std::string &path, const std::string &sc) const {
  int c_id = containerExistsEos(diskInstance, path);
  if (c_id) {
    return c_id;
  }
  auto enclosingPath = cta::utils::getEnclosingPath(path);
  int parent_id = containerExistsEos(diskInstance, enclosingPath);
  if (!parent_id) {
    //parent does not exist, need to add it as well
    parent_id = addContainerEos(diskInstance, enclosingPath, sc);
  }

  std::list<cta::log::Param> params;
  params.push_back(cta::log::Param("userName", getUsername()));
  params.push_back(cta::log::Param("diskInstance", diskInstance));
  params.push_back(cta::log::Param("path", path));
  m_log(cta::log::DEBUG, "Inserting container in EOS namespace", params);

  ::eos::rpc::ContainerMdProto dir;
  dir.set_path(path);
  dir.set_name(cta::utils::getEnclosedName(path));

  // Filemode: filter out S_ISUID, S_ISGID and S_ISVTX because EOS does not follow POSIX semantics for these bits  
  uint64_t filemode = (S_IRWXU | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH); // 0755 permissions by default
  filemode &= ~(S_ISUID | S_ISGID | S_ISVTX);
  dir.set_mode(filemode);

  auto time = ::time(nullptr);
  // Timestamps
  dir.mutable_ctime()->set_sec(time);
  dir.mutable_mtime()->set_sec(time);
  // we don't care about dir.stime (sync time, used for CERNBox)

  dir.mutable_xattrs()->insert(google::protobuf::MapPair<std::string,std::string>("sys.archive.storage_class", sc));


  auto reply = m_endpointMapPtr->containerInsert(diskInstance, dir);

  m_log(cta::log::DEBUG, "Inserted container in EOS namespace successfully, querying again for its id", params);
  
  int cont_id = containerExistsEos(diskInstance, path);
  if (!cont_id) {
    throw RestoreFilesCmdException(std::string("Container ") + path + " does not exist after being inserted in EOS.");
  }
  return cont_id;
}

//------------------------------------------------------------------------------
// containerExistsEos
//------------------------------------------------------------------------------
int RestoreFilesCmd::containerExistsEos(const std::string &diskInstance, const std::string &path) const {
  std::list<cta::log::Param> params;
  params.push_back(cta::log::Param("userName", getUsername()));
  params.push_back(cta::log::Param("diskInstance", diskInstance));
  params.push_back(cta::log::Param("path", path));

  m_log(cta::log::DEBUG, "Verifying if the container exists in the EOS namespace", params);
  
  auto md_response = m_endpointMapPtr->getMD(diskInstance, ::eos::rpc::CONTAINER, 0, path, false);
  int cid = md_response.cmd().id();
  params.push_back(cta::log::Param("containerId", cid));
  if (cid != 0) {
    m_log(cta::log::DEBUG, "Container exists in the eos namespace", params);
  } else {
    m_log(cta::log::DEBUG, "Container does not exist in the eos namespace", params);
  }
  return cid;
}

bool RestoreFilesCmd::fileWasDeletedByRM(const RecycleTapeFileLsItem &file) const {
  return file.reason_log().rfind("(Deleted using cta-admin tf rm)", 0) == 0;
}

//------------------------------------------------------------------------------
// archiveFileExistsCTA
//------------------------------------------------------------------------------
bool RestoreFilesCmd::archiveFileExistsCTA(const uint64_t &archiveFileId) const {

  std::list<cta::log::Param> params;
  params.push_back(cta::log::Param("userName", getUsername()));
  params.push_back(cta::log::Param("archiveFileId", archiveFileId));
  
  m_log(cta::log::DEBUG, "Looking for archive file in the cta catalogue", params);  

  cta::xrd::Request request;

  auto &admincmd = *(request.mutable_admincmd());
   
  request.set_client_cta_version(CTA_VERSION);
  request.set_client_xrootd_ssi_protobuf_interface_version(XROOTD_SSI_PROTOBUF_INTERFACE_VERSION);
  admincmd.set_cmd(AdminCmd::CMD_TAPEFILE);
  admincmd.set_subcmd(AdminCmd::SUBCMD_LS);
  
  auto key = OptionUInt64::ARCHIVE_FILE_ID;
  auto new_opt = admincmd.add_option_uint64();
  new_opt->set_key(key);
  new_opt->set_value(archiveFileId);

  // Send the Request to the Service and get a Response
  cta::xrd::Response response;
  auto stream_future = m_serviceProviderPtr->SendAsync(request, response);

  bool ret;
  // Handle responses
  switch(response.type())
  {
    using namespace cta::xrd;
    using namespace cta::admin;
    case Response::RSP_SUCCESS:                          ret = true; break; //success sent if archive file does not exist
    case Response::RSP_ERR_PROTOBUF:                     throw XrdSsiPb::PbException(response.message_txt());
    case Response::RSP_ERR_USER:                         ret = false; break; //user error sent if archive file does not exist
    case Response::RSP_ERR_CTA:                          throw std::runtime_error(response.message_txt());
    default:                                             throw XrdSsiPb::PbException("Invalid response type.");
  }

  // wait until the data stream has been processed before exiting
  if (ret) {
    stream_future.wait();
  }
  if (ret) {
    m_log(cta::log::DEBUG, "Archive file is present in the CTA catalogue", params);  
  } else {
    m_log(cta::log::DEBUG, "Archive file is missing in the CTA catalogue", params);
  }
  return ret;
}

//------------------------------------------------------------------------------
// fileExistsEos
//------------------------------------------------------------------------------
bool RestoreFilesCmd::fileExistsEos(const std::string &diskInstance, const std::string &diskFileId) const {
  std::list<cta::log::Param> params;
  params.push_back(cta::log::Param("userName", getUsername()));
  params.push_back(cta::log::Param("diskInstance", diskInstance));
  params.push_back(cta::log::Param("diskFileId", diskFileId));

  m_log(cta::log::DEBUG, "Verifying if eos fxid exists in the EOS namespace", params);
  try {
    auto path = m_endpointMapPtr->getPath(diskInstance, diskFileId);
    params.push_back(cta::log::Param("diskFilePath", path));
    m_log(cta::log::DEBUG, "eos fxid exists in the EOS namespace");
    return true;
  } catch(cta::exception::Exception) {
    m_log(cta::log::DEBUG, "eos fxid does not exist in the EOS namespace");
    return false;
  }
}

//------------------------------------------------------------------------------
// getFileIdEos
//------------------------------------------------------------------------------
int RestoreFilesCmd::getFileIdEos(const std::string &diskInstance, const std::string &path) const {
  std::list<cta::log::Param> params;
  params.push_back(cta::log::Param("userName", getUsername()));
  params.push_back(cta::log::Param("diskInstance", diskInstance));
  params.push_back(cta::log::Param("path", path));

  m_log(cta::log::DEBUG, "Querying for file metadata in the eos namespace", params);
  auto md_response = m_endpointMapPtr->getMD(diskInstance, ::eos::rpc::FILE, 0, path, false);
  int fid = md_response.fmd().id();
  params.push_back(cta::log::Param("diskFileId", fid));
  if (fid != 0) {
    m_log(cta::log::DEBUG, "File path exists in the eos namespace", params);
  } else {
    m_log(cta::log::DEBUG, "File path does not exist in the eos namespace", params);
  }
  return fid;
}

//------------------------------------------------------------------------------
// getCurrentEosIds
//------------------------------------------------------------------------------
void RestoreFilesCmd::getCurrentEosIds(const std::string &diskInstance) const {
  uint64_t cid;
  uint64_t fid;
  m_endpointMapPtr->getCurrentIds(diskInstance, cid, fid);

  std::list<cta::log::Param> params;
  params.push_back(cta::log::Param("diskInstance", diskInstance));
  params.push_back(cta::log::Param("ContainerId", cid));
  params.push_back(cta::log::Param("FileId", fid));
  m_log(cta::log::DEBUG, "Obtained current eos container and file id", params);
}


//------------------------------------------------------------------------------
// restoreDeletedFileEos
//------------------------------------------------------------------------------
uint64_t RestoreFilesCmd::restoreDeletedFileEos(const RecycleTapeFileLsItem &rtfls_item) const {

  std::list<cta::log::Param> params;
  params.push_back(cta::log::Param("userName", getUsername()));
  params.push_back(cta::log::Param("diskInstance", rtfls_item.disk_instance()));
  params.push_back(cta::log::Param("archiveFileId", rtfls_item.archive_file_id()));
  params.push_back(cta::log::Param("diskFileId", rtfls_item.disk_file_id()));
  params.push_back(cta::log::Param("diskFilePath", rtfls_item.disk_file_path()));
  
  m_log(cta::log::INFO, "Restoring file in the eos namespace", params);  

  getCurrentEosIds(rtfls_item.disk_instance());
  uint64_t file_id = getFileIdEos(rtfls_item.disk_instance(), rtfls_item.disk_file_path());
  if (file_id) {
    return file_id; //eos disk file id was changed since the file was deleted, just return the new file id
  }

  ::eos::rpc::FileMdProto file;

  auto fullPath = rtfls_item.disk_file_path();  
  auto cont_id = addContainerEos(rtfls_item.disk_instance(), cta::utils::getEnclosingPath(fullPath), rtfls_item.storage_class());
  
  // We do not set the file id. Since the file was deleted the fid cannot be reused, so EOS will generate a new file id
  file.set_cont_id(cont_id);
  file.set_uid(rtfls_item.disk_file_uid());
  file.set_gid(rtfls_item.disk_file_gid());
  file.set_size(rtfls_item.size_in_bytes());
  file.set_layout_id(m_defaultFileLayout);

  // Filemode: filter out S_ISUID, S_ISGID and S_ISVTX because EOS does not follow POSIX semantics for these bits  
  uint64_t filemode = (S_IRWXU | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH); // 0755
  filemode &= ~(S_ISUID | S_ISGID | S_ISVTX);
  file.set_flags(filemode);

  // Timestamps
  auto time = ::time(nullptr);
  file.mutable_ctime()->set_sec(time);
  file.mutable_mtime()->set_sec(time);
  // we don't care about file.stime (sync time, used for CERNBox)
  // BTIME is set as an extended attribute (see below)

  // Filename and path
  file.set_path(fullPath);
  file.set_name(cta::utils::getEnclosedName(fullPath));

  // Checksums
  if(rtfls_item.checksum().empty()) {
    throw RestoreFilesCmdException("File " + rtfls_item.disk_file_id() + " does not have a checksum");
  }
  std::string checksumType("NONE");
  std::string checksumValue;
  const google::protobuf::EnumDescriptor *descriptor = cta::common::ChecksumBlob::Checksum::Type_descriptor();
  checksumType  = descriptor->FindValueByNumber(rtfls_item.checksum().begin()->type())->name();
  checksumValue = rtfls_item.checksum().begin()->value();
  file.mutable_checksum()->set_type(checksumType); //only support adler for now
  file.mutable_checksum()->set_value(checksumValue);

  // Extended attributes:
  //
  // 1. Archive File ID
  std::string archiveId(std::to_string(rtfls_item.archive_file_id()));
  file.mutable_xattrs()->insert(google::protobuf::MapPair<std::string,std::string>("sys.archive.file_id", archiveId));
  // 2. Storage Class
  file.mutable_xattrs()->insert(google::protobuf::MapPair<std::string,std::string>("sys.archive.storage_class", rtfls_item.storage_class()));
  // 3. Birth Time
  // POSIX ATIME (Access Time) is used by CASTOR to store the file creation time. EOS calls this "birth time",
  // but there is no place in the namespace to store it, so it is stored as an extended attribute.
  file.mutable_xattrs()->insert(google::protobuf::MapPair<std::string,std::string>("eos.btime", std::to_string(time)));

  // Indicate that there is a tape-resident replica of this file (except for zero-length files)
  if(file.size() > 0) {
    file.mutable_locations()->Add(65535);
  }

  auto diskInstance = rtfls_item.disk_instance();
  auto reply = m_endpointMapPtr->fileInsert(diskInstance, file);

  m_log(cta::log::INFO, "File successfully restored in the eos namespace", params);

  m_log(cta::log::DEBUG, "Querying eos for the new eos file id", params);
  
  auto new_fid = getFileIdEos(rtfls_item.disk_instance(), rtfls_item.disk_file_path());
  return new_fid;

}

//------------------------------------------------------------------------------
// printUsage
//------------------------------------------------------------------------------
void RestoreFilesCmd::printUsage(std::ostream &os) {
  RestoreFilesCmdLineArgs::printUsage(os);
}

} // namespace admin
} // namespace cta