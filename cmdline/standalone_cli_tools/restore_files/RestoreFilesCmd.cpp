/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#include "cmdline/CtaAdminCmdParse.hpp"
#include "cmdline/standalone_cli_tools/common/CatalogueFetch.hpp"
#include "cmdline/standalone_cli_tools/common/ConnectionConfiguration.hpp"
#include "cmdline/standalone_cli_tools/restore_files/RestoreFilesCmd.hpp"
#include "common/checksum/ChecksumBlob.hpp"
#include "common/utils/utils.hpp"
#include "CtaFrontendApi.hpp"

#include <XrdSsiPbLog.hpp>
#include <XrdSsiPbIStreamBuffer.hpp>

#include <grpc++/grpc++.h>
#include "Rpc.grpc.pb.h"

#include <sys/stat.h>
#include <iostream>
#include <memory>


namespace XrdSsiPb {
/*!
 * User error exception
 */
class UserException : public std::runtime_error
{
public:
  UserException(const std::string &err_msg) : std::runtime_error(err_msg) {}
}; // class UserException

}

namespace cta{
namespace cliTool {

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
  CmdLineArgs cmdLineArgs(argc, argv, StandaloneCliTool::RESTORE_FILES);
  if (cmdLineArgs.m_help) {
    cmdLineArgs.printUsage(m_out);
    return 0;
  }

  m_vid = cmdLineArgs.m_vid;
  m_diskInstance = cmdLineArgs.m_diskInstance;
  m_fids = cmdLineArgs.m_fids;
  m_copyNumber = cmdLineArgs.m_copyNumber;
  m_archiveFileId = cmdLineArgs.m_archiveFileId;

  if (m_fids && !m_diskInstance) {
    throw XrdSsiPb::UserException("Disk instance must be provided when fids are used as input.");
  }

  if (cmdLineArgs.m_debug) {
    m_log.setLogMask("DEBUG");
  } else {
    m_log.setLogMask("INFO");
  }

  auto [serviceProvider, endpointmap] = ConnConfiguration::readAndSetConfiguration(m_log, getUsername(), cmdLineArgs);
  m_serviceProviderPtr = std::move(serviceProvider);
  m_endpointMapPtr = std::move(endpointmap);

  std::list<cta::admin::RecycleTapeFileLsItem> deletedTapeFiles = CatalogueFetch::listDeletedFilesCta(m_vid, m_diskInstance, m_archiveFileId, m_copyNumber, m_fids, m_serviceProviderPtr, m_log);

  for (auto& file : deletedTapeFiles) {
    /* From https://eoscta.docs.cern.ch/lifecycle/Delete/:
     *
     * When a user deletes a file, there are three scenarios for recovery:
     * - The file has been deleted in the EOS namespace and the CTA catalogue (normal case, must restore in both places)
     * - EOS namespace entry is kept and diskFileId has not changed (just restore in CTA)
     * - EOS namespace entry is kept and diskFileId has changed (restore the file with the new EOS disk file id)
     */
    try {
      if (!fileExistsEos(file.disk_instance(), file.disk_file_id())) {
        uint64_t new_fid = restoreDeletedFileEos(file);
        file.set_disk_file_id(std::to_string(new_fid));
      }
      // archive file exists in CTA, so only need to restore the file copy
      restoreDeletedFileCopyCta(file);
      // sanity check
      const auto [diskInstance, diskFileId] = CatalogueFetch::getInstanceAndFid(std::to_string(file.archive_file_id()), m_serviceProviderPtr, m_log);
      auto eosArchiveFileIdAndChecksum = getArchiveFileIdAndChecksumFromEOS(diskInstance, diskFileId);
      auto &eosArchiveFileId = eosArchiveFileIdAndChecksum.first;
      auto &eosChecksum = eosArchiveFileIdAndChecksum.second;
      auto &ctaChecksum = file.checksum().begin()->value();
      std::list<cta::log::Param> params;
      params.push_back(cta::log::Param("archiveFileId", file.archive_file_id()));
      params.push_back(cta::log::Param("diskInstance", diskInstance));
      params.push_back(cta::log::Param("diskFileId", diskFileId));
      params.push_back(cta::log::Param("checksum", ctaChecksum));
      if(eosArchiveFileId == file.archive_file_id() && eosChecksum == ctaChecksum) {
        m_log(cta::log::INFO, "File metadata in EOS and CTA matches", params);
      } else {
        params.push_back(cta::log::Param("eosArchiveFileId", eosArchiveFileId));
        params.push_back(cta::log::Param("eosChecksum", eosChecksum));
        m_log(cta::log::INFO, "File metadata in EOS and CTA does not match", params);
        throw std::runtime_error("Sanity check failed.");
      }
    } catch (RestoreFilesCmdException &e) {
      m_log(cta::log::ERR,e.what());
    }
  }
  return 0;
}

//------------------------------------------------------------------------------
// restoreDeletedFileCopyCta
//------------------------------------------------------------------------------
void RestoreFilesCmd::restoreDeletedFileCopyCta(const cta::admin::RecycleTapeFileLsItem &file) const {

  std::list<cta::log::Param> params;
  params.push_back(cta::log::Param("userName", getUsername()));
  params.push_back(cta::log::Param("tapeVid", file.vid()));
  params.push_back(cta::log::Param("diskInstance", file.disk_instance()));
  params.push_back(cta::log::Param("archiveFileId", file.archive_file_id()));
  params.push_back(cta::log::Param("copyNb", file.copy_nb()));
  params.push_back(cta::log::Param("diskFileId", file.disk_file_id()));

  cta::xrd::Request request;

  auto &admincmd = *(request.mutable_admincmd());

  request.set_client_cta_version(CTA_VERSION);
  request.set_client_xrootd_ssi_protobuf_interface_version(XROOTD_SSI_PROTOBUF_INTERFACE_VERSION);
  admincmd.set_cmd(cta::admin::AdminCmd::CMD_RECYCLETAPEFILE);
  admincmd.set_subcmd(cta::admin::AdminCmd::SUBCMD_RESTORE);

  {
    auto key = cta::admin::OptionString::VID;
    auto new_opt = admincmd.add_option_str();
    new_opt->set_key(key);
    new_opt->set_value(file.vid());
  }
  {
    auto key = cta::admin::OptionString::INSTANCE;
    auto new_opt = admincmd.add_option_str();
    new_opt->set_key(key);
    new_opt->set_value(file.disk_instance());
  }
  {
    auto key = cta::admin::OptionUInt64::ARCHIVE_FILE_ID;
    auto new_opt = admincmd.add_option_uint64();
    new_opt->set_key(key);
    new_opt->set_value(file.archive_file_id());
  }
  {
    auto key = cta::admin::OptionUInt64::COPY_NUMBER;
    auto new_opt = admincmd.add_option_uint64();
    new_opt->set_key(key);
    new_opt->set_value(file.copy_nb());
  }
  {
    auto key = cta::admin::OptionString::FXID;
    auto new_opt = admincmd.add_option_str();

    // Convert diskFileId from base 10 to base 16 before transmitting to CTA
    auto fid = strtoul(file.disk_file_id().c_str(), nullptr, 10);
    if(fid < 1) {
      throw std::runtime_error(file.disk_file_id() + " (base 10) is not a valid disk file ID");
    }
    std::stringstream ss;
    ss << std::hex << fid;
    params.push_back(cta::log::Param("fxid", ss.str()));

    new_opt->set_key(key);
    new_opt->set_value(ss.str());
  }
  m_log(cta::log::DEBUG, "Restoring file copy in CTA catalogue", params);

  // This validation will also be done at the server side
  cta::admin::validateCmd(admincmd);

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
      m_log(cta::log::INFO, "Restored file copy in CTA catalogue", params);
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
uint64_t RestoreFilesCmd::addContainerEos(const std::string &diskInstance, const std::string &path, const std::string &sc) const {
  auto c_id = containerExistsEos(diskInstance, path);
  if (c_id) {
    return c_id;
  }
  auto enclosingPath = cta::utils::getEnclosingPath(path);
  auto parent_id = containerExistsEos(diskInstance, enclosingPath);
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

  auto cont_id = containerExistsEos(diskInstance, path);
  if (!cont_id) {
    throw RestoreFilesCmdException(std::string("Container ") + path + " does not exist after being inserted in EOS.");
  }
  return cont_id;
}

//------------------------------------------------------------------------------
// containerExistsEos
//------------------------------------------------------------------------------
uint64_t RestoreFilesCmd::containerExistsEos(const std::string &diskInstance, const std::string &path) const {
  std::list<cta::log::Param> params;
  params.push_back(cta::log::Param("userName", getUsername()));
  params.push_back(cta::log::Param("diskInstance", diskInstance));
  params.push_back(cta::log::Param("path", path));

  m_log(cta::log::DEBUG, "Verifying if the container exists in the EOS namespace", params);

  auto md_response = m_endpointMapPtr->getMD(diskInstance, ::eos::rpc::CONTAINER, 0, path, false);
  auto cid = md_response.cmd().id();
  params.push_back(cta::log::Param("containerId", cid));
  if (cid != 0) {
    m_log(cta::log::DEBUG, "Container exists in the EOS namespace", params);
  } else {
    m_log(cta::log::DEBUG, "Container does not exist in the EOS namespace", params);
  }
  return cid;
}

bool RestoreFilesCmd::fileWasDeletedByRM(const cta::admin::RecycleTapeFileLsItem &file) const {
  return file.reason_log().rfind("(Deleted using cta-admin tapefile rm)", 0) == 0;
}

//------------------------------------------------------------------------------
// archiveFileExistsCTA
//------------------------------------------------------------------------------
bool RestoreFilesCmd::archiveFileExistsCTA(const uint64_t &archiveFileId) const {

  std::list<cta::log::Param> params;
  params.push_back(cta::log::Param("userName", getUsername()));
  params.push_back(cta::log::Param("archiveFileId", archiveFileId));

  m_log(cta::log::DEBUG, "Looking for archive file in the CTA catalogue", params);

  cta::xrd::Request request;

  auto &admincmd = *(request.mutable_admincmd());

  request.set_client_cta_version(CTA_VERSION);
  request.set_client_xrootd_ssi_protobuf_interface_version(XROOTD_SSI_PROTOBUF_INTERFACE_VERSION);
  admincmd.set_cmd(cta::admin::AdminCmd::CMD_TAPEFILE);
  admincmd.set_subcmd(cta::admin::AdminCmd::SUBCMD_LS);

  auto key = cta::admin::OptionUInt64::ARCHIVE_FILE_ID;
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

  m_log(cta::log::DEBUG, "Verifying if EOS fid exists in the EOS namespace", params);
  try {
    auto path = m_endpointMapPtr->getPath(diskInstance, diskFileId);
    params.push_back(cta::log::Param("diskFilePath", path));
    m_log(cta::log::DEBUG, "EOS fid exists in the EOS namespace");
    return true;
  } catch(cta::exception::Exception&) {
    m_log(cta::log::DEBUG, "EOS fid does not exist in the EOS namespace");
    return false;
  }
}

//------------------------------------------------------------------------------
// getFileIdEos
//------------------------------------------------------------------------------
uint64_t RestoreFilesCmd::getFileIdEos(const std::string &diskInstance, const std::string &path) const {
  std::list<cta::log::Param> params;
  params.push_back(cta::log::Param("userName", getUsername()));
  params.push_back(cta::log::Param("diskInstance", diskInstance));
  params.push_back(cta::log::Param("path", path));

  m_log(cta::log::DEBUG, "Querying for file metadata in the EOS namespace", params);
  auto md_response = m_endpointMapPtr->getMD(diskInstance, ::eos::rpc::FILE, 0, path, false);
  auto fid = md_response.fmd().id();
  params.push_back(cta::log::Param("diskFileId", fid));
  if (fid != 0) {
    m_log(cta::log::DEBUG, "File path exists in the EOS namespace", params);
  } else {
    m_log(cta::log::DEBUG, "File path does not exist in the EOS namespace", params);
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
  m_log(cta::log::DEBUG, "Obtained current EOS container and file id", params);
}


//------------------------------------------------------------------------------
// restoreDeletedFileEos
//------------------------------------------------------------------------------
uint64_t RestoreFilesCmd::restoreDeletedFileEos(const cta::admin::RecycleTapeFileLsItem &rtfls_item) const {

  std::list<cta::log::Param> params;
  params.push_back(cta::log::Param("userName", getUsername()));
  params.push_back(cta::log::Param("diskInstance", rtfls_item.disk_instance()));
  params.push_back(cta::log::Param("archiveFileId", rtfls_item.archive_file_id()));
  params.push_back(cta::log::Param("diskFileId", rtfls_item.disk_file_id()));
  params.push_back(cta::log::Param("diskFilePath", rtfls_item.disk_file_path()));

  m_log(cta::log::INFO, "Restoring file in the EOS namespace", params);

  getCurrentEosIds(rtfls_item.disk_instance());
  uint64_t file_id = getFileIdEos(rtfls_item.disk_instance(), rtfls_item.disk_file_path());
  if (file_id) {
    return file_id; // EOS disk file id was changed since the file was deleted, just return the new file id
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

  // TODO: The protobuf and supporting code here and in EOS should be refactored to take a ChecksumBlob
  // rather than passing a single Adler32 checksum. This would avoid the need for format conversions.
  // For now, only Adler32 is supported.
  file.mutable_checksum()->set_type(checksumType);
  auto byteArray = checksum::ChecksumBlob::HexToByteArray(checksumValue);
  file.mutable_checksum()->set_value(std::string(byteArray.rbegin(),byteArray.rend()));

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

  m_log(cta::log::INFO, "File successfully restored in the EOS namespace", params);

  m_log(cta::log::DEBUG, "Querying EOS for the new EOS file id", params);

  auto new_fid = getFileIdEos(rtfls_item.disk_instance(), rtfls_item.disk_file_path());
  return new_fid;
}


//------------------------------------------------------------------------------
// getArchiveFileIdFromEOS
//------------------------------------------------------------------------------
std::pair<uint64_t, std::string> RestoreFilesCmd::getArchiveFileIdAndChecksumFromEOS(
  const std::string& diskInstance, const std::string& fidStr) {
  auto fid = strtoul(fidStr.c_str(), nullptr, 10);
  if(fid < 1) {
    throw std::runtime_error(fid + " (base 10) is not a valid disk file ID");
  }
  {
    std::list<cta::log::Param> params;
    params.push_back(cta::log::Param("diskInstance", diskInstance));
    params.push_back(cta::log::Param("fid", fid));
    std::stringstream ss;
    ss << std::hex << fid;
    params.push_back(cta::log::Param("fxid", ss.str()));
    m_log(cta::log::DEBUG, "Querying EOS namespace for archiveFileId and checksum", params);
  }
  auto md_response = m_endpointMapPtr->getMD(diskInstance, ::eos::rpc::FILE, fid, "", false);
  auto archiveFileIdItor = md_response.fmd().xattrs().find("sys.archive.file_id");
  if(md_response.fmd().xattrs().end() == archiveFileIdItor) {
    throw std::runtime_error("archiveFileId extended attribute not found.");
  }
  auto archiveFileId = strtoul(archiveFileIdItor->second.c_str(), nullptr, 10);
  auto byteArray = md_response.fmd().checksum().value();
  auto checksumValue = checksum::ChecksumBlob::ByteArrayToHex(std::string(byteArray.rbegin(), byteArray.rend()));
  {
    std::list<cta::log::Param> params;
    params.push_back(cta::log::Param("archiveFileId", archiveFileId));
    params.push_back(cta::log::Param("checksumValue", checksumValue));
    m_log(cta::log::DEBUG, "Response from EOS nameserver", params);
  }

  return std::make_pair(archiveFileId,checksumValue);
}

} // namespace admin
} // namespace cta
