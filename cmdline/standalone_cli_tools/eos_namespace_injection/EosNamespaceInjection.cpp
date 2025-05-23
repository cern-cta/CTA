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

#include <ctime>
#include <sys/stat.h>
#include <string>
#include <memory>

#include "cmdline/CtaAdminParsedCmd.hpp"
#include "cmdline/standalone_cli_tools/common/CmdLineArgs.hpp"
#include "cmdline/standalone_cli_tools/common/ConnectionConfiguration.hpp"
#include "cmdline/standalone_cli_tools/eos_namespace_injection/EosNamespaceInjection.hpp"
#include "cmdline/standalone_cli_tools/eos_namespace_injection/MetaData.hpp"
#include "common/checksum/ChecksumBlobSerDeser.hpp"
#include "common/exception/UserError.hpp"
#include "common/log/StdoutLogger.hpp"
#include "common/utils/utils.hpp"
#include "CtaFrontendApi.hpp"
#include "eos_grpc_client/GrpcEndpoint.hpp"
#include "Rpc.grpc.pb.h"
#include "XrdSsiPbIStreamBuffer.hpp"
#include "XrdSsiPbLog.hpp"
#include <grpc++/grpc++.h>

// global synchronisation flag
std::atomic<bool> isHeaderSent;

// Global variables for recieving stream data
cta::cliTool::MetaDataObject g_metaDataObjectCatalogue;

namespace XrdSsiPb {

/*!
 * Alert callback.
 *
 * Defines how Alert messages should be logged
 */
template<>
void RequestCallback<cta::xrd::Alert>::operator()(const cta::xrd::Alert &alert) {
   Log::DumpProtobuf(Log::PROTOBUF, &alert);
}

/*!
 * Data/Stream callback.
 *
 * Defines how incoming records from the stream should be handled
 */
template<>
void IStreamBuffer<cta::xrd::Data>::DataCallback(cta::xrd::Data record) const {
  using namespace cta::xrd;
  using namespace cta::admin;

  // Wait for primary response to be handled before allowing stream response
  while(!isHeaderSent) { std::this_thread::yield(); }

  switch(record.data_case()) {
    case Data::kTflsItem:
    {
      const auto& item = record.tfls_item();

      g_metaDataObjectCatalogue.archiveId     = std::to_string(item.af().archive_id());
      g_metaDataObjectCatalogue.size          = std::to_string(item.af().size());
      g_metaDataObjectCatalogue.storageClass  = item.af().storage_class();
      g_metaDataObjectCatalogue.creationTime  = std::to_string(item.af().creation_time());
      g_metaDataObjectCatalogue.diskFileId    = item.df().disk_id();
      g_metaDataObjectCatalogue.diskInstance  = item.df().disk_instance();

      std::string checksumType("NONE");
      std::string checksumValue;

      if(!item.af().checksum().empty()) {
        const google::protobuf::EnumDescriptor *descriptor = cta::common::ChecksumBlob::Checksum::Type_descriptor();
        g_metaDataObjectCatalogue.checksumType  = descriptor->FindValueByNumber(item.af().checksum().begin()->type())->name();
        g_metaDataObjectCatalogue.checksumValue = item.af().checksum().begin()->value();
      }
      break;
      }
    default:
      throw std::runtime_error("Received invalid stream data from CTA Frontend for the cta-restore-deleted-files command.");
   }
}

} // namespace XrdSsiPb

namespace cta::cliTool {

class EosNameSpaceInjectionError : public std::runtime_error {
public:
  using runtime_error::runtime_error;
}; // class EosNameSpaceInjectionError

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
EosNamespaceInjection::EosNamespaceInjection(std::istream &inStream, std::ostream &outStream,
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
// destructor
//------------------------------------------------------------------------------
EosNamespaceInjection::~EosNamespaceInjection() = default;

//------------------------------------------------------------------------------
// exceptionThrowingMain
//------------------------------------------------------------------------------
int EosNamespaceInjection::exceptionThrowingMain(const int argc, char *const *const argv) {

  setCmdLineArguments(argc, argv);

  MetaData userInput(m_jsonPath);
  for(const auto &metaDataFromUser : userInput.m_mdCollection) {
    const uint64_t archiveId = cta::utils::toUint64(metaDataFromUser.archiveId);
    checkArchiveIdExistsInCatalogue(archiveId);
    compareJsonAndCtaMetaData(metaDataFromUser, g_metaDataObjectCatalogue);

    const auto enclosingPath = cta::utils::getEnclosingPath(metaDataFromUser.eosPath);
    const auto [parentId, uid, gid] = getContainerIdsEos(metaDataFromUser.diskInstance, enclosingPath);
    checkParentContainerExists(parentId, enclosingPath);

    if(const auto& fid = getFileIdEos(metaDataFromUser.diskInstance, metaDataFromUser.eosPath); pathExists(fid)) {
      checkExistingPathHasInvalidMetadata(archiveId, fid, metaDataFromUser);
      continue;
    }

    const auto newFid = createFileInEos(metaDataFromUser, parentId, uid, gid);
    checkFileCreated(newFid);

    auto diskFileId = std::to_string(newFid);
    updateDiskFileIdAndDiskInstanceInCatalogue(metaDataFromUser.archiveId, diskFileId, metaDataFromUser.diskInstance);

    checkEosCtaConsistency(archiveId, diskFileId, metaDataFromUser);
  }
  createTxtFileWithSkippedMetadata();
  return 0;
}

//------------------------------------------------------------------------------
// updateFxidAndDiskInstanceInCatalogue
//------------------------------------------------------------------------------
void EosNamespaceInjection::updateDiskFileIdAndDiskInstanceInCatalogue(const std::string &archiveId, const std::string &diskFileId, const std::string &diskInstance) const {
  cta::xrd::Request request;

  const auto admincmd = request.mutable_admincmd();

  admincmd->set_client_version(CTA_VERSION);
  admincmd->set_protobuf_tag(XROOTD_SSI_PROTOBUF_INTERFACE_VERSION);
  admincmd->set_cmd(cta::admin::AdminCmd::CMD_ARCHIVEFILE);
  admincmd->set_subcmd(cta::admin::AdminCmd::SUBCMD_CH);

  {
    constexpr auto key = cta::admin::OptionString::DISK_FILE_ID;
    const auto new_opt = admincmd->add_option_str();
    new_opt->set_key(key);
    new_opt->set_value(diskFileId);
  }

  {
    constexpr auto key = cta::admin::OptionString::DISK_INSTANCE;
    const auto new_opt = admincmd->add_option_str();
    new_opt->set_key(key);
    new_opt->set_value(diskInstance);
  }

  {
    constexpr auto key = cta::admin::OptionStrList::FILE_ID;
    const auto new_opt = admincmd->add_option_str_list();
    new_opt->set_key(key);
    new_opt->add_item(archiveId);
  }

  // Validate the Protocol Buffer
  cta::admin::validateCmd(*admincmd);

  // Send the Request to the Service and get a Response
  cta::xrd::Response response;
  m_serviceProviderPtr->Send(request, response, false);

  // Handle responses
  switch(response.type()) {
    using namespace cta::xrd;
    using namespace cta::admin;
    case Response::RSP_SUCCESS:
      // Print message text
      std::cout << response.message_txt();
      break;
    case Response::RSP_ERR_PROTOBUF:                     throw XrdSsiPb::PbException(response.message_txt());
    case Response::RSP_ERR_USER:                         throw exception::UserError(response.message_txt());
    case Response::RSP_ERR_CTA:                          throw std::runtime_error(response.message_txt());
    default:                                             throw XrdSsiPb::PbException("Invalid response type.");
  }
}

//------------------------------------------------------------------------------
// compareJsonAndCtaMetaData
//------------------------------------------------------------------------------
void EosNamespaceInjection::compareJsonAndCtaMetaData(const MetaDataObject &metaDataJson, const MetaDataObject &metaDataCatalogue) const {
  auto compareValues = [](const std::string &lhs, const std::string &rhs, const std::string &errorMessage) {
    if(lhs != rhs) {
      throw std::runtime_error(errorMessage);
    }
  };

  compareValues(metaDataJson.size,          metaDataCatalogue.size,          "Error: File size does not match when comparing the provided json file and the catalogue.");
  compareValues(metaDataJson.checksumType,  metaDataCatalogue.checksumType,  "Error: Checksum type does not match when comparing the provided json file and the catalogue.");
  compareValues(metaDataJson.checksumValue, metaDataCatalogue.checksumValue, "Error: Checksum value does not match when comparing the provided json file and the catalogue.");
}


//------------------------------------------------------------------------------
// archiveFileExistsCTA
//------------------------------------------------------------------------------
bool EosNamespaceInjection::getMetaDataFromCatalogue(const uint64_t &archiveId) const {

  std::list<cta::log::Param> params;
  params.push_back(cta::log::Param("userName", getUsername()));
  params.push_back(cta::log::Param("archiveFileId", archiveId));

  m_log(cta::log::DEBUG, "Looking for archive file in the CTA catalogue", params);

  cta::xrd::Request request;

  auto &admincmd = *(request.mutable_admincmd());

  admincmd.set_client_version(CTA_VERSION);
  admincmd.set_protobuf_tag(XROOTD_SSI_PROTOBUF_INTERFACE_VERSION);
  admincmd.set_cmd(cta::admin::AdminCmd::CMD_TAPEFILE);
  admincmd.set_subcmd(cta::admin::AdminCmd::SUBCMD_LS);

  auto key = cta::admin::OptionUInt64::ARCHIVE_FILE_ID;
  auto new_opt = admincmd.add_option_uint64();
  new_opt->set_key(key);
  new_opt->set_value(archiveId);

  // Send the Request to the Service and get a Response
  cta::xrd::Response response;
  auto stream_future = m_serviceProviderPtr->SendAsync(request, response, false);

  bool ret = false;

  // Handle responses
  switch(response.type()) {
    using namespace cta::xrd;
    using namespace cta::admin;
    case Response::RSP_SUCCESS:                          ret = true; isHeaderSent=true; break; //success sent if archive file does not exist
    case Response::RSP_ERR_PROTOBUF:                     throw XrdSsiPb::PbException(response.message_txt());
    case Response::RSP_ERR_USER:                         ret = false; isHeaderSent=true; break; //user error sent if archive file does not exist
    case Response::RSP_ERR_CTA:                          throw std::runtime_error(response.message_txt());
    default:                                             throw XrdSsiPb::PbException("Invalid response type.");
  }
  // wait until the data stream has been processed before exiting
  stream_future.wait();

  return ret;
}

//------------------------------------------------------------------------------
// containerExistsEos
//------------------------------------------------------------------------------
 std::tuple<cid, uid, gid> EosNamespaceInjection::getContainerIdsEos(const std::string &diskInstance, const std::string &path) const {
  const auto md_response = m_endpointMapPtr->getMD(diskInstance, ::eos::rpc::CONTAINER, 0, path, false);
  const auto cid = md_response.cmd().id();
  const auto uid = md_response.cmd().uid();
  const auto gid = md_response.cmd().gid();

  std::list<cta::log::Param> params;
  params.push_back(cta::log::Param("containerId", cid));
  if (cid != 0) {
    m_log(cta::log::INFO, "Container exists in the EOS namespace", params);
  } else {
    m_log(cta::log::WARNING, "Container does not exist in the EOS namespace", params);
  }
  return std::make_tuple(cid, uid, gid);
}

//------------------------------------------------------------------------------
// getFileIdEos
//------------------------------------------------------------------------------
uint64_t EosNamespaceInjection::getFileIdEos(const std::string &diskInstance, const std::string &path) const {
  std::list<cta::log::Param> params;
  params.push_back(cta::log::Param("userName", getUsername()));
  params.push_back(cta::log::Param("diskInstance", diskInstance));
  params.push_back(cta::log::Param("path", path));

  m_log(cta::log::DEBUG, "Querying for file metadata in the EOS namespace", params);
  const auto md_response = m_endpointMapPtr->getMD(diskInstance, ::eos::rpc::FILE, 0, path, false);
  const auto fid = md_response.fmd().id();
  return fid;
}


//------------------------------------------------------------------------------
// createFileInEos
//------------------------------------------------------------------------------
uint64_t EosNamespaceInjection::createFileInEos(const MetaDataObject &metaDataFromUser, const uint64_t &parentId, const uint64_t uid, const uint64_t gid) const {
  ::eos::rpc::FileMdProto file;

  const auto& fullPath = metaDataFromUser.eosPath;

  file.set_id(0); // Setting a fid as 0 will tell eos to generate a new ID
  file.set_cont_id(parentId);
  file.set_uid(uid);
  file.set_gid(gid);
  file.set_size(cta::utils::toUint64(metaDataFromUser.size));
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
  file.mutable_checksum()->set_type(metaDataFromUser.checksumType);{{{{{}}}}}
  const auto byteArray = checksum::ChecksumBlob::HexToByteArray(metaDataFromUser.checksumValue);
  file.mutable_checksum()->set_value(std::string(byteArray.rbegin(),byteArray.rend()));

  // Extended attributes:
  //
  // 1. Archive File ID
  std::string archiveId(metaDataFromUser.archiveId);
  file.mutable_xattrs()->insert(google::protobuf::MapPair<std::string,std::string>("sys.archive.file_id", metaDataFromUser.archiveId));
  // 2. Storage Class
  file.mutable_xattrs()->insert(google::protobuf::MapPair<std::string,std::string>("sys.archive.storage_class", g_metaDataObjectCatalogue.storageClass));
  // 3. Birth Time
  // POSIX ATIME (Access Time) is used by CASTOR to store the file creation time. EOS calls this "birth time",
  // but there is no place in the namespace to store it, so it is stored as an extended attribute.
  file.mutable_xattrs()->insert(google::protobuf::MapPair<std::string,std::string>("eos.btime", std::to_string(time)));

  // Indicate that there is a tape-resident replica of this file (except for zero-length files)
  if(file.size() > 0) {
    file.mutable_locations()->Add(65535);
  }

  auto reply = m_endpointMapPtr->fileInsert(metaDataFromUser.diskInstance, file);

  const auto new_fid = getFileIdEos(metaDataFromUser.diskInstance, metaDataFromUser.eosPath);
  return new_fid;

}

//------------------------------------------------------------------------------
// getArchiveFileIdFromEOS
//------------------------------------------------------------------------------
std::pair<ArchiveId, Checksum> EosNamespaceInjection::getArchiveFileIdAndChecksumFromEOS(
  const std::string& diskInstance, const std::string& diskFileId) const {
  const auto fid = strtoul(diskFileId.c_str(), nullptr, 10);
  if(fid < 1) {
    throw std::runtime_error(diskFileId + " is not a valid EOS base-10 disk file ID");
  }
  {
    std::list<cta::log::Param> params;
    params.push_back(cta::log::Param("diskInstance", diskInstance));
    params.push_back(cta::log::Param("diskFileId", diskFileId));
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
    m_log(cta::log::INFO, "Response from EOS nameserver", params);
  }

  return std::make_pair(archiveFileId,checksumValue);
}

//------------------------------------------------------------------------------
// setCmdLineArguments
//------------------------------------------------------------------------------
void EosNamespaceInjection::setCmdLineArguments(const int argc, char *const *const argv) {
  CmdLineArgs cmdLineArgs(argc, argv, StandaloneCliTool::EOS_NAMESPACE_INJECTION);
  auto [serviceProvider, endpointmap] = ConnConfiguration::readAndSetConfiguration(m_log, getUsername(), cmdLineArgs);
  m_serviceProviderPtr = std::move(serviceProvider);
  m_endpointMapPtr     = std::move(endpointmap);

  if(cmdLineArgs.m_help) {
    cmdLineArgs.printUsage(std::cout);
    throw exception::UserError("");
  }

  if(cmdLineArgs.m_json) {
    m_jsonPath = cmdLineArgs.m_json.value();
  } else {
    cmdLineArgs.printUsage(std::cout);
    throw exception::UserError("The required json file was not provided.");
  }
}

//------------------------------------------------------------------------------
// checkEosCtaConsistency
//------------------------------------------------------------------------------
bool EosNamespaceInjection::checkEosCtaConsistency(const uint64_t& archiveId, const std::string& newDiskFileId, const MetaDataObject &metaDataFromUser) {
  getMetaDataFromCatalogue(archiveId);
  const auto [eosArchiveFileId, eosChecksumDecimal] = getArchiveFileIdAndChecksumFromEOS(metaDataFromUser.diskInstance, newDiskFileId);
  const std::string eosChecksum = cta::utils::decimalToHexadecimal(eosChecksumDecimal);
  const auto& ctaChecksum = g_metaDataObjectCatalogue.checksumValue;
  std::list<cta::log::Param> params;
  params.push_back(cta::log::Param("archiveFileId", archiveId));
  params.push_back(cta::log::Param("diskFileId in EOS for new file", newDiskFileId));
  params.push_back(cta::log::Param("diskFileId in Catalogue", g_metaDataObjectCatalogue.diskFileId));
  params.push_back(cta::log::Param("diskInstance in Catalogue", g_metaDataObjectCatalogue.diskInstance));
  params.push_back(cta::log::Param("checksum", ctaChecksum));
  if(eosArchiveFileId == archiveId && eosChecksum == ctaChecksum && g_metaDataObjectCatalogue.diskFileId == newDiskFileId) {
    m_log(cta::log::INFO, "File metadata in EOS and CTA matches", params);
    return true;
  } else {
    params.push_back(cta::log::Param("eosArchiveFileId", eosArchiveFileId));
    params.push_back(cta::log::Param("eosChecksum", eosChecksum));
    m_log(cta::log::WARNING, "File metadata in EOS and CTA does not match", params);
    m_inconsistentMetadata.push_back(metaDataFromUser);
    return false;
  }
}

//------------------------------------------------------------------------------
// pathExists
//------------------------------------------------------------------------------
bool EosNamespaceInjection::pathExists(const uint64_t fid) const {
  return (fid != 0);
}

//------------------------------------------------------------------------------
// checkFileCreated
//------------------------------------------------------------------------------
void EosNamespaceInjection::checkFileCreated(const uint64_t newFid) {
  if (pathExists(newFid)) {
    std::list<cta::log::Param> params;
    params.push_back(cta::log::Param("diskFileId", newFid));
    m_log(cta::log::INFO, "File was created in the EOS namespace", params);
  } else {
    std::list<cta::log::Param> params;
    params.push_back(cta::log::Param("diskFileId", newFid));
    m_log(cta::log::WARNING, "Could not find file in the EOS namespace. Check that gRPC authentication is set up correctly, and that the path exists", params);
  }
}

//------------------------------------------------------------------------------
// checkParentContainerExists
//------------------------------------------------------------------------------
void EosNamespaceInjection::checkParentContainerExists(const uint64_t parentId, const std::string& enclosingPath) const {
  if(!pathExists(parentId)) {
    throw exception::UserError("Could not find: " + enclosingPath + ". Check that gRPC authentication is set up correctly, and that the path exists");
  }
}

//------------------------------------------------------------------------------
// checkArchiveIdExistsInCatalogue
//------------------------------------------------------------------------------
void EosNamespaceInjection::checkArchiveIdExistsInCatalogue(const uint64_t &archiveId) const {
  if(!getMetaDataFromCatalogue(archiveId)) {
    throw exception::UserError("archive file id " + std::to_string(archiveId) + " does not exist");
  }
}

//------------------------------------------------------------------------------
// checkExistingPathHasInvalidMetadata
//------------------------------------------------------------------------------
void EosNamespaceInjection::checkExistingPathHasInvalidMetadata(const uint64_t &archiveId, const uint64_t& fid, const MetaDataObject& metaDataFromUser) {
  if(!checkEosCtaConsistency(archiveId, std::to_string(fid), metaDataFromUser)) {
    throw cta::cliTool::EosNameSpaceInjectionError("The file with path " + metaDataFromUser.eosPath + " already exists for instance " + metaDataFromUser.diskInstance + ". This tool does not overwrite existing files");
  }
}

//------------------------------------------------------------------------------
// writeSkippedArchiveIdsToFile
//------------------------------------------------------------------------------
void EosNamespaceInjection::createTxtFileWithSkippedMetadata() const {
  auto unix_epoch_time = std::time(nullptr);
  const std::string currentTime = std::to_string(unix_epoch_time);
  const std::filesystem::path filePath = "/tmp/skippedMetadataEosInjection" + currentTime + ".txt";
  std::ofstream archiveIdFile(filePath);

  if (archiveIdFile.fail()) {
    throw std::runtime_error("Unable to open file " + filePath.string());
  }

  if (archiveIdFile.is_open()) {
    for (const auto& metadata : m_inconsistentMetadata) {
      archiveIdFile          <<
      "{ eosPath: "          << metadata.eosPath       <<
      " , diskInstance: "    << metadata.diskInstance  <<
      " , archiveId: "       << metadata.archiveId     <<
      " , size: "            << metadata.size          <<
      " , checksumType: "    << metadata.checksumType  <<
      " , checksumValue: "   << metadata.checksumValue <<
      " }" << std::endl;
    }
    archiveIdFile.close();
    std::cout << m_inconsistentMetadata.size() << " entries finished with inconsistent metadata." << std::endl;
    std::cout << "The skipped metadata can be found here: " << filePath << std::endl;
  }
}

} // namespace cta::cliTool
