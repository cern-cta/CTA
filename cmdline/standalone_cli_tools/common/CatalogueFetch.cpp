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

#include <list>
#include <iostream>
#include <string>
#include <utility>

#include <XrdSsiPbLog.hpp>
#include <XrdSsiPbIStreamBuffer.hpp>

#include "cmdline/standalone_cli_tools/common/CatalogueFetch.hpp"
#include "common/exception/UserError.hpp"
#include "common/log/StdoutLogger.hpp"
#include "common/utils/utils.hpp"
#include "cta_frontend.pb.h"                               //!< Auto-generated message types from .proto file

#include "version.h"

// GLOBAL VARIABLES : used to pass information between main thread and stream handler thread

// global synchronisation flag
std::atomic<bool> isHeaderSent = false;

std::list<cta::admin::RecycleTapeFileLsItem> deletedTapeFiles;
std::list<std::pair<std::string,std::string>> listedTapeFiles;
std::list<std::string> g_storageClasses;
std::list<std::string> g_listedVids;

namespace XrdSsiPb {

/*!
 * Alert callback.
 *
 * Defines how Alert messages should be logged
 */
template<>
void RequestCallback<cta::xrd::Alert>::operator()(const cta::xrd::Alert &alert)
{
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
      }
      break;
    case Data::kTflsItem:
      {
        auto item = record.tfls_item();
        auto instanceAndFid = std::make_pair(item.df().disk_instance(), item.df().disk_id());
        listedTapeFiles.push_back(instanceAndFid);
      }
      break;
    case Data::kSclsItem:
      {
        const auto item = record.scls_item();
        g_storageClasses.push_back(item.name());
      }
      break;
    case Data::kTalsItem:
      {
        const auto item = record.tals_item();
        g_listedVids.push_back(item.vid());
      }
      break;
    default:
      throw std::runtime_error("Received invalid stream data from CTA Frontend for the cta-restore-deleted-files command.");
   }
}

} // namespace XrdSsiPb

namespace cta {
namespace cliTool {

std::tuple<std::string,std::string> CatalogueFetch::getInstanceAndFid(const std::string& archiveFileId, std::unique_ptr<XrdSsiPbServiceType> &serviceProviderPtr, cta::log::StdoutLogger &log) {
  {
    std::list<cta::log::Param> params;
    params.push_back(cta::log::Param("archiveFileId", archiveFileId));
    log(cta::log::DEBUG, "getInstanceAndFidFromCTA() ", params);
  }

  cta::xrd::Request request;
  auto admincmd = request.mutable_admincmd();

  request.set_client_cta_version(CTA_VERSION);
  request.set_client_xrootd_ssi_protobuf_interface_version(XROOTD_SSI_PROTOBUF_INTERFACE_VERSION);
  admincmd->set_cmd(cta::admin::AdminCmd::CMD_TAPEFILE);
  admincmd->set_subcmd(cta::admin::AdminCmd::SUBCMD_LS);
  auto new_opt = admincmd->add_option_uint64();
  new_opt->set_key(cta::admin::OptionUInt64::ARCHIVE_FILE_ID);
  new_opt->set_value(cta::utils::toUint64(archiveFileId));

  handleResponse(request, serviceProviderPtr);

  if(listedTapeFiles.size() != 1) {
    throw std::runtime_error("Unexpected result set: listedTapeFiles size expected=1 received=" + std::to_string(listedTapeFiles.size()));
  }
  auto listedTapeFile = listedTapeFiles.back();
  listedTapeFiles.clear();
  {
    std::list<cta::log::Param> params;
    params.push_back(cta::log::Param("diskInstance", listedTapeFile.first));
    params.push_back(cta::log::Param("diskFileId", listedTapeFile.second));
    log(cta::log::DEBUG, "Obtained file metadata from CTA", params);
  }
  return listedTapeFile;
}

std::list<std::string> CatalogueFetch::getVids(std::unique_ptr<XrdSsiPbServiceType> &serviceProviderPtr, cta::log::StdoutLogger &log) {
  cta::xrd::Request request;
  auto admincmd = request.mutable_admincmd();

  request.set_client_cta_version(CTA_VERSION);
  request.set_client_xrootd_ssi_protobuf_interface_version(XROOTD_SSI_PROTOBUF_INTERFACE_VERSION);
  admincmd->set_cmd(cta::admin::AdminCmd::CMD_TAPE);
  admincmd->set_subcmd(cta::admin::AdminCmd::SUBCMD_LS);

  auto new_opt = admincmd->add_option_bool();
  new_opt->set_key(cta::admin::OptionBoolean::ALL);
  new_opt->set_value(true);

  handleResponse(request, serviceProviderPtr);

  return g_listedVids;
}

void CatalogueFetch::handleResponse(const cta::xrd::Request &request, std::unique_ptr<XrdSsiPbServiceType> &serviceProviderPtr) {
  // Send the Request to the Service and get a Response
  cta::xrd::Response response;
  auto stream_future = serviceProviderPtr->SendAsync(request, response);

  // Handle responses
  switch(response.type()) {
    using namespace cta::xrd;
    using namespace cta::admin;
    case Response::RSP_SUCCESS:
      // Print message text
      std::cout << response.message_txt();
      // Allow stream processing to commence
      isHeaderSent = true;
      break;
    case Response::RSP_ERR_PROTOBUF:                     throw XrdSsiPb::PbException(response.message_txt());
    case Response::RSP_ERR_USER:                         throw exception::UserError(response.message_txt());
    case Response::RSP_ERR_CTA:                          throw std::runtime_error(response.message_txt());
    default:                                             throw XrdSsiPb::PbException("Invalid response type.");
  }

  // wait until the data stream has been processed before exiting
  stream_future.wait();
}

} // cliTool
} // cta