/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
*/

#include "CtaFrontendApi.hpp"
#include "tapeserver/daemon/common/TapedConfiguration.hpp"

#include <XrdSsiPbIStreamBuffer.hpp>
#include <XrdSsiPbLog.hpp>
#include <filesystem>
#include <iostream>
#include <sstream>
#include <tools/CtaAdminTextFormatter.hpp>
#include <tools/CtaAdminXrdCmd.hpp>

// GLOBAL VARIABLES : used to pass information between main thread and stream handler thread

// global synchronisation flag
std::atomic<bool> isHeaderSent(false);

// initialise an output buffer of 1000 lines
cta::admin::TextFormatter formattedText(1000);

namespace XrdSsiPb {

/*!
* User error exception
*/
class UserException : public std::runtime_error {
  using std::runtime_error::runtime_error;
};

/*!
* Alert callback.
*
* Defines how Alert messages should be logged
*/
template<>
void RequestCallback<cta::xrd::Alert>::operator()(const cta::xrd::Alert& alert) {
  std::cout << "AlertCallback():" << std::endl;
  Log::DumpProtobuf(Log::PROTOBUF, &alert);
}

/*!
* Data/Stream callback.
*
* Defines how incoming records from the stream should be handled
*/
template<>
void IStreamBuffer<cta::xrd::Data>::DataCallback(const cta::xrd::Data& record) const {
  using namespace cta::xrd;
  using namespace cta::admin;

  // Wait for primary response to be handled before allowing stream response
  while (!isHeaderSent) {
    std::this_thread::yield();
  }

  // Output results in JSON format for parsing by a script
  if (CtaAdminParsedCmd::isJson()) {
    std::cout << CtaAdminParsedCmd::jsonDelim();
    // clang-format off
    switch(record.data_case()) {
      case Data::kAdlsItem:      std::cout << Log::DumpProtobuf(&record.adls_item());    break;
      case Data::kAflsItem:      std::cout << Log::DumpProtobuf(&record.afls_item());    break;
      case Data::kAflsSummary:   std::cout << Log::DumpProtobuf(&record.afls_summary()); break;
      case Data::kArlsItem:      std::cout << Log::DumpProtobuf(&record.arls_item());    break;
      case Data::kDrlsItem:      std::cout << Log::DumpProtobuf(&record.drls_item());    break;
      case Data::kFrlsItem:      std::cout << Log::DumpProtobuf(&record.frls_item());    break;
      case Data::kFrlsSummary:   std::cout << Log::DumpProtobuf(&record.frls_summary()); break;
      case Data::kGmrlsItem:     std::cout << Log::DumpProtobuf(&record.gmrls_item());   break;
      case Data::kLpaItem:       std::cout << Log::DumpProtobuf(&record.lpa_item());     break;
      case Data::kLpaSummary:    std::cout << Log::DumpProtobuf(&record.lpa_summary());  break;
      case Data::kLprItem:       std::cout << Log::DumpProtobuf(&record.lpr_item());     break;
      case Data::kLprSummary:    std::cout << Log::DumpProtobuf(&record.lpr_summary());  break;
      case Data::kLllsItem:      std::cout << Log::DumpProtobuf(&record.llls_item());    break;
      case Data::kMplsItem:      std::cout << Log::DumpProtobuf(&record.mpls_item());    break;
      case Data::kRelsItem:      std::cout << Log::DumpProtobuf(&record.rels_item());    break;
      case Data::kRmrlsItem:     std::cout << Log::DumpProtobuf(&record.rmrls_item());   break;
      case Data::kAmrlsItem:     std::cout << Log::DumpProtobuf(&record.amrls_item());   break;
      case Data::kSqItem:        std::cout << Log::DumpProtobuf(&record.sq_item());      break;
      case Data::kSclsItem:      std::cout << Log::DumpProtobuf(&record.scls_item());    break;
      case Data::kTalsItem:      std::cout << Log::DumpProtobuf(&record.tals_item());    break;
      case Data::kTflsItem:      std::cout << Log::DumpProtobuf(&record.tfls_item());    break;
      case Data::kTplsItem:      std::cout << Log::DumpProtobuf(&record.tpls_item());    break;
      case Data::kDslsItem:      std::cout << Log::DumpProtobuf(&record.dsls_item());    break;
      case Data::kDilsItem:      std::cout << Log::DumpProtobuf(&record.dils_item());    break;
      case Data::kDislsItem:     std::cout << Log::DumpProtobuf(&record.disls_item());   break;
      case Data::kVolsItem:      std::cout << Log::DumpProtobuf(&record.vols_item());    break;
      case Data::kVersionItem:   std::cout << Log::DumpProtobuf(&record.version_item()); break;
      case Data::kMtlsItem:      std::cout << Log::DumpProtobuf(&record.mtls_item());    break;
      case Data::kRtflsItem:     std::cout << Log::DumpProtobuf(&record.rtfls_item());   break;
      case Data::kPllsItem:      std::cout << Log::DumpProtobuf(&record.plls_item());    break;
      default:
        throw std::runtime_error("Received invalid stream data from CTA Frontend.");
    }
    // clang-format on
  } else {
    // Format results in a tabular format for a human
    switch (record.data_case()) {
        // clang-format off
      case Data::kAdlsItem:      formattedText.print(record.adls_item());    break;
      case Data::kArlsItem:      formattedText.print(record.arls_item());    break;
      case Data::kDrlsItem:      formattedText.print(record.drls_item());    break;
      case Data::kFrlsItem:      formattedText.print(record.frls_item());    break;
      case Data::kFrlsSummary:   formattedText.print(record.frls_summary()); break;
      case Data::kGmrlsItem:     formattedText.print(record.gmrls_item());   break;
      case Data::kLpaItem:       formattedText.print(record.lpa_item());     break;
      case Data::kLpaSummary:    formattedText.print(record.lpa_summary());  break;
      case Data::kLprItem:       formattedText.print(record.lpr_item());     break;
      case Data::kLprSummary:    formattedText.print(record.lpr_summary());  break;
      case Data::kLllsItem:      formattedText.print(record.llls_item());    break;
      case Data::kMplsItem:      formattedText.print(record.mpls_item());    break;
      case Data::kRelsItem:      formattedText.print(record.rels_item());    break;
      case Data::kRmrlsItem:     formattedText.print(record.rmrls_item());   break;
      case Data::kAmrlsItem:     formattedText.print(record.amrls_item());   break;
      case Data::kSqItem:        formattedText.print(record.sq_item());      break;
      case Data::kSclsItem:      formattedText.print(record.scls_item());    break;
      case Data::kTalsItem:      formattedText.print(record.tals_item());    break;
      case Data::kTflsItem:      formattedText.print(record.tfls_item());    break;
      case Data::kTplsItem:      formattedText.print(record.tpls_item());    break;
      case Data::kDslsItem:      formattedText.print(record.dsls_item());    break;
      case Data::kDilsItem:      formattedText.print(record.dils_item());    break;
      case Data::kDislsItem:     formattedText.print(record.disls_item());   break;
      case Data::kVolsItem:      formattedText.print(record.vols_item());    break;
      case Data::kVersionItem:   formattedText.print(record.version_item()); break;
      case Data::kMtlsItem:      formattedText.print(record.mtls_item());    break;
      case Data::kRtflsItem:     formattedText.print(record.rtfls_item());   break;
      case Data::kPllsItem:      formattedText.print(record.plls_item());    break;
      default:
        throw std::runtime_error("Received invalid stream data from CTA Frontend.");
        // clang-format on
    }
  }
}
}  // namespace XrdSsiPb

namespace cta::admin {

void CtaAdminXrdCmd::send(const CtaAdminParsedCmd& parsedCmd) const {
  // Validate the Protocol Buffer
  const auto& request = parsedCmd.getRequest();
  try {
    validateCmd(request.admincmd());
  } catch (std::runtime_error& ex) {
    parsedCmd.throwUsage(ex.what());
  }

  const std::filesystem::path config_file = parsedCmd.getConfigFilePath();

  // Set configuration options
  XrdSsiPb::Config config(config_file, "cta");
  config.set("resource", "/ctafrontend");
  config.set("response_bufsize", StreamBufferSize);      // default value = 1024 bytes
  config.set("request_timeout", DefaultRequestTimeout);  // default value = 10s

  // Allow environment variables to override config file
  config.getEnv("request_timeout", "XRD_REQUESTTIMEOUT");

  // If XRDDEBUG=1, switch on all logging
  if (getenv("XRDDEBUG")) {
    config.set("log", "all");
  }
  // If fine-grained control over log level is required, use XrdSsiPbLogLevel
  config.getEnv("log", "XrdSsiPbLogLevel");

  // Validate that endpoint was specified in the config file
  if (!config.getOptionValueStr("endpoint").first) {
    throw std::runtime_error("Configuration error: cta.endpoint missing from " + config_file.string());
  }

  // Check drive timeout value
  auto [driveTimeoutConfigExists, driveTimeoutVal] = config.getOptionValueInt("drive_timeout");
  if (driveTimeoutConfigExists) {
    if (driveTimeoutVal > 0) {
      formattedText.setDriveTimeout(driveTimeoutVal);
    } else {
      throw std::runtime_error("Configuration error: cta.drive_timeout not a positive value in "
                               + config_file.string());
    }
  }

  // If the server is down, we want an immediate failure. Set client retry to a single attempt.
  XrdSsiProviderClient->SetTimeout(XrdSsiProvider::connect_N, 1);

  // Obtain a Service Provider
  XrdSsiPbServiceType cta_service(config);

  // Send the Request to the Service and get a Response
  cta::xrd::Response response;
  auto stream_future = cta_service.SendAsync(request, response, false);

  // Handle responses
  switch (response.type()) {
    using namespace cta::xrd;
    using namespace cta::admin;

    case Response::RSP_SUCCESS:
      // Print message text
      std::cout << response.message_txt();
      // Print streaming response header
      if (!parsedCmd.isJson()) {
        switch (response.show_header()) {
            // clang-format off
       case HeaderType::ADMIN_LS:                     formattedText.printAdminLsHeader(); break;
       case HeaderType::ARCHIVEROUTE_LS:              formattedText.printArchiveRouteLsHeader(); break;
       case HeaderType::DRIVE_LS:                     formattedText.printDriveLsHeader(); break;
       case HeaderType::FAILEDREQUEST_LS:             formattedText.printFailedRequestLsHeader(); break;
       case HeaderType::FAILEDREQUEST_LS_SUMMARY:     formattedText.printFailedRequestLsSummaryHeader(); break;
       case HeaderType::GROUPMOUNTRULE_LS:            formattedText.printGroupMountRuleLsHeader(); break;
       case HeaderType::LISTPENDINGARCHIVES:          formattedText.printListPendingArchivesHeader(); break;
       case HeaderType::LISTPENDINGARCHIVES_SUMMARY:  formattedText.printListPendingArchivesSummaryHeader(); break;
       case HeaderType::LISTPENDINGRETRIEVES:         formattedText.printListPendingRetrievesHeader(); break;
       case HeaderType::LISTPENDINGRETRIEVES_SUMMARY: formattedText.printListPendingRetrievesSummaryHeader(); break;
       case HeaderType::LOGICALLIBRARY_LS:            formattedText.printLogicalLibraryLsHeader(); break;
       case HeaderType::MOUNTPOLICY_LS:               formattedText.printMountPolicyLsHeader(); break;
       case HeaderType::REPACK_LS:                    formattedText.printRepackLsHeader(); break;
       case HeaderType::REQUESTERMOUNTRULE_LS:        formattedText.printRequesterMountRuleLsHeader(); break;
       case HeaderType::ACTIVITYMOUNTRULE_LS:         formattedText.printActivityMountRuleLsHeader(); break;
       case HeaderType::SHOWQUEUES:                   formattedText.printShowQueuesHeader(); break;
       case HeaderType::STORAGECLASS_LS:              formattedText.printStorageClassLsHeader(); break;
       case HeaderType::TAPE_LS:                      formattedText.printTapeLsHeader(); break;
       case HeaderType::TAPEFILE_LS:                  formattedText.printTapeFileLsHeader(); break;
       case HeaderType::TAPEPOOL_LS:                  formattedText.printTapePoolLsHeader(); break;
       case HeaderType::DISKSYSTEM_LS:                formattedText.printDiskSystemLsHeader(); break;
       case HeaderType::DISKINSTANCE_LS:              formattedText.printDiskInstanceLsHeader(); break;
       case HeaderType::DISKINSTANCESPACE_LS:         formattedText.printDiskInstanceSpaceLsHeader(); break;
       case HeaderType::VIRTUALORGANIZATION_LS:       formattedText.printVirtualOrganizationLsHeader(); break;
       case HeaderType::VERSION_CMD:                  formattedText.printVersionHeader(); break;
       case HeaderType::MEDIATYPE_LS:                 formattedText.printMediaTypeLsHeader(); break;
       case HeaderType::RECYLETAPEFILE_LS:            formattedText.printRecycleTapeFileLsHeader(); break;
       case HeaderType::PHYSICALLIBRARY_LS:           formattedText.printPhysicalLibraryLsHeader(); break;
       case HeaderType::NONE:
       default:                                       break;
            // clang-format on
        }
      }
      // Allow stream processing to commence
      isHeaderSent = true;
      break;
    case Response::RSP_ERR_PROTOBUF:
      throw XrdSsiPb::PbException(response.message_txt());
    case Response::RSP_ERR_USER:
      throw XrdSsiPb::UserException(response.message_txt());
    case Response::RSP_ERR_CTA:
      throw std::runtime_error(response.message_txt());
    default:
      throw XrdSsiPb::PbException("Invalid response type.");
  }

  // clang-format on

  // If there is a Data/Stream payload, wait until it has been processed before exiting
  stream_future.wait();

  // JSON output is an array of structs, close bracket
  if (parsedCmd.isJson()) {
    std::cout << CtaAdminParsedCmd::jsonCloseDelim();
  }
}  // namespace cta::admin

}  // namespace cta::admin

/*!
* Start here
*
* @param    argc[in]    The number of command-line arguments
* @param    argv[in]    The command-line arguments
*/

int main(int argc, const char** argv) {
  using namespace cta::admin;

  try {
    // Parse the command line arguments
    CtaAdminParsedCmd parsedCmd(argc, argv);
    CtaAdminXrdCmd cmd;

    // Send the protocol buffer
    cmd.send(parsedCmd);

    // Delete all global objects allocated by libprotobuf
    google::protobuf::ShutdownProtobufLibrary();

    return 0;
  } catch (XrdSsiPb::PbException& ex) {
    std::cerr << "Error in Google Protocol Buffers: " << ex.what() << std::endl;
  } catch (XrdSsiPb::XrdSsiException& ex) {
    std::cerr << "Error from XRootD SSI Framework: " << ex.what() << std::endl;
  } catch (XrdSsiPb::UserException& ex) {
    if (CtaAdminParsedCmd::isJson()) {
      std::cout << CtaAdminParsedCmd::jsonCloseDelim();
    }
    std::cerr << ex.what() << std::endl;
    return 2;
  } catch (std::runtime_error& ex) {
    std::cerr << ex.what() << std::endl;
  } catch (std::exception& ex) {
    std::cerr << "Caught exception: " << ex.what() << std::endl;
  } catch (...) {
    std::cerr << "Caught an unknown exception" << std::endl;
  }

  return 1;
}
