/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "cmdline/CtaAdminCmdParser.hpp"
#include "cmdline/CtaAdminParsedCmd.hpp"
#include "version.h"
#include "frontend/common/PbException.hpp"
#include "common/exception/UserError.hpp"

#include <grpcpp/grpcpp.h>
#include <google/protobuf/util/json_util.h>

#include "cta_frontend.pb.h"
#include "cta_frontend.grpc.pb.h"

#include <condition_variable>
#include <variant>

constexpr unsigned int cmd_pair(cta::admin::AdminCmd::Cmd cmd, cta::admin::AdminCmd::SubCmd subcmd) {
  return (cmd << 16) + subcmd;
}

static std::string DumpProtobuf(const google::protobuf::Message* message) {
  using namespace google::protobuf::util;

  std::string logstring;
  JsonPrintOptions options;

#if GOOGLE_PROTOBUF_VERSION >= 5027000
  options.always_print_fields_with_no_presence = true;
#else
  options.always_print_primitive_fields = true;
#endif
  MessageToJsonString(*message, &logstring, options);

  return logstring;
}

// This is a virtual (maybe not all of its methods) class, each command implementation will inherit from this
class CtaAdminClientReadReactor : public grpc::ClientReadReactor<cta::xrd::StreamResponse> {
public:
  void OnDone(const ::grpc::Status& s) override {
    std::unique_lock<std::mutex> l(mu_);
    status_ = s;
    done_ = true;
    cv_.notify_one();
  }

  ::grpc::Status Await() {
    std::unique_lock<std::mutex> l(mu_);
    cv_.wait(l, [this] { return done_; });
    return std::move(status_);
  }

  virtual void OnReadDone(bool ok) override {
    if (ok) {
      if (m_response.has_header() && !m_isJson) {
        switch (m_response.header().type()) {
          case cta::xrd::Response::RSP_SUCCESS:
            switch (m_response.header().show_header()) {
              case cta::admin::HeaderType::ADMIN_LS:
                m_textFormatter.printAdminLsHeader();
                break;
              case cta::admin::HeaderType::ARCHIVEROUTE_LS:
                m_textFormatter.printArchiveRouteLsHeader();
                break;
              case cta::admin::HeaderType::DRIVE_LS:
                m_textFormatter.printDriveLsHeader();
                break;
              case cta::admin::HeaderType::FAILEDREQUEST_LS:
                m_textFormatter.printFailedRequestLsHeader();
                break;
              case cta::admin::HeaderType::FAILEDREQUEST_LS_SUMMARY:
                m_textFormatter.printFailedRequestLsSummaryHeader();
                break;
              case cta::admin::HeaderType::GROUPMOUNTRULE_LS:
                m_textFormatter.printGroupMountRuleLsHeader();
                break;
              case cta::admin::HeaderType::LISTPENDINGARCHIVES:
                m_textFormatter.printListPendingArchivesHeader();
                break;
              case cta::admin::HeaderType::LISTPENDINGARCHIVES_SUMMARY:
                m_textFormatter.printListPendingArchivesSummaryHeader();
                break;
              case cta::admin::HeaderType::LISTPENDINGRETRIEVES:
                m_textFormatter.printListPendingRetrievesHeader();
                break;
              case cta::admin::HeaderType::LISTPENDINGRETRIEVES_SUMMARY:
                m_textFormatter.printListPendingRetrievesSummaryHeader();
                break;
              case cta::admin::HeaderType::LOGICALLIBRARY_LS:
                m_textFormatter.printLogicalLibraryLsHeader();
                break;
              case cta::admin::HeaderType::MOUNTPOLICY_LS:
                m_textFormatter.printMountPolicyLsHeader();
                break;
              case cta::admin::HeaderType::REPACK_LS:
                m_textFormatter.printRepackLsHeader();
                break;
              case cta::admin::HeaderType::REQUESTERMOUNTRULE_LS:
                m_textFormatter.printRequesterMountRuleLsHeader();
                break;
              case cta::admin::HeaderType::ACTIVITYMOUNTRULE_LS:
                m_textFormatter.printActivityMountRuleLsHeader();
                break;
              case cta::admin::HeaderType::SHOWQUEUES:
                m_textFormatter.printShowQueuesHeader();
                break;
              case cta::admin::HeaderType::STORAGECLASS_LS:
                m_textFormatter.printStorageClassLsHeader();
                break;
              case cta::admin::HeaderType::TAPE_LS:
                m_textFormatter.printTapeLsHeader();
                break;
              case cta::admin::HeaderType::TAPEFILE_LS:
                m_textFormatter.printTapeFileLsHeader();
                break;
              case cta::admin::HeaderType::TAPEPOOL_LS:
                m_textFormatter.printTapePoolLsHeader();
                break;
              case cta::admin::HeaderType::DISKSYSTEM_LS:
                m_textFormatter.printDiskSystemLsHeader();
                break;
              case cta::admin::HeaderType::DISKINSTANCE_LS:
                m_textFormatter.printDiskInstanceLsHeader();
                break;
              case cta::admin::HeaderType::DISKINSTANCESPACE_LS:
                m_textFormatter.printDiskInstanceSpaceLsHeader();
                break;
              case cta::admin::HeaderType::VIRTUALORGANIZATION_LS:
                m_textFormatter.printVirtualOrganizationLsHeader();
                break;
              case cta::admin::HeaderType::VERSION_CMD:
                m_textFormatter.printVersionHeader();
                break;
              case cta::admin::HeaderType::MEDIATYPE_LS:
                m_textFormatter.printMediaTypeLsHeader();
                break;
              case cta::admin::HeaderType::RECYLETAPEFILE_LS:
                m_textFormatter.printRecycleTapeFileLsHeader();
                break;
              case cta::admin::HeaderType::PHYSICALLIBRARY_LS:
                m_textFormatter.printPhysicalLibraryLsHeader();
                break;
              case cta::admin::HeaderType::NONE:
              default:
                break;
            }
            break;
          case cta::xrd::Response::RSP_ERR_PROTOBUF:
            throw cta::exception::PbException(m_response.header().message_txt());
          case cta::xrd::Response::RSP_ERR_USER:
            throw cta::exception::UserError(m_response.header().message_txt());
          case cta::xrd::Response::RSP_ERR_CTA:
            throw std::runtime_error(m_response.header().message_txt());
          default:
            throw cta::exception::PbException("Invalid response type.");
            // strErrorMsg = m_response.header().message_txt();
            // need to log an ERROR here, but figure out later how to do this
        }
      } else if (m_response.has_data()) {
        using value_t = std::variant<cta::admin::TapeLsItem,
                                     cta::admin::StorageClassLsItem,
                                     cta::admin::TapePoolLsItem,
                                     cta::admin::VirtualOrganizationLsItem,
                                     cta::admin::DiskInstanceLsItem,
                                     cta::admin::DriveLsItem,
                                     cta::admin::AdminLsItem,
                                     cta::admin::VersionItem,
                                     cta::admin::ArchiveRouteLsItem,
                                     cta::admin::FailedRequestLsItem,
                                     cta::admin::GroupMountRuleLsItem,
                                     cta::admin::LogicalLibraryLsItem,
                                     cta::admin::PhysicalLibraryLsItem,
                                     cta::admin::MediaTypeLsItem,
                                     cta::admin::MountPolicyLsItem,
                                     cta::admin::RepackLsItem,
                                     cta::admin::RequesterMountRuleLsItem,
                                     cta::admin::ActivityMountRuleLsItem,
                                     cta::admin::ShowQueuesItem,
                                     cta::admin::TapeFileLsItem,
                                     cta::admin::DiskSystemLsItem,
                                     cta::admin::DiskInstanceSpaceLsItem,
                                     cta::admin::RecycleTapeFileLsItem>;

        auto visitor = [this](const auto& item) {
          if (m_isJson) {
            std::cout << cta::admin::CtaAdminParsedCmd::jsonDelim();
            std::cout << DumpProtobuf(&item);
          } else {
            m_textFormatter.print(item);
          }
        };

        // clang-format off
                switch (m_response.data().data_case()) {
                    case cta::xrd::Data::kTalsItem:  std::visit(visitor, value_t{m_response.data().tals_item()});  break;
                    case cta::xrd::Data::kSclsItem:  std::visit(visitor, value_t{m_response.data().scls_item()});  break;
                    case cta::xrd::Data::kTplsItem:  std::visit(visitor, value_t{m_response.data().tpls_item()});  break;
                    case cta::xrd::Data::kVolsItem:  std::visit(visitor, value_t{m_response.data().vols_item()});  break;
                    case cta::xrd::Data::kDilsItem:  std::visit(visitor, value_t{m_response.data().dils_item()});  break;
                    case cta::xrd::Data::kDrlsItem:  std::visit(visitor, value_t{m_response.data().drls_item()});  break;
                    case cta::xrd::Data::kAdlsItem:  std::visit(visitor, value_t{m_response.data().adls_item()});  break;
                    case cta::xrd::Data::kVersionItem: std::visit(visitor, value_t{m_response.data().version_item()}); break;
                    case cta::xrd::Data::kArlsItem:  std::visit(visitor, value_t{m_response.data().arls_item()});  break;
                    case cta::xrd::Data::kFrlsItem:  std::visit(visitor, value_t{m_response.data().frls_item()});  break;
                    case cta::xrd::Data::kGmrlsItem: std::visit(visitor, value_t{m_response.data().gmrls_item()}); break;
                    case cta::xrd::Data::kLllsItem:  std::visit(visitor, value_t{m_response.data().llls_item()});  break;
                    case cta::xrd::Data::kPllsItem:  std::visit(visitor, value_t{m_response.data().plls_item()});  break;
                    case cta::xrd::Data::kMtlsItem:  std::visit(visitor, value_t{m_response.data().mtls_item()});  break;
                    case cta::xrd::Data::kMplsItem:  std::visit(visitor, value_t{m_response.data().mpls_item()});  break;
                    case cta::xrd::Data::kRelsItem:  std::visit(visitor, value_t{m_response.data().rels_item()});  break;
                    case cta::xrd::Data::kRmrlsItem: std::visit(visitor, value_t{m_response.data().rmrls_item()}); break;
                    case cta::xrd::Data::kAmrlsItem: std::visit(visitor, value_t{m_response.data().amrls_item()}); break;
                    case cta::xrd::Data::kSqItem:    std::visit(visitor, value_t{m_response.data().sq_item()});    break;
                    case cta::xrd::Data::kTflsItem:  std::visit(visitor, value_t{m_response.data().tfls_item()});  break;
                    case cta::xrd::Data::kDslsItem:  std::visit(visitor, value_t{m_response.data().dsls_item()});  break;
                    case cta::xrd::Data::kDislsItem: std::visit(visitor, value_t{m_response.data().disls_item()}); break;
                    case cta::xrd::Data::kRtflsItem: std::visit(visitor, value_t{m_response.data().rtfls_item()}); break;
                    default: break;
                }
        // clang-format on
      }
      // just read the next input from the server until done (ok is false)
      StartRead(&m_response);  // consume the next response
    }  // if (ok)
  }

  CtaAdminClientReadReactor(cta::xrd::CtaRpcStream::Stub* client_stub, const cta::admin::CtaAdminParsedCmd& parsedCmd) {
    const auto request = parsedCmd.getRequest();
    client_stub->async()->GenericAdminStream(&m_context, &request, this);
    m_isJson = parsedCmd.isJson();
    StartRead(&m_response);  // where to store the received response?
    StartCall();             // activate the RPC!
  }

private:
  ::grpc::ClientContext m_context;
  cta::xrd::StreamResponse m_response;
  std::mutex mu_;
  std::condition_variable cv_;
  ::grpc::Status status_;
  bool done_ = false;
  cta::admin::TextFormatter m_textFormatter;
  bool m_isJson;
};
