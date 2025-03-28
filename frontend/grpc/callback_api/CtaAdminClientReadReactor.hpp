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

 #pragma once

 #include "cmdline/CtaAdminCmdParse.hpp"
 #include "version.h"
 #include <grpcpp/grpcpp.h>
 // #include <grpcpp/resource_quota.h>
 // #include <grpcpp/security/server_credentials.h>
 
 // #include <scheduler/Scheduler.hpp> // what happens if I skip this?
 // #include "common/log/Logger.hpp"
 #include "cta_frontend.pb.h"
 #include "cta_frontend.grpc.pb.h"

constexpr unsigned int cmd_pair(cta::admin::AdminCmd::Cmd cmd, cta::admin::AdminCmd::SubCmd subcmd) {
    return (cmd << 16) + subcmd;
}

// This is a virtual (maybe not all of its methods) class, each command implementation will inherit from this
class CtaAdminClientReadReactor : public grpc::ClientReadReactor<cta::xrd::StreamResponse> {
public:
    void OnDone(const ::grpc::Status &s) override {
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
    // with a Completion Queue, we would have a state machine transitioning header -> data
    // do I need to check for header/data here?
    virtual void OnReadDone(bool ok) override {
        std::cout << "In CtaAdminCmdStream, inside OnReadDone() " << std::endl;
        if (!ok) {
            std::cout << "Something went wrong with reading the response in the client" << std::endl;
        } else {
            // if this is the header, print the formatted header I guess?
            if (m_response.has_header()) {
                std::cout << "We received the header on the client side " << std::endl;
                switch (m_response.header().type()) {
                    case cta::xrd::Response::RSP_SUCCESS:
                        std::cout << "The header type was set to SUCCESS" << std::endl;
                        switch (m_response.header().show_header()) {
                            case cta::admin::HeaderType::ADMIN_LS:                     m_textFormatter.printAdminLsHeader(); break;
                            case cta::admin::HeaderType::ARCHIVEROUTE_LS:              m_textFormatter.printArchiveRouteLsHeader(); break;
                            case cta::admin::HeaderType::DRIVE_LS:                     m_textFormatter.printDriveLsHeader(); break;
                            case cta::admin::HeaderType::FAILEDREQUEST_LS:             m_textFormatter.printFailedRequestLsHeader(); break;
                            case cta::admin::HeaderType::FAILEDREQUEST_LS_SUMMARY:     m_textFormatter.printFailedRequestLsSummaryHeader(); break;
                            case cta::admin::HeaderType::GROUPMOUNTRULE_LS:            m_textFormatter.printGroupMountRuleLsHeader(); break;
                            case cta::admin::HeaderType::LISTPENDINGARCHIVES:          m_textFormatter.printListPendingArchivesHeader(); break;
                            case cta::admin::HeaderType::LISTPENDINGARCHIVES_SUMMARY:  m_textFormatter.printListPendingArchivesSummaryHeader(); break;
                            case cta::admin::HeaderType::LISTPENDINGRETRIEVES:         m_textFormatter.printListPendingRetrievesHeader(); break;
                            case cta::admin::HeaderType::LISTPENDINGRETRIEVES_SUMMARY: m_textFormatter.printListPendingRetrievesSummaryHeader(); break;
                            case cta::admin::HeaderType::LOGICALLIBRARY_LS:            m_textFormatter.printLogicalLibraryLsHeader(); break;
                            case cta::admin::HeaderType::MOUNTPOLICY_LS:               m_textFormatter.printMountPolicyLsHeader(); break;
                            case cta::admin::HeaderType::REPACK_LS:                    m_textFormatter.printRepackLsHeader(); break;
                            case cta::admin::HeaderType::REQUESTERMOUNTRULE_LS:        m_textFormatter.printRequesterMountRuleLsHeader(); break;
                            case cta::admin::HeaderType::ACTIVITYMOUNTRULE_LS:         m_textFormatter.printActivityMountRuleLsHeader(); break;
                            case cta::admin::HeaderType::SHOWQUEUES:                   m_textFormatter.printShowQueuesHeader(); break;
                            case cta::admin::HeaderType::STORAGECLASS_LS:              m_textFormatter.printStorageClassLsHeader(); break;
                            case cta::admin::HeaderType::TAPE_LS:                      m_textFormatter.printTapeLsHeader(); break;
                            case cta::admin::HeaderType::TAPEFILE_LS:                  m_textFormatter.printTapeFileLsHeader(); break;
                            case cta::admin::HeaderType::TAPEPOOL_LS:                  m_textFormatter.printTapePoolLsHeader(); break;
                            case cta::admin::HeaderType::DISKSYSTEM_LS:                m_textFormatter.printDiskSystemLsHeader(); break;
                            case cta::admin::HeaderType::DISKINSTANCE_LS:              m_textFormatter.printDiskInstanceLsHeader(); break;
                            case cta::admin::HeaderType::DISKINSTANCESPACE_LS:         m_textFormatter.printDiskInstanceSpaceLsHeader(); break;
                            case cta::admin::HeaderType::VIRTUALORGANIZATION_LS:       m_textFormatter.printVirtualOrganizationLsHeader(); break;
                            case cta::admin::HeaderType::VERSION_CMD:                  m_textFormatter.printVersionHeader(); break;
                            case cta::admin::HeaderType::MEDIATYPE_LS:                 m_textFormatter.printMediaTypeLsHeader(); break;
                            case cta::admin::HeaderType::RECYLETAPEFILE_LS:            m_textFormatter.printRecycleTapeFileLsHeader(); break;
                            case cta::admin::HeaderType::PHYSICALLIBRARY_LS:           m_textFormatter.printPhysicalLibraryLsHeader(); break;
                            case cta::admin::HeaderType::NONE:
                            default:
                                break;
                        }
                        break;
                    case cta::xrd::Response::RSP_ERR_PROTOBUF:
                        [[fallthrough]];
                    case cta::xrd::Response::RSP_ERR_USER:
                        [[fallthrough]];
                    case cta::xrd::Response::RSP_ERR_CTA:
                        [[fallthrough]];
                    default:
                        std::cout << "The header type was NOT set to SUCCESS" << std::endl;
                        break;
                        // strErrorMsg = m_response.header().message_txt();
                        // need to log an ERROR here, but figure out later how to do this
                }
            } else if (m_response.has_data()) {
                std::cout << "We received data on the client side" << std::endl;
                switch (m_response.data().data_case()) {
                    case cta::xrd::Data::kTalsItem:
                    {
                        const cta::admin::TapeLsItem& tapeLsItem = m_response.data().tals_item();
                        m_textFormatter.print(tapeLsItem);
                        break;
                    }
                    case cta::xrd::Data::kSclsItem:
                    {
                        const cta::admin::StorageClassLsItem& storageClassLsItem = m_response.data().scls_item();
                        m_textFormatter.print(storageClassLsItem);
                        break;
                    }
                    case cta::xrd::Data::kTplsItem:
                    {
                        const cta::admin::TapePoolLsItem& tapePoolLsItem = m_response.data().tpls_item();
                        m_textFormatter.print(tapePoolLsItem);
                        break;
                    }
                    case cta::xrd::Data::kVolsItem:
                    {
                        const cta::admin::VirtualOrganizationLsItem& virtualOrganizationLsItem = m_response.data().vols_item();
                        m_textFormatter.print(virtualOrganizationLsItem);
                        break;
                    }
                    case cta::xrd::Data::kDilsItem:
                    {
                        const cta::admin::DiskInstanceLsItem& diskInstanceLsItem = m_response.data().dils_item();
                        m_textFormatter.print(diskInstanceLsItem);
                        break;
                    }
                    case cta::xrd::Data::kDrlsItem:
                    {
                        const cta::admin::DriveLsItem& driveLsItem = m_response.data().drls_item();
                        m_textFormatter.print(driveLsItem);
                        break;
                    }
                    case cta::xrd::Data::kAdlsItem:
                    {
                        const cta::admin::AdminLsItem& adminLsItem = m_response.data().adls_item();
                        m_textFormatter.print(adminLsItem);
                        break;
                    }
                    case cta::xrd::Data::kVersionItem:
                    {
                        const cta::admin::VersionItem& versionItem = m_response.data().version_item();
                        m_textFormatter.print(versionItem);
                        break;
                    }
                    case cta::xrd::Data::kArlsItem:
                    {
                        const cta::admin::ArchiveRouteLsItem& archiveRouteLsItem = m_response.data().arls_item();
                        m_textFormatter.print(archiveRouteLsItem);
                        break;
                    }
                    case cta::xrd::Data::kFrlsItem:
                    {
                        const cta::admin::FailedRequestLsItem& failedRequestLsItem = m_response.data().frls_item();
                        m_textFormatter.print(failedRequestLsItem);
                        break;
                    }
                    case cta::xrd::Data::kGmrlsItem:
                    {
                        const cta::admin::GroupMountRuleLsItem& groupMountRuleLsItem = m_response.data().gmrls_item();
                        m_textFormatter.print(groupMountRuleLsItem);
                        break;
                    }
                    case cta::xrd::Data::kLllsItem:
                    {
                        const cta::admin::LogicalLibraryLsItem& logicalLibraryLsItem = m_response.data().llls_item();
                        m_textFormatter.print(logicalLibraryLsItem);
                        break;
                    }
                    case cta::xrd::Data::kPllsItem:
                    {
                        const cta::admin::PhysicalLibraryLsItem& physicalLibraryLsItem = m_response.data().plls_item();
                        m_textFormatter.print(physicalLibraryLsItem);
                        break;
                    }
                    case cta::xrd::Data::kMtlsItem:
                    {
                        const cta::admin::MediaTypeLsItem& mediaTypeLsItem = m_response.data().mtls_item();
                        m_textFormatter.print(mediaTypeLsItem);
                        break;
                    }
                    case cta::xrd::Data::kMplsItem:
                    {
                        const cta::admin::MountPolicyLsItem& mountPolicyLsItem = m_response.data().mpls_item();
                        m_textFormatter.print(mountPolicyLsItem);
                        break;
                    }
                    case cta::xrd::Data::kRelsItem:
                    {
                        const cta::admin::RepackLsItem& RepackLsItem = m_response.data().rels_item();
                        m_textFormatter.print(RepackLsItem);
                        break;
                    }
                    case cta::xrd::Data::kRmrlsItem:
                    {
                        const cta::admin::RequesterMountRuleLsItem& requesterMountRuleLsItem = m_response.data().rmrls_item();
                        m_textFormatter.print(requesterMountRuleLsItem);
                        break;
                    }
                    case cta::xrd::Data::kAmrlsItem:
                    {
                        const cta::admin::ActivityMountRuleLsItem& activityMountRuleLsItem = m_response.data().amrls_item();
                        m_textFormatter.print(activityMountRuleLsItem);
                        break;
                    }
                    case cta::xrd::Data::kSqItem:
                    {
                        const cta::admin::ShowQueuesItem& showQueuesItem = m_response.data().sq_item();
                        m_textFormatter.print(showQueuesItem);
                        break;
                    }
                    case cta::xrd::Data::kTflsItem:
                    {
                        const cta::admin::TapeFileLsItem& tapeFileLsItem = m_response.data().tfls_item();
                        m_textFormatter.print(tapeFileLsItem);
                        break;
                    }
                    case cta::xrd::Data::kDslsItem:
                    {
                        const cta::admin::DiskSystemLsItem& diskSystemLsItem = m_response.data().dsls_item();
                        m_textFormatter.print(diskSystemLsItem);
                        break;
                    }
                    case cta::xrd::Data::kDislsItem:
                    {
                        const cta::admin::DiskInstanceSpaceLsItem& diskInstanceSpaceLsItem = m_response.data().disls_item();
                        m_textFormatter.print(diskInstanceSpaceLsItem);
                        break;
                    }
                    case cta::xrd::Data::kRtflsItem:
                    {
                        const cta::admin::RecycleTapeFileLsItem& recycleTapeFileLsItem = m_response.data().rtfls_item();
                        m_textFormatter.print(recycleTapeFileLsItem);
                        break;
                    }
                    default:
                        // keep compiler happy
                        break;
                }
            }
            // just read the next input from the server until done (ok is false)
            StartRead(&m_response); // consume the next response, maybe do this at the end or you will lose one?
        } // if (ok)
    }

    CtaAdminClientReadReactor(cta::xrd::CtaRpcStream::Stub* client_stub, const cta::xrd::Request* request) {
        // or Otherwise, I can have a generic method
        setenv("GRPC_VERBOSITY", "debug", 1);
        setenv("GRPC_TRACE", "all", 1);

        std::cout << "In CtaAdminClientReadReactor, about to call the async GenericAdminStream" << std::endl;
        client_stub->async()->GenericAdminStream(&m_context, request, this);
        std::cout << "Started the request to the server by calling GenericAdminStream" << std::endl;
        // switch (cmd_pair(request.admincmd().cmd(), request.admincmd().subcmd())) {
        //     case cmd_pair(cta::admin::AdminCmd::CMD_TAPE, cta::admin::AdminCmd::SUBCMD_LS):
        //         stub->async()->TapeLs(context, request, this); 
        //         break;
        //     case cmd_pair(cta::admin::AdminCmd::CMD_STORAGECLASS, cta::admin::AdminCmd::SUBCMD_LS):
        //         stub->async()->StorageClassLs(context, request, this);
        //         break;
        // }
        StartRead(&m_response); // where to store the received response?
        std::cout << "In CtaAdminClientReadReactor, called startRead" << std::endl;
        StartCall(); // activate the RPC!
        std::cout << "In CtaAdminClientReadReactor, called StartCall()" << std::endl;
    }
    // This method I will put in a separate class, that will inherit from CtaAdminClientReactor
    // void ProcessTapeLs() {
    //     stub->async()->TapeLs(context, request, this); // all these steps I will do in the respective call path
    //     StartRead(&m_response); // where to store the received response?
    //     StartCall(); // activate the RPC!
    //     // the onReadDone method should be implemented separately for each of the commands
    // }

private:
    ::grpc::ClientContext m_context;
    cta::xrd::StreamResponse m_response;
    std::mutex mu_;
    std::condition_variable cv_;
    ::grpc::Status status_;
    bool done_ = false;
    cta::admin::TextFormatter m_textFormatter;
};
