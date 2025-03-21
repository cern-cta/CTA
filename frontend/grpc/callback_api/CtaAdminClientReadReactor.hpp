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
            // StartRead(&m_response); // no, don't do this, we are just getting in an endless loop
        } else {
            // if this is the header, print the formatted header I guess?
            if (m_response.has_header()) {
                std::cout << "We received the header on the client side " << std::endl;
                switch (m_response.header().type()) {
                    case cta::xrd::Response::RSP_SUCCESS:
                        std::cout << "The header type was set to SUCCESS" << std::endl;
                        switch (m_response.header().show_header()) {
                            case cta::admin::HeaderType::TAPE_LS:
                                m_textFormatter.printTapeLsHeader();
                                break;
                            case cta::admin::HeaderType::STORAGECLASS_LS:
                                m_textFormatter.printStorageClassLsHeader();
                                break;
                            default:
                                // keep compiler happy
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
