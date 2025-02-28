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

// This is a virtual (maybe not all of its methods) class, each command implementation will inherit from this
class CtaAdminClientReadReactor : public grpc::ClientReadReactor<cta::xrd::StreamResponse> {
public:
    void OnDone(const Status &s) override {
        std::unique_lock<std::mutex> l(mu_);
        status_ = s;
        done_ = true;
        cv_.notify_one();
    }
    Status Await() {
        std::unique_lock<std::mutex> l(mu_);
        cv_.wait(l, [this] { return done_; });
        return std::move(status_);
    }
    virtual void OnReadDone(bool ok) override {
        if (ok) {
            // just read the next input from the server until done (ok is false)
            StartRead(&m_response);
        }
    }

private:
    ClientContext context_;
    cta::xrd::StreamResponse m_response;
    std::mutex mtx_;
    std::condition_variable cv_;
    Status status_;
    bool done_ = false;
}