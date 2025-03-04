/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2023 CERN
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

 #include "cta_frontend.pb.h"
 #include "cta_frontend.grpc.pb.h"
 #include "common/log/Logger.hpp"
 #include "common/log/LogContext.hpp"
 #include "common/exception/Exception.hpp"
 
 #include <grpcpp/grpcpp.h>
 
 #include <string>
 #include <memory>
 #include <mutex>
 #include <thread>

namespace cta::frontend::grpc {

// callbackService class is the one that must implement the rpc methods
// a streaming rpc method must have return type ServerWriteReactor
class CtaRpcStreamImpl : public CtaRpcStream::CallbackService {
  public:
    cta::log::LogContext getLogContext() const { return m_lc; }
    // CtaRpcStreamImpl() = delete;
    CtaRpcStreamImpl(cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler) : m_catalogue(catalogue), m_scheduler(scheduler) {}
    /* CtaAdminServerWriteReactor is what the type of GenericAdminStream could be */
    grpc::ServerWriteReactor<cta::xrd::Response>* GenericAdminStream(CallbackServerContext* context, const cta::xrd::Request* request);
    grpc::ServerWriteReactor<cta::xrd::Response>* TapeLs(CallbackServerContext* context, const cta::xrd::Request* request);

  private:
    cta::log::LogContext m_lc;
    cta::catalogue::Catalogue &m_catalogue;    //!< Reference to CTA Catalogue
    cta::Scheduler            &m_scheduler;    //!< Reference to CTA Scheduler
    // I do not think a reactor could be a member of this class because it must be reinitialized upon each call
    // CtaAdminServerWriteReactor *m_reactor;      // this will have to be initialized to TapeLs or StorageClassLs or whatever...
};

// serverWriteReactor<Feature> is the RETURN TYPE of the function which is server-side streaming!!!
// Is serverWriteReactor a class? yes it is
// but clientReadReactor is a class
class CtaAdminServerWriteReactor : public grpc::ServerWriteReactor<cta::xrd::StreamResponse> {
  public:
    void OnWriteDone(bool ok) override {
      if (!ok) {
        Finish(Status(grpc::StatusCode::UNKNOWN, "Unexpected Failure in OnWriteDone"));
      }
      NextWrite();
    }
  private:
    // bool isHeaderSent = false;
    xrd::cta::Response stream_item_;
    std::vector<> // will hold the database query, each protobuf sent as the response is of the type cta::xrd::Response (which is either Header or Data)
    // for tapeLS for example, we hold the results in std::list<common::dataStructures::Tape> m_tapeList; - XrdCtaTapeLs.hpp implementation
}

// request object will be filled in by the Parser of the command on the client-side.
// Currently I am calling this class CtaAdminCmdStreamingClient
grpc::ServerWriteReactor<StreamResponse>* GenericAdminStream(
    CallbackServerContext* context,
    const cta::xrd::Request* request) override {
  class Lister : public grpc::ServerWriteReactor<Feature> {
   public:
    Lister(const routeguide::Rectangle* rectangle,
           const std::vector<Feature>* feature_list)
        : left_((std::min)(rectangle->lo().longitude(),
                           rectangle->hi().longitude())),
          right_((std::max)(rectangle->lo().longitude(),
                            rectangle->hi().longitude())),
          top_((std::max)(rectangle->lo().latitude(),
                          rectangle->hi().latitude())),
          bottom_((std::min)(rectangle->lo().latitude(),
                             rectangle->hi().latitude())),
          feature_list_(feature_list),
          next_feature_(feature_list_->begin()) {
      NextWrite();
    }

    void OnWriteDone(bool ok) override {
      if (!ok) {
        Finish(Status(grpc::StatusCode::UNKNOWN, "Unexpected Failure"));
      }
      NextWrite();
    }

    void OnDone() override {
      LOG(INFO) << "RPC Completed";
      delete this;
    }

    void OnCancel() override { LOG(ERROR) << "RPC Cancelled"; }

   private:
    void NextWrite() {
      while (next_feature_ != feature_list_->end()) {
        const Feature& f = *next_feature_;
        next_feature_++;
        if (f.location().longitude() >= left_ &&
            f.location().longitude() <= right_ &&
            f.location().latitude() >= bottom_ &&
            f.location().latitude() <= top_) {
          StartWrite(&f);
          return;
        }
      }
      // Didn't write anything, all is done.
      Finish(Status::OK);
    }
    const long left_;
    const long right_;
    const long top_;
    const long bottom_;
    const std::vector<Feature>* feature_list_;
    std::vector<Feature>::const_iterator next_feature_;
  };
  return new Lister(rectangle, &feature_list_);
}
} // namespace cta::frontend::grpc