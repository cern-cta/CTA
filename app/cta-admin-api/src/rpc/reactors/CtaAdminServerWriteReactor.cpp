/*
 * SPDX-FileCopyrightText: 2025 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "rpc/reactors/CtaAdminServerWriteReactor.hpp"

#include "common/exception/Exception.hpp"
#include "common/exception/UserError.hpp"
#include "grpc-server/PbException.hpp"

#include <grpcpp/grpcpp.h>

#include "cta_frontend.grpc.pb.h"
#include "cta_frontend.pb.h"

namespace cta::admin_api::rpc {

void CtaAdminServerWriteReactor::OnWriteDone(bool ok) {
  if (!ok) {
    Finish(::grpc::Status(::grpc::StatusCode::CANCELLED, "Write failed: client gone or call cancelled"));
    return;
  }
  NextWrite();
}

void CtaAdminServerWriteReactor::OnDone() {
  delete this;
}

void CtaAdminServerWriteReactor::NextWrite() {
  m_response.Clear();

  if (!m_isHeaderSent) {
    cta::xrd::Response* header = m_response.mutable_header();
    header->set_type(cta::xrd::Response::RSP_SUCCESS);
    header->set_show_header(m_stream->headerType());

    m_isHeaderSent = true;
    StartWrite(&m_response);
    return;
  }

  // isDone()/next() run a DB query for every queue/row: unlike the code above and below, this can
  // fail well after the header has already been streamed to the client. Without this try/catch, an
  // exception here would escape uncaught from a gRPC callback and crash the whole frontend process,
  // taking down every other in-flight request with it. Mapped onto the same exception types and
  // gRPC status codes as the equivalent catch chain in FrontendGrpcService.cpp's Admin(), so a
  // client sees the same kind of error whether the command used the unary or the streaming RPC.
  try {
    if (!m_stream->isDone()) {
      *m_response.mutable_data() = m_stream->next();
      StartWrite(&m_response);
    } else {
      Finish(::grpc::Status::OK);
    }
  } catch (cta::exception::PbException& ex) {
    Finish(::grpc::Status(::grpc::StatusCode::FAILED_PRECONDITION, ex.getMessageValue()));
  } catch (cta::exception::UserError& ex) {
    Finish(::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, ex.getMessageValue()));
  } catch (cta::exception::Exception& ex) {
    Finish(::grpc::Status(::grpc::StatusCode::FAILED_PRECONDITION, ex.getMessageValue()));
  } catch (std::exception& ex) {
    Finish(::grpc::Status(::grpc::StatusCode::UNKNOWN, ex.what()));
  }
}

CtaAdminServerWriteReactor::CtaAdminServerWriteReactor(std::unique_ptr<commands::stream::CtaAdminResponseStream> stream)
    : m_stream(std::move(stream)) {
  NextWrite();
}

}  // namespace cta::admin_api::rpc
