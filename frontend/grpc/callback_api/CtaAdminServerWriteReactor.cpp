/*
 * SPDX-FileCopyrightText: 2025 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "CtaAdminServerWriteReactor.hpp"

#include "common/exception/Exception.hpp"
#include "common/exception/UserError.hpp"
#include "frontend/common/PbException.hpp"
#include "frontend/grpc/RequestMessage.hpp"

#include <catalogue/Catalogue.hpp>
#include <grpcpp/grpcpp.h>
#include <scheduler/Scheduler.hpp>

#include "cta_frontend.grpc.pb.h"
#include "cta_frontend.pb.h"

namespace cta::frontend::grpc {

/* This is the base class from which all commands will inherit,
 * we introduce it to avoid having to write boilerplate code (OnDone, OnWriteDone) for each command */
void CtaAdminServerWriteReactor::OnWriteDone(bool ok) {
  if (!ok) {
    Finish(::grpc::Status(::grpc::StatusCode::UNKNOWN, "Unexpected Failure in OnWriteDone"));
  }
  NextWrite();
}

void CtaAdminServerWriteReactor::OnDone() {
  delete this;
}

void CtaAdminServerWriteReactor::NextWrite() {
  m_response.Clear();

  if (!m_isHeaderSent) {
    cta::xrd::Response* header = new cta::xrd::Response();
    header->set_type(cta::xrd::Response::RSP_SUCCESS);
    header->set_show_header(m_headerType);
    m_response.set_allocated_header(header);

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
      cta::xrd::Data* data = new cta::xrd::Data();
      *data = m_stream->next();

      m_response.set_allocated_data(data);
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

CtaAdminServerWriteReactor::CtaAdminServerWriteReactor(cta::Scheduler& scheduler,
                                                       const std::string& instanceName,
                                                       std::unique_ptr<CtaAdminResponseStream> stream,
                                                       cta::admin::HeaderType headerType)
    : m_schedulerBackendName(scheduler.getSchedulerBackendName()),
      m_instanceName(instanceName),
      m_stream(std::move(stream)),
      m_headerType(headerType) {
  if (m_stream != nullptr) {
    NextWrite();
  }
}
}  // namespace cta::frontend::grpc
