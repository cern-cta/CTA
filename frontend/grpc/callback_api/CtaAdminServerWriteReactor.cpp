/*
 * SPDX-FileCopyrightText: 2025 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#include <catalogue/Catalogue.hpp>
#include <scheduler/Scheduler.hpp>

#include "cta_frontend.pb.h"
#include "cta_frontend.grpc.pb.h"
#include <grpcpp/grpcpp.h>
#include "../RequestMessage.hpp"
#include "CtaAdminServerWriteReactor.hpp"

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

  if (!m_stream->isDone()) {
    cta::xrd::Data* data = new cta::xrd::Data();
    *data = m_stream->next();

    m_response.set_allocated_data(data);
    StartWrite(&m_response);
  } else {
    Finish(::grpc::Status::OK);
  }
}

CtaAdminServerWriteReactor::CtaAdminServerWriteReactor(cta::catalogue::Catalogue& catalogue,
                                                       cta::Scheduler& scheduler,
                                                       const std::string& instanceName,
                                                       std::unique_ptr<cta::cmdline::CtaAdminResponseStream> stream,
                                                       cta::admin::HeaderType headerType)
    : m_isHeaderSent(false),
      m_schedulerBackendName(scheduler.getSchedulerBackendName()),
      m_instanceName(instanceName),
      m_stream(std::move(stream)),
      m_headerType(headerType) {
  if (m_stream != nullptr) {
    NextWrite();
  }
}
}  // namespace cta::frontend::grpc
