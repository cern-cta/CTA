/*
 * SPDX-FileCopyrightText: 2025 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "commands/stream/CtaAdminResponseStream.hpp"

#include <grpcpp/grpcpp.h>
#include <memory>

#include "cta_frontend.grpc.pb.h"
#include "cta_frontend.pb.h"

namespace cta::admin_api::rpc {

/**
 * @brief Writes a stream of items to the client: first the header of the command, then the items of the stream, one
 * message each, and finally the status of the call
 *
 * The reactor deletes itself once the call is done.
 */
class CtaAdminServerWriteReactor final : public ::grpc::ServerWriteReactor<cta::xrd::StreamResponse> {
public:
  /**
   * @param stream  The items to send, taken from the catalogue or the scheduler as the call progresses, along with
   *                 the header to send before them
   */
  explicit CtaAdminServerWriteReactor(std::unique_ptr<commands::stream::CtaAdminResponseStream> stream);

  void OnWriteDone(bool ok) override;
  void OnDone() override;

private:
  /** @brief Sends the next message: the header, then an item of the stream, then finishes the call */
  void NextWrite();

  bool m_isHeaderSent = false;
  cta::xrd::StreamResponse m_response;
  std::unique_ptr<commands::stream::CtaAdminResponseStream> m_stream;
};

}  // namespace cta::admin_api::rpc
