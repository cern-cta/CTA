/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "RequestMessage.hpp"

#include "cmdline/CtaAdminCmdParser.hpp"

cta::frontend::grpc::request::RequestMessage::RequestMessage(const cta::xrd::Request& request) {
  AdminCmdOptions::importOptions(request.admincmd());
}
