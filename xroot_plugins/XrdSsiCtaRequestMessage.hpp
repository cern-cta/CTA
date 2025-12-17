/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "XrdSsiCtaServiceProvider.hpp"
#include "common/utils/utils.hpp"
#include "frontend/common/Version.hpp"

#include <XrdSsi/XrdSsiEntity.hh>
#include <XrdSsi/XrdSsiStream.hh>

#include "cta_frontend.pb.h"

namespace cta::xrd {

/*!
 * CTA Frontend Request Message class
 */
class RequestMessage {
public:
  RequestMessage(const XrdSsiEntity& client, const XrdSsiCtaServiceProvider* service)
      : m_cliIdentity(client.name, cta::utils::getShortHostname(), client.host, client.prot),
        m_service(*service) {}

  /*!
   * Process a Notification request or an Admin command request
   *
   * @param[in]     request
   * @param[out]    response        Response protocol buffer
   * @param[out]    stream          Reference to Response stream pointer
   */
  void process(const cta::xrd::Request& request, cta::xrd::Response& response, XrdSsiStream*& stream);

private:
  // Member variables
  cta::common::dataStructures::SecurityIdentity m_cliIdentity;  //!< Client identity: username/host
  const XrdSsiCtaServiceProvider& m_service;                    //!< Const reference to the XRootD SSI Service
};

}  // namespace cta::xrd
