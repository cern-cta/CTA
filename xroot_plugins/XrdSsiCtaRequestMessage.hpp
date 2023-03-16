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

#include <XrdSsi/XrdSsiEntity.hh>

#include "common/utils/utils.hpp"
#include "frontend/common/Version.hpp"
#include "XrdSsiCtaServiceProvider.hpp"
#include "cta_frontend.pb.h"

namespace cta { namespace xrd {

/*!
 * CTA Frontend Request Message class
 */
class RequestMessage
{
public:
  RequestMessage(const XrdSsiEntity &client, const XrdSsiCtaServiceProvider *service) :
    m_cliIdentity(client.name, cta::utils::getShortHostname(), client.host, client.prot),
    m_service(*service) { }

  /*!
   * Process a Notification request or an Admin command request
   *
   * @param[in]     request
   * @param[out]    response        Response protocol buffer
   * @param[out]    stream          Reference to Response stream pointer
   */
  void process(const cta::xrd::Request &request, cta::xrd::Response &response, XrdSsiStream* &stream);

private:

  // Member variables
  cta::common::dataStructures::SecurityIdentity         m_cliIdentity;                //!< Client identity: username/host
  const XrdSsiCtaServiceProvider                       &m_service;                    //!< Const reference to the XRootD SSI Service
};

}} // namespace cta::xrd
