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

#include <memory>
#include <string>
#include <XrdSsi/XrdSsiProvider.hh>
#include <XrdSsiPbLog.hpp>

#include "catalogue/Catalogue.hpp"
#include "common/Configuration.hpp"
#include "common/utils/utils.hpp"
#include "scheduler/Scheduler.hpp"
#ifdef CTA_PGSCHED
#include "scheduler/PostgresSchedDB/PostgresSchedDBInit.hpp"
#else
#include "scheduler/OStoreDB/OStoreDBInit.hpp"
#endif
#include "frontend/common/Namespace.hpp"
#include "frontend/common/FrontendService.hpp"

/*!
 * Global pointer to the Service Provider object.
 */
extern XrdSsiProvider *XrdSsiProviderServer;

/*!
 * Instantiates a Service to process client requests.
 */
class XrdSsiCtaServiceProvider : public XrdSsiProvider {
public:
  XrdSsiCtaServiceProvider() {
    XrdSsiPb::Log::Msg(XrdSsiPb::Log::DEBUG, LOG_SUFFIX, "XrdSsiCtaServiceProvider() constructor");
  }

  virtual ~XrdSsiCtaServiceProvider() {
    XrdSsiPb::Log::Msg(XrdSsiPb::Log::DEBUG, LOG_SUFFIX, "~XrdSsiCtaServiceProvider() destructor");
  }

  /*!
   * Initialize the object.
   *
   * This is always called before any other method.
   */
  // This method inherits from an external class to this project, so we cannot modify the interface
  bool Init(XrdSsiLogger *logP, XrdSsiCluster *clsP, const std::string cfgFn,  // cppcheck-suppress passedByValue
    const std::string parms, int argc, char **argv) override;  // cppcheck-suppress passedByValue

  /*!
   * Instantiate a Service object.
   *
   * Called exactly once after initialisation to obtain an instance of an XrdSsiService object
   */
  XrdSsiService *GetService(XrdSsiErrInfo &eInfo, const std::string &contact, int oHold = 256) override;

  /*!
   * Query whether a resource exists on a server.
   *
   * Determines resource availability. Can be called any time the client asks for the resource status.
   *
   * @param[in]    rName      The resource name
   * @param[in]    contact    Used by client-initiated queries for a resource at a particular endpoint.
   *                          It is set to nullptr for server-initiated queries.
   *
   * @retval    XrdSsiProvider::notPresent    The resource does not exist
   * @retval    XrdSsiProvider::isPresent     The resource exists
   * @retval    XrdSsiProvider::isPending     The resource exists but is not immediately available. (Useful
   *                                          only in clustered environments where the resource may be
   *                                          immediately available on some other node.)
   */
  XrdSsiProvider::rStat QueryResource(const char *rName, const char *contact = nullptr) override;

  /*!
   * Get a reference to the FrontendService object
   */
  cta::frontend::FrontendService& getFrontendService() const { return *m_frontendService; }

 private:
  // Member variables
  std::unique_ptr<cta::frontend::FrontendService> m_frontendService;             //!< protocol-neutral CTA Frontend Service object
  static constexpr const char* const LOG_SUFFIX = "XrdSsiCtaServiceProvider";    //!< Identifier for log messages
};
