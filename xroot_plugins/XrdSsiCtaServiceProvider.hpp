/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include <memory>
#include <string>
#include <XrdSsi/XrdSsiProvider.hh>
#include <XrdSsiPbLog.hpp>

#include "catalogue/Catalogue.hpp"
#include "common/utils/utils.hpp"
#include "scheduler/Scheduler.hpp"
#ifdef CTA_PGSCHED
#include "scheduler/rdbms/RelationalDBInit.hpp"
#else
#include "scheduler/OStoreDB/OStoreDBInit.hpp"
#endif
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
