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

#include <XrdSsiPbAlert.hpp>
#include <XrdSsiPbConfig.hpp>
#include <XrdSsiPbService.hpp>

#include "XrdSsiCtaServiceProvider.hpp"
#include "cta_frontend.pb.h"

/*
 * Global pointer to the Service Provider object.
 *
 * This must be defined at library load time (i.e. it is a file-level global static symbol). When the
 * shared library is loaded, XRootD initialization fails if the appropriate symbol cannot be found (or
 * it is a null pointer).
 */
XrdSsiProvider *XrdSsiProviderServer = new XrdSsiCtaServiceProvider;

// This method inherits from an external class to this project, so we cannot modify the interface
bool XrdSsiCtaServiceProvider::Init(XrdSsiLogger *logP, XrdSsiCluster *clsP, const std::string cfgFn,  // cppcheck-suppress passedByValue
  const std::string parms, int argc, char **argv) {  // cppcheck-suppress passedByValue
  try {
    XrdSsiPb::Log::Msg(XrdSsiPb::Log::INFO, LOG_SUFFIX, "Called Init(", cfgFn, ',', parms, ')');

    // Set XRootD SSI Protobuf logging level from config file
    XrdSsiPb::Config config(cfgFn);
    auto loglevel = config.getOptionList("cta.log.ssi");
    if(loglevel.empty()) {
      XrdSsiPb::Log::SetLogLevel("info");
    } else {
      XrdSsiPb::Log::SetLogLevel(loglevel);
    }

    // Initialise the Frontend Service object from the config file
    m_frontendService = std::make_unique<cta::frontend::FrontendService>(cfgFn);
    return true;
  } catch(XrdSsiPb::XrdSsiException &ex) {
    XrdSsiPb::Log::Msg(XrdSsiPb::Log::ERROR, LOG_SUFFIX, "XrdSsiCtaServiceProvider::Init(): XrdSsiPb::XrdSsiException ", ex.what());
  } catch(cta::exception::Exception &ex) {
    XrdSsiPb::Log::Msg(XrdSsiPb::Log::ERROR, LOG_SUFFIX, "XrdSsiCtaServiceProvider::Init(): cta::exception::Exception ", ex.what());
  } catch(std::exception &ex) {
    XrdSsiPb::Log::Msg(XrdSsiPb::Log::ERROR, LOG_SUFFIX, "XrdSsiCtaServiceProvider::Init(): std::exception ", ex.what());
  } catch(...) {
    XrdSsiPb::Log::Msg(XrdSsiPb::Log::ERROR, LOG_SUFFIX, "XrdSsiCtaServiceProvider::Init(): unknown exception");
  }

  // XRootD has a bug where if we return false here, it triggers a SIGABRT. Until this is fixed, exit(1) as a workaround.
  // Fixed in https://github.com/xrootd/xrootd/issues/751
  // exit(1);

  return false;
}

XrdSsiService* XrdSsiCtaServiceProvider::GetService(XrdSsiErrInfo &eInfo, const std::string &contact, int oHold)
{
  XrdSsiPb::Log::Msg(XrdSsiPb::Log::INFO, LOG_SUFFIX, "Called GetService(", contact, ',', oHold, ')');

  XrdSsiService *ptr = new XrdSsiPb::Service<cta::xrd::Request, cta::xrd::Response, cta::xrd::Alert>;

  return ptr;
}

XrdSsiProvider::rStat XrdSsiCtaServiceProvider::QueryResource(const char *rName, const char *contact)
{
  // We only have one resource

  XrdSsiProvider::rStat resourcePresence = (strcmp(rName, "/ctafrontend") == 0) ?
                                          XrdSsiProvider::isPresent : XrdSsiProvider::notPresent;

  XrdSsiPb::Log::Msg(XrdSsiPb::Log::INFO, LOG_SUFFIX, "QueryResource(", rName, "): ",
                   ((resourcePresence == XrdSsiProvider::isPresent) ? "isPresent" : "notPresent"));

  return resourcePresence;
}
