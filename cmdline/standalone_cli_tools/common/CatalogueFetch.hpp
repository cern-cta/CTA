/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/log/StdoutLogger.hpp"
#include "version.h"
#include "xrootd-ssi-protobuf-interface/eos_cta/include/CtaFrontendApi.hpp"

#include <XrdSsiPbIStreamBuffer.hpp>
#include <XrdSsiPbLog.hpp>
#include <atomic>
#include <list>
#include <string>
#include <utility>

#include "cta_frontend.pb.h"  //!< Auto-generated message types from .proto file

namespace cta::cliTool {

class CatalogueFetch {
public:
  /**
  * Fetches the instance and fid from the CTA catalogue
  *
  * @param archiveFileId The arhive file id.
  * @param serviceProviderPtr Service provider for communication with the catalogue.
  * @return a pair with the instance and the fid.
  */
  static std::tuple<std::string, std::string>
  getInstanceAndFid(const std::string& archiveFileId,
                    std::unique_ptr<XrdSsiPbServiceType>& serviceProviderPtr,
                    cta::log::StdoutLogger& log);

  /**
  * Fetches the vids form the CTA catalogue
  *
  * @param serviceProviderPtr Service provider for communication with the catalogue.
  * @return True if vid exists, false if it does not exist
  */
  static bool vidExists(const std::string& vid, std::unique_ptr<XrdSsiPbServiceType>& serviceProviderPtr);

private:
  static void handleResponse(const cta::xrd::Request& request,
                             std::unique_ptr<XrdSsiPbServiceType>& serviceProviderPtr);
};

}  // namespace cta::cliTool
