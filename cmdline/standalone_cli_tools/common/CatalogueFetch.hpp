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

#include <atomic>
#include <list>
#include <string>
#include <utility>

#include <XrdSsiPbLog.hpp>
#include <XrdSsiPbIStreamBuffer.hpp>

#include "common/log/StdoutLogger.hpp"
#include "xrootd-ssi-protobuf-interface/eos_cta/include/CtaFrontendApi.hpp"
#include "cta_frontend.pb.h"  //!< Auto-generated message types from .proto file

#include "version.h"

namespace cta {
namespace cliTool {

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

}  // namespace cliTool
}  // namespace cta