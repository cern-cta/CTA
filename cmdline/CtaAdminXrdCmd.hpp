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

#include "version.h"
#include "CtaAdminParsedCmd.hpp"

namespace cta::admin {

class CtaAdminXrdCmd
{
public:
   //! Send the protocol buffer across the XRootD SSI transport
   void send(const CtaAdminParsedCmd& parsedCmd) const;

private:
   const std::string StreamBufferSize      = "1024";                  //!< Buffer size for Data/Stream Responses
   const std::string DefaultRequestTimeout = "10";                    //!< Default Request Timeout. Can be overridden by

   static constexpr const char* const LOG_SUFFIX  = "CtaAdminXrdCmd";    //!< Identifier for log messages
};

} // namespace cta::admin
