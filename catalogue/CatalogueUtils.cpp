/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2022 CERN
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

#include <memory>
#include <optional>
#include <string>

#include "catalogue/CatalogueExceptions.hpp"
#include "catalogue/CatalogueUtils.hpp"
#include "common/log/LogContext.hpp"

namespace cta {
namespace catalogue {

void CatalogueUtils::checkCommentOrReasonMaxLength(const std::optional<std::string>& str, log::Logger &log) {
  const size_t MAX_CHAR_COMMENT = 1000;
  if (!str.has_value()) return;
  if (str.value().length() > MAX_CHAR_COMMENT) {
    log::LogContext lc(log);
    log::ScopedParamContainer spc(lc);
    spc.add("Large_Message: ", str.value());
    lc.log(log::ERR, "The reason or comment has more characters than the maximun allowed.");
    throw exception::CommentOrReasonWithMoreSizeThanMaximunAllowed(
      "The comment or reason string value has more than 1000 characters");
  }
}

}  // namespace catalogue
}  // namespace cta
