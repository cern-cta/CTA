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

#include "common/dataStructures/ArchiveRouteType.hpp"
#include "common/exception/Exception.hpp"
#include "common/exception/UserError.hpp"
#include "common/utils/utils.hpp"

#include <sstream>

namespace cta::common::dataStructures {

std::string toString(ArchiveRouteType type) {
  switch (type) {
  case ArchiveRouteType::DEFAULT:
    return "DEFAULT";
  case ArchiveRouteType::REPACK:
    return "REPACK";
  default:
    throw cta::exception::Exception(std::string("The type given (") + std::to_string(type) + ") does not exist.");
  }
}

ArchiveRouteType strToArchiveRouteType(const std::string &archiveRouteTypeStr) {
  std::string typeUpperCase = archiveRouteTypeStr;
  cta::utils::toUpper(typeUpperCase);
  if (typeUpperCase == "DEFAULT") {
    return ArchiveRouteType::DEFAULT;
  } else if (typeUpperCase == "REPACK") {
    return ArchiveRouteType::REPACK;
  } else {
    throw cta::exception::UserError(std::string("The type given (") + typeUpperCase + ") does not exist. Possible values are DEFAULT and REPACK.");
  }
}

}