/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
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
      throw cta::exception::Exception(std::string("The type given (") + std::to_string(type)
                                      + ") does not exist. Possible values are DEFAULT and REPACK.");
  }
}

ArchiveRouteType strToArchiveRouteType(const std::string& archiveRouteTypeStr) {
  std::string typeUpperCase = archiveRouteTypeStr;
  cta::utils::toUpper(typeUpperCase);
  if (typeUpperCase == "DEFAULT") {
    return ArchiveRouteType::DEFAULT;
  } else if (typeUpperCase == "REPACK") {
    return ArchiveRouteType::REPACK;
  } else {
    throw cta::exception::UserError(std::string("The type given (") + typeUpperCase
                                    + ") does not exist. Possible values are DEFAULT and REPACK.");
  }
}

}  // namespace cta::common::dataStructures
