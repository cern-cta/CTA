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

// Include Files
#include "common/exception/Exception.hpp"

#include <source_location>

namespace cta::exception {

/**
 * Not implemented exception
 */
class NotImplementedException : public cta::exception::Exception {
public:
  NotImplementedException([[maybe_unused]] const std::string& what = "",
                          const std::source_location loc = std::source_location::current())
      : cta::exception::Exception(makeMessage(what, loc)) {}

private:
  static std::string makeMessage(const std::string& what, const std::source_location& loc) {
    std::ostringstream oss;
    oss << "Unimplemented: " << loc.file_name() << '(' << loc.line() << ':' << loc.column() << ") `"
        << loc.function_name() << "`: " << what;
    return oss.str();
  }
};

}  // namespace cta::exception
