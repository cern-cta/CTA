/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
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
