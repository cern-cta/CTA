/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "BackendFactory.hpp"

#include "BackendRados.hpp"
#include "BackendVFS.hpp"
#include "common/exception/Exception.hpp"
#include "common/utils/Regex.hpp"
#include "common/utils/utils.hpp"

#include <string>
#include <vector>

auto cta::objectstore::BackendFactory::createBackend(const std::string& URL, log::Logger& logger)
  -> std::unique_ptr<Backend> {
  utils::Regex fileRe("^file://(.*)$"), radosRe("^rados://([^@]+)@([^:]+)(|:(.+))$");
  std::vector<std::string> regexResult;
  // Is it a file:// URL?
  regexResult = fileRe.exec(URL);
  if (regexResult.size()) {
    return std::unique_ptr<Backend>(new BackendVFS(regexResult[1]));
  }
  // Is it a rados:// URL?
  regexResult = radosRe.exec(URL);
  if (regexResult.size()) {
    if (regexResult.size() != 5 && regexResult.size() != 4) {
      throw cta::exception::Exception("In BackendFactory::createBackend(): unexpected number of matches in regex");
    }
    if (regexResult.size() == 5) {
      return std::unique_ptr<Backend>(new BackendRados(logger, regexResult[1], regexResult[2], regexResult[4]));
    } else {
      return std::unique_ptr<Backend>(new BackendRados(logger, regexResult[1], regexResult[2]));
    }
  }
  // Fall back to a file URL if all failed
  return std::unique_ptr<Backend>(new BackendVFS(URL));
}
