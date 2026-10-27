/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "runtime/config/ConfigLoader.hpp"

#include "common/utils/utils.hpp"
#include "runtime/config/EnvironmentSubstitution.hpp"

#include <stdexcept>

namespace cta::runtime {

ResolvedToml resolveToml(std::string_view contents,
                         const std::string& sourcePath,
                         bool expandEnvironmentVariables,
                         bool errorOnMissingEnvironmentVariable) {
  ResolvedToml document {sourcePath, std::string(contents), {}};
  try {
    document.resolved = expandEnvironmentVariables ?
                          substituteEnvironmentVariables(document.original, errorOnMissingEnvironmentVariable) :
                          document.original;
  } catch (const cta::exception::UserError& error) {
    throw cta::exception::UserError("Failed to resolve config '" + sourcePath + "': " + error.getMessageValue(), false);
  }
  return document;
}

ResolvedToml
readTomlFile(const std::string& filePath, bool expandEnvironmentVariables, bool errorOnMissingEnvironmentVariable) {
  std::string contents;
  try {
    contents = cta::utils::readFileAsString(filePath);
  } catch (const std::runtime_error& error) {
    throw cta::exception::UserError("Failed to read config file '" + filePath + "': " + error.what(), false);
  }
  return resolveToml(contents, filePath, expandEnvironmentVariables, errorOnMissingEnvironmentVariable);
}

}  // namespace cta::runtime
