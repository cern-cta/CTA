/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "runtime/config/ConfigLoader.hpp"

#include "runtime/config/EnvironmentSubstitution.hpp"

#include <fstream>

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
  std::ifstream file(filePath, std::ios::binary);
  if (!file.is_open()) {
    throw cta::exception::UserError("Failed to open config file '" + filePath + "'.", false);
  }

  // Preserve exact bytes for snapshots and reject partial reads.
  std::string contents;
  char buffer[8192];
  while (file.read(buffer, sizeof(buffer)) || file.gcount() > 0) {
    contents.append(buffer, static_cast<std::size_t>(file.gcount()));
  }
  if (file.bad() || !file.eof()) {
    throw cta::exception::UserError("Failed to read config file '" + filePath + "'.", false);
  }
  return resolveToml(contents, filePath, expandEnvironmentVariables, errorOnMissingEnvironmentVariable);
}

}  // namespace cta::runtime
