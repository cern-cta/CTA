/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "FileUtils.hpp"

#include "common/exception/Exception.hpp"

#include <filesystem>
#include <fstream>
#include <iostream>
#include <unistd.h>

namespace cta::utils {

void restrictFilePermissions(const std::string& path) {
  std::error_code ec;
  std::filesystem::permissions(path,
                               std::filesystem::perms::owner_read | std::filesystem::perms::owner_write
                                 | std::filesystem::perms::group_read | std::filesystem::perms::others_read,
                               std::filesystem::perm_options::replace,
                               ec);
  if (ec) {
    throw exception::Exception("Failed to set file permissions for '" + path + "': " + ec.message());
  }
}

std::string copyFile(const std::string& sourcePath, const std::string& destinationPath, bool restricted) {
  std::error_code ec;
  std::filesystem::copy_file(sourcePath, destinationPath, std::filesystem::copy_options::overwrite_existing, ec);
  if (ec) {
    throw exception::Exception("Failed to copy file from '" + sourcePath + "' to '" + destinationPath
                               + "': " + ec.message());
  }
  if (restricted) {
    restrictFilePermissions(destinationPath);
  }
  return destinationPath;
}

std::string createFileWithContents(const std::string& contents, const std::string& destinationPath, bool restricted) {
  std::ofstream out(destinationPath, std::ios::binary | std::ios::trunc);
  out.exceptions(std::ios::badbit | std::ios::failbit);
  out.write(contents.data(), static_cast<std::streamsize>(contents.size()));
  if (restricted) {
    restrictFilePermissions(destinationPath);
  }
  return destinationPath;
}

}  // namespace cta::utils
