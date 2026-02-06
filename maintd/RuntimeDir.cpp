/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "RuntimeDir.hpp"

#include "exception/Exception.hpp"

#include <filesystem>

namespace cta::runtime {

RuntimeDir::RuntimeDir() : m_dirPath("/run/cta/" + std::to_string(getpid())), m_log(log) {
  // Delete the directory in case it already exists, NOOP if it doesn't
  deleteRuntimeDir();

  std::error_code ec;
  fs::create_directories(m_dirPath, ec);
  if (ec) {
    throw exception::Exception("Failed to create directory: " + ec.message());
  }
  restrictDirPermissions(m_dirPath);
}

RuntimeDir::~RuntimeDir() {
  try {
    deleteRuntimeDir();
  } catch (...) {
    // Directory failed to clean up, but we can't throw and no access to logger
    // Should only be triggered at shutdown and is not critical.
  }
}

std::string RuntimeDir::runtimeDirPath() const {
  return m_dirPath;
}

void RuntimeDir::restrictDirPermissions(const std::string& path) const {
  std::error_code ec;
  fs::permissions(path,
                  fs::perms::owner_all | fs::perms::group_read | fs::perms::group_exec | fs::perms::others_read
                    | fs::perms::others_exec,
                  fs::perm_options::replace,
                  ec);
  if (ec) {
    throw exception::Exception("Failed to set dir permissions: " + ec.message());
  }
}

void RuntimeDir::restrictFilePermissions(const std::string& path) const {
  std::error_code ec;
  fs::permissions(path,
                  fs::perms::owner_read | fs::perms::owner_write | fs::perms::group_read | fs::perms::others_read,
                  fs::perm_options::replace,
                  ec);
  if (ec) {
    throw exception::Exception("Failed to set file permissions: " + ec.message());
  }
}

std::string RuntimeDir::copyFile(const std::string& sourcePath, const std::string& newFileName) {
  std::string destinationPath = m_dirPath + "/" + newFileName;
  std::error_code ec;
  fs::copy_file(sourcePath, m_dirPath + "/" + newFileName, fs::copy_options::overwrite_existing, ec);
  if (ec) {
    throw exception::Exception("Failed to copy file from " + sourcePath + ": " + ec.message());
  }
  restrictFilePermissions(destinationPath);
  return destinationPath;
}

std::string RuntimeDir::createFile(const std::string& contents, const std::string& newFileName) {
  std::string destinationPath = m_dirPath + "/" + newFileName;
  std::ofstream out(destinationPath, std::ios::binary | std::ios::trunc);
  out.exceptions(std::ios::badbit | std::ios::failbit);
  out.write(contents.data(), static_cast<std::streamsize>(contents.size()));
  restrictPermissions(destinationPath);
  return destinationPath;
}

void RuntimeDir::deleteRuntimeDir() const {
  // Yes this string construction is duplicated on purpose.
  // Removing directories recursively is a dangerous business.
  // Better double check we are not deleting something unintentionally.
  const std::string expected = "/run/cta/" + std::to_string(getpid());
  if (m_dirPath != expected) {
    return;
  }

  // Ignore if it doesn't exist
  if (!fs::exists(m_dirPath)) {
    return;
  }
  // Don't delete if it is a symlink
  // That could lead to some sketchy scenarios...
  const auto ss = fs::symlink_status(m_dirPath);
  if (fs::is_symlink(ss)) {
    throw exception::Exception("Failed to delete " + m_dirPath + ". Path is a symlink.");
  }
  // We want to delete a directory, so if the thing is a file, something is wrong
  if (!fs::is_directory(ss)) {
    throw exception::Exception("Failed to delete " + m_dirPath + ". Path is not a directory.");
  }

  // Prevent weird paths like /run/cta/../../someotherdirectory
  fs::path canon = fs::weakly_canonical(m_dirPath);
  if (canon != fs::path(expected)) {
    throw exception::Exception("Failed to delete " + m_dirPath + ". Path is not canonical.");
  }
  // Finally remove it
  fs::remove_all(m_dirPath);
}

}  // namespace cta::runtime
