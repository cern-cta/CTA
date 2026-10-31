/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "TempDirectory.hpp"

#include "common/exception/Errnum.hpp"

#include <filesystem>

namespace unitTests {

TempDirectory::TempDirectory() {
  char path[] = "/tmp/testCTADir-XXXXXX";
  char* dirPath = ::mkdtemp(path);
  cta::exception::Errnum::throwOnNull(dirPath, "In TempDirectory::TempDirectory() failed to mkdtemp: ");
  m_root = std::string(dirPath);
}

TempDirectory::~TempDirectory() {
  if (m_root.size()) {
    // Cleanup must not throw during stack unwinding.
    std::error_code error;
    std::filesystem::remove_all(m_root, error);
  }
}

void TempDirectory::mkdir() {
  std::filesystem::create_directories(m_root + m_subfolder_path);
}

void TempDirectory::append(const std::string& path) {
  m_subfolder_path += path;
}

}  // namespace unitTests
