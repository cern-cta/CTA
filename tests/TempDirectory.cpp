/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "TempDirectory.hpp"

#include "common/exception/Errnum.hpp"

namespace unitTests {

TempDirectory::TempDirectory() {
  char path[] = "/tmp/testCTADir-XXXXXX";
  char* dirPath = ::mkdtemp(path);
  cta::exception::Errnum::throwOnNull(dirPath, "In TempDirectory::TempDirectory() failed to mkdtemp: ");
  m_root = std::string(dirPath);
}

TempDirectory::~TempDirectory() {
  if (m_root.size()) {
    std::string rmCommand = "rm -rf " + m_root;
    ::system(rmCommand.c_str());
  }
}

void TempDirectory::mkdir() {
  std::string mkdirCommand = "mkdir -p " + m_root + m_subfolder_path;
  ::system(mkdirCommand.c_str());
}

void TempDirectory::append(const std::string& path) {
  m_subfolder_path += path;
}

}  // namespace unitTests
