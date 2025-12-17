/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "TempFile.hpp"

#include "common/exception/Errnum.hpp"
#include "common/utils/utils.hpp"

#include <fstream>
#include <memory>
#include <stdlib.h>
#include <sys/stat.h>
#include <unistd.h>

namespace unitTests {

TempFile::TempFile() {
  char path[] = "/tmp/testCTA-XXXXXX";
  int fd = ::mkstemp(path);
  cta::exception::Errnum::throwOnMinusOne(fd, "In TempFile::TempFile: failed to mkstemp: ");
  ::close(fd);
  m_path = path;
}

TempFile::TempFile(const std::string& path) : m_path(path) {}

TempFile::~TempFile() {
  if (m_path.size()) {
    ::unlink(m_path.c_str());
  }
}

void TempFile::randomFill(size_t size) {
  std::ofstream out(m_path, std::ios::out | std::ios::binary);
  std::ifstream in("/dev/urandom", std::ios::in | std::ios::binary);
  std::unique_ptr<char[]> buff(new char[size]);
  in.read(buff.get(), size);
  out.write(buff.get(), size);
}

uint32_t TempFile::adler32() {
  struct ::stat fileStat;
  cta::exception::Errnum::throwOnMinusOne(::stat(m_path.c_str(), &fileStat),
                                          "In TempFile::adler32(): failed to stat file.");
  std::unique_ptr<char[]> buff(new char[fileStat.st_size]);
  std::ifstream in(m_path, std::ios::in | std::ios::binary);
  in.read(buff.get(), fileStat.st_size);
  return cta::utils::getAdler32((uint8_t*) buff.get(), fileStat.st_size);
}

void TempFile::stringFill(const std::string& string) {
  std::ofstream out(m_path, std::ios::out | std::ios::binary | std::ios::trunc);
  out << string;
}

void TempFile::stringAppend(const std::string& string) {
  std::ofstream out(m_path, std::ios::out | std::ios::binary | std::ios::app);
  out << string;
}

}  // namespace unitTests
