/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2015-2021 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "TempFile.hpp"
#include "common/exception/Errnum.hpp"
#include "common/utils/utils.hpp"

#include <stdlib.h>
#include <unistd.h>
#include <fstream>
#include <memory>
#include <sys/stat.h>


namespace unitTests {

TempFile::TempFile() {
  char path[] = "/tmp/testCTA-XXXXXX";
  int fd = ::mkstemp(path);
  cta::exception::Errnum::throwOnMinusOne(fd, "In TempFile::TempFile: failed to mkstemp: ");
  ::close(fd);
  m_path = path;
}

TempFile::TempFile(const std::string& path) : m_path(path) { }

TempFile::~TempFile() {
  if (m_path.size()) {
    ::unlink(m_path.c_str());
  }
}

std::string TempFile::path() {
  return m_path;
}

void TempFile::randomFill(size_t size) {
  std::ofstream out(m_path, std::ios::out | std::ios::binary);
  std::ifstream in("/dev/urandom", std::ios::in | std::ios::binary);
  std::unique_ptr<char[] > buff(new char[size]);
  in.read(buff.get(), size);
  out.write(buff.get(), size);
}

uint32_t TempFile::adler32() {
  struct ::stat fileStat;
  cta::exception::Errnum::throwOnMinusOne(::stat(m_path.c_str(), &fileStat),
      "In TempFile::adler32(): failed to stat file.");
  std::unique_ptr<char[] > buff(new char[fileStat.st_size]);
  std::ifstream in(m_path, std::ios::in | std::ios::binary);
  in.read(buff.get(), fileStat.st_size);
  return cta::utils::getAdler32((uint8_t*)buff.get(), fileStat.st_size);
}

void TempFile::stringFill(const std::string& string) {
  std::ofstream out(m_path, std::ios::out | std::ios::binary | std::ios::trunc);
  out << string;
}

void TempFile::stringAppend(const std::string& string) {
  std::ofstream out(m_path, std::ios::out | std::ios::binary | std::ios::app);
  out << string;
}
  
}
