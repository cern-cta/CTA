/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#pragma once

#include <string>

namespace unitTests {
/**
 * A class creating a temporary file (deleted by destructor). Various population
 * operations are provided.
 */
class TempFile {
public:
  TempFile();
  explicit TempFile(const std::string& path);
  std::string_view path() { return m_path; };
  void randomFill(size_t size);
  uint32_t adler32();
  void stringFill(const std::string &string);
  void stringAppend(const std::string &string);
  ~TempFile();
private:
  std::string m_path;
};
}
