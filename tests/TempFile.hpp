/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
  TempFile(const std::string& path);
  std::string path();
  void randomFill(size_t size);
  uint32_t adler32();
  void stringFill(const std::string &string);
  void stringAppend(const std::string &string);
  ~TempFile();
private:
  std::string m_path;
};
}
