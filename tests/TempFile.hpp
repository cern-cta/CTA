/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
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
  TempFile(const std::string& content, const std::string& suffix);

  const std::string& path() const { return m_path; };

  void randomFill(size_t size);
  uint32_t adler32();
  void stringFill(const std::string& string);
  void stringAppend(const std::string& string);
  ~TempFile();

private:
  std::string m_path;
};
}  // namespace unitTests
