/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once
#include <string>
namespace unitTests {

class TempDirectory{
  public:
    TempDirectory();
    TempDirectory(const std::string& rootPath): m_root(rootPath) {}
    std::string path() {return m_root + m_subfolder_path; }
    void append(const std::string & path);
    void mkdir();
    ~TempDirectory();
  private:
    std::string m_root;
    std::string m_subfolder_path;
};

}

