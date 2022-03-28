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

