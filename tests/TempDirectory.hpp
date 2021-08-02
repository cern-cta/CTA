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

