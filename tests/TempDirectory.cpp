/**
 * The CERN Tape Archive (CTA) project
 * Copyright © 2018 CERN
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

#include "TempDirectory.hpp"
#include "common/exception/Errnum.hpp"

namespace unitTests {
  
TempDirectory::TempDirectory(){
  char path[] = "/tmp/testCTADir-XXXXXX";
  char *dirPath = ::mkdtemp(path);
  cta::exception::Errnum::throwOnNull(dirPath,"In TempDirectory::TempDirectory() failed to mkdtemp: ");
  m_path = std::string(dirPath);
}

TempDirectory::~TempDirectory(){
  if(m_path.size()){
    std::string rmCommand = "rm -rf "+m_path;
    ::system(rmCommand.c_str());
  }
}

}