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

#include "TempDirectory.hpp"
#include "common/exception/Errnum.hpp"

namespace unitTests {
  
TempDirectory::TempDirectory(){
  char path[] = "/tmp/testCTADir-XXXXXX";
  char *dirPath = ::mkdtemp(path);
  cta::exception::Errnum::throwOnNull(dirPath,"In TempDirectory::TempDirectory() failed to mkdtemp: ");
  m_root = std::string(dirPath);
}

TempDirectory::~TempDirectory(){
  if(m_root.size()){
    std::string rmCommand = "rm -rf "+m_root;
    ::system(rmCommand.c_str());
  }
}

void TempDirectory::mkdir(){
  std::string mkdirCommand = "mkdir -p "+m_root + m_subfolder_path;
  ::system(mkdirCommand.c_str());
}

void TempDirectory::append(const std::string& path){
  m_subfolder_path += path;
}

}