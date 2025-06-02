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

#include "FileLogger.hpp"
#include "tests/TempFile.hpp"

#include <gtest/gtest.h>
#include <fstream>
#include <sstream>

using namespace cta::log;

namespace unitTests {
TEST(cta_log_FileLogger, basicTest) {
  std::string jat = "Just a test";
  TempFile tf;
  FileLogger fl("dummy", "cta_log_StringLogger", tf.path(), DEBUG);
  fl(INFO, jat);
  std::ifstream ifs(tf.path());
  std::stringstream res;
  res << ifs.rdbuf();
  ASSERT_NE(std::string::npos, res.str().find(jat));
}
}  // namespace unitTests
