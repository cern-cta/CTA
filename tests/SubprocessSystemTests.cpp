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

#include "common/threading/SubProcess.hpp"

#include <gtest/gtest.h>

namespace systemTests {
TEST(SubProcessHelper, basicTests) {
  cta::threading::SubProcess sp("echo", std::list<std::string>({"echo", "Hello,", "world."}));
  sp.wait();
  ASSERT_EQ("Hello, world.\n", sp.stdout());
  ASSERT_EQ("", sp.stderr());
  ASSERT_EQ(0, sp.exitValue());
  cta::threading::SubProcess sp2("cat", std::list<std::string>({"cat", "/no/such/file"}));
  sp2.wait();
  ASSERT_EQ("", sp2.stdout());
  ASSERT_NE(std::string::npos, sp2.stderr().find("/no/such/file"));
  ASSERT_EQ(1, sp2.exitValue());
  cta::threading::SubProcess sp3("/no/such/file", std::list<std::string>({"/no/such/file"}));
  sp3.wait();
  ASSERT_EQ("", sp3.stdout());
  ASSERT_EQ(127, sp3.exitValue());
  ASSERT_EQ("", sp3.stderr());
}

TEST(SubProcessHelper, testSubprocessWithStdinInput) {
  std::string stdinInput = "{\"integer_number\":42,\"str\":\"forty two\",\"double_number\":42.000000}";
  cta::threading::SubProcess sp2("tee", std::list<std::string>({"tee"}), stdinInput);
  sp2.wait();
  ASSERT_EQ(stdinInput, sp2.stdout());
  ASSERT_EQ(0, sp2.exitValue());
  ASSERT_EQ("", sp2.stderr());
}
}  // namespace systemTests
