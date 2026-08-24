/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "common/exception/Errnum.hpp"
#include "common/process/threading/SubProcess.hpp"

#include <gtest/gtest.h>

namespace unitTests {
TEST(SubProcessHelper, basicTests) {
  cta::threading::SubProcess sp("/usr/bin/echo", std::list<std::string>({"/usr/bin/echo", "Hello,", "world."}));
  sp.wait();
  ASSERT_EQ("Hello, world.\n", sp.stdout());
  ASSERT_EQ("", sp.stderr());
  ASSERT_EQ(0, sp.exitValue());
  cta::threading::SubProcess sp2("/usr/bin/cat", std::list<std::string>({"/usr/bin/cat", "/no/such/file"}));
  sp2.wait();
  ASSERT_EQ("", sp2.stdout());
  ASSERT_NE(std::string::npos, sp2.stderr().find("/no/such/file"));
  ASSERT_EQ(1, sp2.exitValue());
  try {
    cta::threading::SubProcess sp3("/no/such/file", std::list<std::string>({"/no/such/file"}));
    sp3.wait();
    EXPECT_NE(0, sp3.exitValue());
  } catch (const cta::exception::Errnum&) {
    // Valgrind may report the exec error through a nonzero child exit instead of returning it to the parent.
    // See https://bugs.kde.org/show_bug.cgi?id=481679
  }
}

TEST(SubProcessHelper, testSubprocessWithStdinInput) {
  std::string stdinInput = "{\"integer_number\":42,\"str\":\"forty two\",\"double_number\":42.000000}";
  cta::threading::SubProcess sp2("tee", std::list<std::string>({"tee"}), stdinInput);
  sp2.wait();
  ASSERT_EQ("", sp2.stderr());
  ASSERT_EQ(0, sp2.exitValue());
  ASSERT_EQ(stdinInput, sp2.stdout());
}
}  // namespace unitTests
