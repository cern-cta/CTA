/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "ArgParser.hpp"

#include "CommonCliOptions.hpp"
#include "common/exception/UserError.hpp"

#include <chrono>
#include <functional>
#include <gtest/gtest.h>
#include <thread>

namespace unitTests {

// Since we want to (be able to) initialise this with an initialiser list, we need a struct to "own" the storage of the argument list
struct Argv {
  int count = 0;
  std::vector<std::string> storage;  // owns the strings
  std::vector<char*> argv;           // points to the storage

  Argv(std::initializer_list<std::string> args) : storage(args) {
    count = static_cast<int>(storage.size());
    argv.reserve(storage.size() + 1);

    for (auto& s : storage) {
      argv.push_back(s.data());
    }
    argv.push_back(nullptr);  // argv must be null-terminated
  }

  char** data() { return argv.data(); }
};

TEST(ArgParser, SetsHelpCorrectly) {
  const std::string appName = "cta-test";
  Argv args({appName, "--help"});

  EXPECT_EXIT(
    {
      cta::runtime::ArgParser<cta::runtime::CommonCliOptions> argParser(appName);
      argParser.parse(args.count, args.data());
    },
    ::testing::ExitedWithCode(EXIT_SUCCESS),
    "Usage: cta-test");
}

TEST(ArgParser, SetsStrictConfigCorrectly) {
  const std::string appName = "cta-test";
  Argv args({appName, "--config-strict"});

  cta::runtime::ArgParser<cta::runtime::CommonCliOptions> argParser(appName);
  auto opts = argParser.parse(args.count, args.data());
  ASSERT_TRUE(opts.configStrict);
}

TEST(ArgParser, OptionalConfigPath) {
  const std::string appName = "cta-test";
  Argv args({appName});

  cta::runtime::ArgParser<cta::runtime::CommonCliOptions> argParser(appName);
  auto opts = argParser.parse(args.count, args.data());
  ASSERT_TRUE(opts.configFilePath.empty());
}

TEST(ArgParser, SetConfigPath) {
  const std::string appName = "cta-test";
  Argv args({appName, "--config", "/etc/cta/differentpath.toml"});

  cta::runtime::ArgParser<cta::runtime::CommonCliOptions> argParser(appName);
  auto opts = argParser.parse(args.count, args.data());
  ASSERT_EQ(opts.configFilePath, "/etc/cta/differentpath.toml");
}

TEST(ArgParser, LogFilePathEmptyByDefault) {
  const std::string appName = "cta-test";
  Argv args({appName});

  cta::runtime::ArgParser<cta::runtime::CommonCliOptions> argParser(appName);
  auto opts = argParser.parse(args.count, args.data());
  ASSERT_TRUE(opts.logFilePath.empty());
}

TEST(ArgParser, LogFilePathSetCorrectly) {
  const std::string appName = "cta-test";
  Argv args({appName, "--log-file", "/tmp/test.log"});

  cta::runtime::ArgParser<cta::runtime::CommonCliOptions> argParser(appName);
  auto opts = argParser.parse(args.count, args.data());
  ASSERT_EQ(opts.logFilePath, "/tmp/test.log");
}

TEST(ArgParser, MissingArgumentFails) {
  const std::string appName = "cta-test";
  Argv args({appName, "--log-file"});

  cta::runtime::ArgParser<cta::runtime::CommonCliOptions> argParser(appName);
  EXPECT_THROW(argParser.parse(args.count, args.data()), cta::exception::UserError);
}

TEST(ArgParser, UnrecognisedLongFlagFails) {
  const std::string appName = "cta-test";
  Argv args({appName, "--i-dont-exist"});

  cta::runtime::ArgParser<cta::runtime::CommonCliOptions> argParser(appName);
  EXPECT_THROW(argParser.parse(args.count, args.data()), cta::exception::UserError);
}

TEST(ArgParser, UnrecognisedShortFlagFails) {
  const std::string appName = "cta-test";
  Argv args({appName, "-i"});

  cta::runtime::ArgParser<cta::runtime::CommonCliOptions> argParser(appName);
  EXPECT_THROW(argParser.parse(args.count, args.data()), cta::exception::UserError);
}

TEST(ArgParser, WithCustomStructSameOptions) {
  // Shows that we don't need to extend the struct as long as we define the same member variables
  struct SameOptionsAsCliOptions {
    bool showHelp = false;
    bool configStrict = false;
    std::string configFilePath;
    std::string logFilePath;
    std::string iAmExtra;
  };

  const std::string appName = "cta-test";
  Argv args({appName, "--extra", "extra"});

  cta::runtime::ArgParser<SameOptionsAsCliOptions> argParser(appName);
  argParser.withStringArg(&SameOptionsAsCliOptions::iAmExtra, "extra", 'e', "stuff", "Some extra argument");
  auto opts = argParser.parse(args.count, args.data());
  ASSERT_EQ(opts.iAmExtra, "extra");
}

TEST(ArgParser, WithCustomStructExtends) {
  struct ExtendsFromCliOptions : public cta::runtime::CommonCliOptions {
    std::string iAmExtra;
  };

  const std::string appName = "cta-test";
  Argv args({appName, "--extra", "extra"});

  cta::runtime::ArgParser<ExtendsFromCliOptions> argParser(appName);
  argParser.withStringArg(&ExtendsFromCliOptions::iAmExtra, "extra", 'e', "stuff", "Some extra argument");
  auto opts = argParser.parse(args.count, args.data());
  ASSERT_EQ(opts.iAmExtra, "extra");
}

TEST(ArgParser, ShortHandStringFlag) {
  struct ExtendsFromCliOptions : public cta::runtime::CommonCliOptions {
    std::string iAmExtra;
  };

  const std::string appName = "cta-test";
  Argv args({appName, "-e", "extra"});

  cta::runtime::ArgParser<ExtendsFromCliOptions> argParser(appName);
  argParser.withStringArg(&ExtendsFromCliOptions::iAmExtra, "extra", 'e', "stuff", "Some extra argument");
  auto opts = argParser.parse(args.count, args.data());
  ASSERT_EQ(opts.iAmExtra, "extra");
}

TEST(ArgParser, ShortHandBoolFlag) {
  struct ExtendsFromCliOptions : public cta::runtime::CommonCliOptions {
    bool iAmExtra;
  };

  const std::string appName = "cta-test";
  Argv args({appName, "-b"});

  cta::runtime::ArgParser<ExtendsFromCliOptions> argParser(appName);
  argParser.withBoolArg(&ExtendsFromCliOptions::iAmExtra, "bool-extra", 'b', "Some boolean extra argument");
  auto opts = argParser.parse(args.count, args.data());
  ASSERT_TRUE(opts.iAmExtra);
}

}  // namespace unitTests
