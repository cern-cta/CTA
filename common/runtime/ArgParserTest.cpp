/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "CliArgParser.hpp"
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
      runtime::ArgParser<CommonCliOptions> argParser(appName);
      argParser.parse(args.count, args.data());
    },
    ::testing::ExitedWithCode(EXIT_SUCCESS),
    "Usage: cta-test");
}

TEST(ArgParser, SetsStrictConfigCorrectly) {
  const std::string appName = "cta-test";
  Argv args({appName, "--config-strict"});

  runtime::ArgParser<CommonCliOptions> argParser(appName);
  auto opts = argParser.parse(args.count, args.data());
  ASSERT_TRUE(opts.configStrict);
}

TEST(ArgParser, OptionalConfigPath) {
  const std::string appName = "cta-test";
  Argv args({appName});

  runtime::ArgParser<CommonCliOptions> argParser(appName);
  auto opts = argParser.parse(args.count, args.data());
  ASSERT_FALSE(opts.configFilePath.has_value());
}

TEST(ArgParser, SetConfigPath) {
  const std::string appName = "cta-test";
  Argv args({appName, "--config", "/etc/cta/differentpath.toml"});

  runtime::ArgParser<CommonCliOptions> argParser(appName);
  auto opts = argParser.parse(args.count, args.data());
  ASSERT_TRUE(opts.configFilePath.has_value());
  ASSERT_EQ(opts.configFilePath, "/etc/cta/differentpath.toml");
}

TEST(ArgParser, LogFilePathEmptyByDefault) {
  const std::string appName = "cta-test";
  Argv args({appName});

  runtime::ArgParser<CommonCliOptions> argParser(appName);
  auto opts = argParser.parse(args.count, args.data());
  ASSERT_FALSE(opts.logFilePath.has_value());
}

TEST(ArgParser, LogFilePathSetCorrectly) {
  const std::string appName = "cta-test";
  Argv args({appName, "--log-file", "/tmp/test.log"});

  runtime::ArgParser<CommonCliOptions> argParser(appName);
  auto opts = argParser.parse(args.count, args.data());
  ASSERT_TRUE(opts.logFilePath.has_value());
  ASSERT_EQ(opts.logFilePath.value(), "/tmp/test.log");
}

TEST(ArgParser, MissingArgumentFails) {
  const std::string appName = "cta-test";
  Argv args({appName, "--log-file"});

  runtime::ArgParser<CommonCliOptions> argParser(appName);
  EXPECT_THROW(argParser.parse(args.count, args.data()), exception::UserError);
}

TEST(ArgParser, UnrecognisedLongFlagFails) {
  const std::string appName = "cta-test";
  Argv args({appName, "--i-dont-exist"});

  runtime::ArgParser<CommonCliOptions> argParser(appName);
  EXPECT_THROW(argParser.parse(args.count, args.data()), exception::UserError);
}

TEST(ArgParser, UnrecognisedShortFlagFails) {
  const std::string appName = "cta-test";
  Argv args({appName, "-i"});

  runtime::ArgParser<CommonCliOptions> argParser(appName);
  EXPECT_THROW(argParser.parse(args.count, args.data()), exception::UserError);
}

TEST(ArgParser, WithCustomStructSameOptions) {
  // Shows that we don't need to extend the struct as long as we define the same member variables
  struct SameOptionsAsCliOptions {
    bool showHelp = false;
    bool configStrict = false;
    std::optional<std::string> configFilePath;
    std::optional<std::string> logFilePath;
    std::string iAmExtra;
  };

  const std::string appName = "cta-test";
  Argv args({appName, "--extra", "extra"});

  runtime::ArgParser<SameOptionsAsCliOptions> argParser(appName);
  argParser.withStringArg(&SameOptionsAsCliOptions::iAmExtra, "extra", 'e', "stuff", "Some extra argument");
  auto opts = argParser.parse(args.count, args.data());
  ASSERT_EQ(opts.iAmExtra, "extra");
}

TEST(ArgParser, WithCustomStructExtends) {
  struct ExtendsFromCliOptions : public CommonCliOptions {
    std::string iAmExtra;
  };

  const std::string appName = "cta-test";
  Argv args({appName, "--extra", "extra"});

  runtime::ArgParser<ExtendsFromCliOptions> argParser(appName);
  argParser.withStringArg(&ExtendsFromCliOptions::iAmExtra, "extra", 'e', "stuff", "Some extra argument");
  auto opts = argParser.parse(args.count, args.data());
  ASSERT_EQ(opts.iAmExtra, "extra");
}

TEST(ArgParser, ShortHandStringFlag) {
  struct ExtendsFromCliOptions : public CommonCliOptions {
    std::string iAmExtra;
  };

  const std::string appName = "cta-test";
  Argv args({appName, "-e", "extra"});

  runtime::ArgParser<ExtendsFromCliOptions> argParser(appName);
  argParser.withStringArg(&ExtendsFromCliOptions::iAmExtra, "extra", 'e', "stuff", "Some extra argument");
  auto opts = argParser.parse(args.count, args.data());
  ASSERT_EQ(opts.iAmExtra, "extra");
}

TEST(ArgParser, ShortHandBoolFlag) {
  struct ExtendsFromCliOptions : public CommonCliOptions {
    bool iAmExtra;
  };

  const std::string appName = "cta-test";
  Argv args({appName, "-b"});

  runtime::ArgParser<ExtendsFromCliOptions> argParser(appName);
  argParser.withBoolArg(&ExtendsFromCliOptions::iAmExtra, "bool-extra", 'b', "Some boolean extra argument");
  auto opts = argParser.parse(args.count, args.data());
  ASSERT_TRUE(opts.iAmExtra);
}

}  // namespace unitTests
