/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "CliArgParser.hpp"

#include <chrono>
#include <functional>
#include <gtest/gtest.h>
#include <thread>

namespace unitTests {

// Since we want to initialise this with an initialiser list, we need a struct to "own" the storage of the argument list
struct Argv {
  int count = 0;
  std::vector<std::string> storage;  // owns the strings
  std::vector<char*> argv;           // points into storage

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

  runtime::CommonCliOptions opts;
  runtime::ArgParser argParser(appName, opts);
  ASSERT_FALSE(opts.showHelp);
  bool success = argParser.parse(args.count, args.data());  // TODO: this will call exit()
  ASSERT_TRUE(success);
  ASSERT_TRUE(opts.showHelp);
  // TODO: test show help?
}

TEST(ArgParser, SetsStrictConfigCorrectly) {
  const std::string appName = "cta-test";
  Argv args({appName, "--config-strict"});

  runtime::CommonCliOptions opts;
  runtime::ArgParser argParser(appName, opts);
  ASSERT_FALSE(opts.configStrict);
  bool success = argParser.parse(args.count, args.data());
  ASSERT_TRUE(success);
  ASSERT_TRUE(opts.configStrict);
}

TEST(ArgParser, DefaultConfigPath) {
  const std::string appName = "cta-test";
  Argv args({appName});

  runtime::CommonCliOptions opts;
  runtime::ArgParser argParser(appName, opts);
  bool success = argParser.parse(args.count, args.data());
  ASSERT_TRUE(success);
  ASSERT_EQ(opts.configFilePath, "/etc/cta/" + appName + ".toml");
}

TEST(ArgParser, SetConfigPath) {
  const std::string appName = "cta-test";
  Argv args({appName, "--config", "/etc/cta/differentpath.toml"});

  runtime::CommonCliOptions opts;
  runtime::ArgParser argParser(appName, opts);
  bool success = argParser.parse(args.count, args.data());
  ASSERT_TRUE(success);
  ASSERT_EQ(opts.configFilePath, "/etc/cta/differentpath.toml");
}

TEST(ArgParser, LogFilePathEmptyByDefault) {
  const std::string appName = "cta-test";
  Argv args({appName});

  runtime::CommonCliOptions opts;
  runtime::ArgParser argParser(appName, opts);
  ASSERT_FALSE(opts.logFilePath.has_value());
  bool success = argParser.parse(args.count, args.data());
  ASSERT_TRUE(success);
  ASSERT_FALSE(opts.logFilePath.has_value());
}

TEST(ArgParser, LogFilePathSetCorrectly) {
  const std::string appName = "cta-test";
  Argv args({appName, "--log-file", "/tmp/test.log"});

  runtime::CommonCliOptions opts;
  runtime::ArgParser argParser(appName, opts);
  ASSERT_FALSE(opts.logFilePath.has_value());
  bool success = argParser.parse(args.count, args.data());
  ASSERT_TRUE(success);
  ASSERT_TRUE(opts.logFilePath.has_value());
  ASSERT_EQ(opts.logFilePath, "/tmp/test.log");
}

TEST(ArgParser, MissingArgumentFails) {
  const std::string appName = "cta-test";
  Argv args({appName, "--log-file"});

  runtime::CommonCliOptions opts;
  runtime::ArgParser argParser(appName, opts);
  ASSERT_FALSE(opts.logFilePath.has_value());
  bool success = argParser.parse(args.count, args.data());
  ASSERT_FALSE(success);
}

TEST(ArgParser, UnrecognisedLongFlagFails) {
  const std::string appName = "cta-test";
  Argv args({appName, "--i-dont-exist"});

  runtime::CommonCliOptions opts;
  runtime::ArgParser argParser(appName, opts);
  ASSERT_FALSE(opts.logFilePath.has_value());
  bool success = argParser.parse(args.count, args.data());
  ASSERT_FALSE(success);
}

TEST(ArgParser, UnrecognisedShortFlagFails) {
  const std::string appName = "cta-test";
  Argv args({appName, "-i"});

  runtime::CommonCliOptions opts;
  runtime::ArgParser argParser(appName, opts);
  ASSERT_FALSE(opts.logFilePath.has_value());
  bool success = argParser.parse(args.count, args.data());
  ASSERT_FALSE(success);
}

TEST(ArgParser, WithCustomStructSameOptions) {
  struct SameOptionsAsCliOptions {
    bool showHelp = false;
    bool configStrict = false;
    std::string configFilePath;
    std::optional<std::string> logFilePath;
    std::string iAmExtra;
  };

  const std::string appName = "cta-test";
  Argv args({appName, "--extra", "extra"});

  runtime::SameOptionsAsCliOptions opts;
  runtime::ArgParser argParser(appName, opts);
  argParser.withStringArg(&SameOptionsAsCliOptions::iAmExtra, "extra", 'e', "stuff", "Some extra argument");
  ASSERT_TRUE(opts.iAmExtra.empty());
  bool success = argParser.parse(args.count, args.data());
  ASSERT_TRUE(success);
  ASSERT_EQ(opts.iAmExtra, "extra");
}

TEST(ArgParser, WithCustomStructExtends) {
  struct ExtendsFromCliOptions : public CommonCliOptions {
    std::string iAmExtra;
  };

  const std::string appName = "cta-test";
  Argv args({appName, "--extra", "extra"});

  runtime::ExtendsFromCliOptions opts;
  runtime::ArgParser argParser(appName, opts);
  argParser.withStringArg(&ExtendsFromCliOptions::iAmExtra, "extra", 'e', "stuff", "Some extra argument");
  ASSERT_TRUE(opts.iAmExtra.empty());
  bool success = argParser.parse(args.count, args.data());
  ASSERT_TRUE(success);
  ASSERT_EQ(opts.iAmExtra, "extra");
}

TEST(ArgParser, ShortHandStringFlag) {
  struct ExtendsFromCliOptions : public CommonCliOptions {
    std::string iAmExtra;
  };

  const std::string appName = "cta-test";
  Argv args({appName, "-e", "extra"});

  runtime::ExtendsFromCliOptions opts;
  runtime::ArgParser argParser(appName, opts);
  argParser.withStringArg(&ExtendsFromCliOptions::iAmExtra, "extra", 'e', "stuff", "Some extra argument");
  ASSERT_TRUE(opts.iAmExtra.empty());
  bool success = argParser.parse(args.count, args.data());
  ASSERT_TRUE(success);
  ASSERT_EQ(opts.iAmExtra, "extra");
}

TEST(ArgParser, ShortHandBoolFlag) {
  struct ExtendsFromCliOptions : public CommonCliOptions {
    bool iAmExtra;
  };

  const std::string appName = "cta-test";
  Argv args({appName, "-b"});

  runtime::ExtendsFromCliOptions opts;
  runtime::ArgParser argParser(appName, opts);
  argParser.withBoolArg(&ExtendsFromCliOptions::iAmExtra, "extra", 'e', "Some extra argument");
  ASSERT_FALSE(opts.iAmExtra);
  bool success = argParser.parse(args.count, args.data());
  ASSERT_TRUE(success);
  ASSERT_TRUE(opts.iAmExtra);
}

}  // namespace unitTests
