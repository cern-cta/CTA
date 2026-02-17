/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "ArgParser.hpp"

#include "CommonCliOptions.hpp"
#include "RuntimeTestHelpers.hpp"
#include "common/exception/UserError.hpp"
#include "version.h"

#include <chrono>
#include <functional>
#include <gtest/gtest.h>
#include <string>
#include <thread>

namespace unitTests {

TEST(ArgParser, SetsStrictConfigCorrectly) {
  const std::string appName = "cta-test";
  Argv args({appName, "--config-strict"});

  cta::runtime::ArgParser<cta::runtime::CommonCliOptions> argParser(appName);
  auto opts = argParser.parse(args.count, args.data());
  ASSERT_TRUE(opts.configStrict);
}

TEST(ArgParser, RepeatedBooleanFlag) {
  const std::string appName = "cta-test";
  Argv args({appName, "--config-strict", "--config-strict"});

  cta::runtime::ArgParser<cta::runtime::CommonCliOptions> argParser(appName);
  auto opts = argParser.parse(args.count, args.data());
  ASSERT_TRUE(opts.configStrict);
}

TEST(ArgParser, OptionalConfigPath) {
  const std::string appName = "cta-test";
  Argv args({appName});

  cta::runtime::ArgParser<cta::runtime::CommonCliOptions> argParser(appName);
  auto opts = argParser.parse(args.count, args.data());
  ASSERT_EQ(opts.configFilePath, "/etc/cta/cta-test.toml");
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
  // Note that if you added options to CommonCliOptions, these must be added here as well
  // Or this test won't compile (which is the whole point)
  struct SameOptionsAsCliOptions {
    bool showHelp = false;
    bool showVersion = false;
    std::string runtimeDir;
    bool configCheck = false;
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

TEST(ArgParser, UsageStringDefault) {
  const std::string appName = "cta-test";
  cta::runtime::ArgParser<cta::runtime::CommonCliOptions> argParser(appName);

  std::string expectedUsageString = R"""(Usage: cta-test [OPTIONS]

Options:
  -l, --log-file PATH
      Write logs to PATH (defaults to stdout/stderr).
  -c, --config PATH
      Path to the main configuration file. Defaults to /etc/cta/cta-test.toml if not provided.
  --config-strict
      Treat unknown keys, missing keys, and type mismatches in the config file as errors.
  --config-check
      Validate the configuration, then exit. Respects --config-strict.
  --runtime-dir PATH
      Put state metadata such as the consumed config and version information into the provided directory.
  -v, --version
      Print version information, then exit.
  -h, --help
      Show this help message, then exit.)""";

  ASSERT_EQ(argParser.usageString(), expectedUsageString);
}

TEST(ArgParser, UsageStringDefaultWithDescription) {
  const std::string appName = "cta-test";
  cta::runtime::ArgParser<cta::runtime::CommonCliOptions> argParser(appName);
  argParser.withDescription("my fancy description");

  std::string expectedUsageString = R"""(Usage: cta-test [OPTIONS]

my fancy description

Options:
  -l, --log-file PATH
      Write logs to PATH (defaults to stdout/stderr).
  -c, --config PATH
      Path to the main configuration file. Defaults to /etc/cta/cta-test.toml if not provided.
  --config-strict
      Treat unknown keys, missing keys, and type mismatches in the config file as errors.
  --config-check
      Validate the configuration, then exit. Respects --config-strict.
  --runtime-dir PATH
      Put state metadata such as the consumed config and version information into the provided directory.
  -v, --version
      Print version information, then exit.
  -h, --help
      Show this help message, then exit.)""";

  ASSERT_EQ(argParser.usageString(), expectedUsageString);
}

TEST(ArgParser, UsageStringDefaultWithMultilineDescription) {
  const std::string appName = "cta-test";
  cta::runtime::ArgParser<cta::runtime::CommonCliOptions> argParser(appName);
  argParser.withDescription("my fancy description\nA second line!");

  std::string expectedUsageString = R"""(Usage: cta-test [OPTIONS]

my fancy description
A second line!

Options:
  -l, --log-file PATH
      Write logs to PATH (defaults to stdout/stderr).
  -c, --config PATH
      Path to the main configuration file. Defaults to /etc/cta/cta-test.toml if not provided.
  --config-strict
      Treat unknown keys, missing keys, and type mismatches in the config file as errors.
  --config-check
      Validate the configuration, then exit. Respects --config-strict.
  --runtime-dir PATH
      Put state metadata such as the consumed config and version information into the provided directory.
  -v, --version
      Print version information, then exit.
  -h, --help
      Show this help message, then exit.)""";

  ASSERT_EQ(argParser.usageString(), expectedUsageString);
}

TEST(ArgParser, UsageStringExtra) {
  struct ExtendsFromCliOptions : public cta::runtime::CommonCliOptions {
    std::string iAmExtra;
  };

  const std::string appName = "cta-test";
  cta::runtime::ArgParser<ExtendsFromCliOptions> argParser(appName);
  argParser.withStringArg(&ExtendsFromCliOptions::iAmExtra, "extra", 'e', "STUFF", "Some extra argument.");

  std::string expectedUsageString = R"""(Usage: cta-test [OPTIONS]

Options:
  -e, --extra STUFF
      Some extra argument.
  -l, --log-file PATH
      Write logs to PATH (defaults to stdout/stderr).
  -c, --config PATH
      Path to the main configuration file. Defaults to /etc/cta/cta-test.toml if not provided.
  --config-strict
      Treat unknown keys, missing keys, and type mismatches in the config file as errors.
  --config-check
      Validate the configuration, then exit. Respects --config-strict.
  --runtime-dir PATH
      Put state metadata such as the consumed config and version information into the provided directory.
  -v, --version
      Print version information, then exit.
  -h, --help
      Show this help message, then exit.)""";

  ASSERT_EQ(argParser.usageString(), expectedUsageString);
}

TEST(ArgParser, VersionString) {
  const std::string appName = "cta-test";
  cta::runtime::ArgParser<cta::runtime::CommonCliOptions> argParser(appName);

  std::string expectedVersionString =
    "cta-test " + std::string(CTA_VERSION) + "\nCopyright (C) 2026 CERN\nLicense GPL-3.0-or-later";

  ASSERT_EQ(argParser.versionString(), expectedVersionString);
}

TEST(ArgParser, SinglePositionalArgument) {
  struct PositionalArgsOptions : public cta::runtime::CommonCliOptions {
    std::vector<std::string> positionalArgs;
  };

  const std::string appName = "cta-test";
  Argv args({appName, "arg1"});

  cta::runtime::ArgParser<PositionalArgsOptions> argParser(appName);
  argParser.withPositionalArgs();
  auto opts = argParser.parse(args.count, args.data());
  ASSERT_EQ(opts.positionalArgs.size(), 1);
  ASSERT_EQ(opts.positionalArgs[0], "arg1");
}

TEST(ArgParser, MultiplePositionalArguments) {
  struct PositionalArgsOptions : public cta::runtime::CommonCliOptions {
    std::vector<std::string> positionalArgs;
  };

  const std::string appName = "cta-test";
  Argv args({appName, "arg1", "arg2", "arg3"});

  cta::runtime::ArgParser<PositionalArgsOptions> argParser(appName);
  argParser.withPositionalArgs();
  auto opts = argParser.parse(args.count, args.data());
  ASSERT_EQ(opts.positionalArgs.size(), 3);
  ASSERT_EQ(opts.positionalArgs[0], "arg1");
  ASSERT_EQ(opts.positionalArgs[1], "arg2");
  ASSERT_EQ(opts.positionalArgs[2], "arg3");
}

TEST(ArgParser, MultiplePositionalArgumentsWithFlags) {
  struct PositionalArgsOptions : public cta::runtime::CommonCliOptions {
    std::vector<std::string> positionalArgs;
  };

  const std::string appName = "cta-test";
  Argv args({appName, "arg1", "--config", "myconfig", "arg2", "arg3", "--config-strict"});

  cta::runtime::ArgParser<PositionalArgsOptions> argParser(appName);
  argParser.withPositionalArgs();
  auto opts = argParser.parse(args.count, args.data());
  ASSERT_EQ(opts.positionalArgs.size(), 3);
  ASSERT_EQ(opts.positionalArgs[0], "arg1");
  ASSERT_EQ(opts.positionalArgs[1], "arg2");
  ASSERT_EQ(opts.positionalArgs[2], "arg3");
  ASSERT_TRUE(opts.configStrict);
  ASSERT_EQ(opts.configFilePath, "myconfig");
}

}  // namespace unitTests
