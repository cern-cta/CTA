/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "CommonCliOptions.hpp"
#include "common/exception/UserError.hpp"
#include "version.h"

#include <algorithm>
#include <cassert>
#include <cstdlib>
#include <functional>
#include <getopt.h>
#include <iostream>
#include <optional>
#include <ranges>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace cta::runtime {

struct ArgSpec {
  std::optional<char> shortFlag;       // e.g. "c" (no dash), nullopt => no shorthand
  std::string longFlag;                // e.g. "help". Used to show --help
  std::optional<std::string> argName;  // e.g. "config-file". Used to show --config <config-file>
  std::string description;             // Description for the flag

  std::function<void(std::optional<std::string_view>)> apply;
};

// Basic usage
//  runtime::CommonCliOptions opts;
//  runtime::ArgParser argParser(appName, opts);
//  argParser.parser(argc, argv);
// no support for positional arguments yet, but should not be too complicated to add
// TODO: parser builder
template<class T>
class ArgParser {
public:
  ArgParser(const std::string& appName)
    requires HasRequiredCliOptions<T>
      : m_appName(appName) {
    // Help is displayed in reverse order so that --help always shows last and custom added arguments show first
    withBoolArg(&T::showHelp, "help", 'h', "Show this help message, then exit.");
    withBoolArg(&T::showVersion, "version", 'v', "Print version information, then exit.");
    withBoolArg(&T::configCheck,
                "config-check",
                std::nullopt,
                "Validate the configuration, then exit. Respects --config-strict.");
    withBoolArg(&T::configStrict,
                "config-strict",
                std::nullopt,
                "Treat unknown keys, missing keys, and type mismatches in the config file as errors.");

    withStringArg(&T::configFilePath,
                  "config",
                  'c',
                  "PATH",
                  ("Path to the main configuration file "
                   "(default: /etc/cta/"
                   + m_appName + ".toml)."));
    withStringArg(&T::logFilePath, "log-file", 'l', "PATH", "Write logs to PATH (defaults to stdout/stderr).");
  }

  void withDescription(const std::string& description) { m_appDescription = description; }

  void withStringArg(std::string T::* field,
                     const std::string& longFlag,
                     std::optional<char> shortFlag,
                     const std::string& argName,
                     const std::string& description) {
    ArgSpec arg;
    arg.longFlag = longFlag;
    arg.shortFlag = shortFlag;
    arg.argName = argName;
    arg.description = description;
    arg.apply = [this, field, longFlag](std::optional<std::string_view> argVal) {
      if (!argVal.has_value() || argVal.value().empty()) {
        throw cta::exception::UserError("Missing required argument for option: --" + longFlag);
      }
      m_options.*field = std::string(argVal.value());
    };
    assertCorrectArgSpec(arg);
    m_supportedArgs.push_back(arg);
  }

  void withBoolArg(bool T::* field,
                   const std::string& longFlag,
                   std::optional<char> shortFlag,
                   const std::string& description) {
    ArgSpec arg;
    arg.longFlag = longFlag;
    arg.shortFlag = shortFlag;
    arg.description = description;
    arg.apply = [this, field](std::optional<std::string_view>) { m_options.*field = true; };
    assertCorrectArgSpec(arg);
    m_supportedArgs.push_back(arg);
  }

  T parse(const int argc, char** const argv) {
    // Build short option string and long option array
    std::string shortopts;
    shortopts.reserve(m_supportedArgs.size() * 2 + 1);
    shortopts.push_back(':');  // return ':' on missing required argument
    std::vector<option> longopts;
    longopts.reserve(m_supportedArgs.size() + 1);

    // Map getopt_long return value -> spec index
    std::unordered_map<int, std::size_t> valToIndex;
    valToIndex.reserve(m_supportedArgs.size());

    // avoid collision with ASCII short flags
    int nextLongOnlyVal = 256;

    for (std::size_t i = 0; i < m_supportedArgs.size(); ++i) {
      const ArgSpec& s = m_supportedArgs[i];

      // The value that will be returned by getopt_long
      int val = 0;
      if (s.shortFlag.has_value()) {
        // For short options, just return the corresponding ASCII value
        val = static_cast<unsigned char>(*s.shortFlag);

        shortopts.push_back(static_cast<char>(val));
        if (s.argName.has_value()) {
          shortopts.push_back(':');
        }
      } else {
        // For long options, return a unique value
        val = nextLongOnlyVal++;
      }

      option o {};
      o.name = s.longFlag.c_str();
      o.has_arg = s.argName.has_value() ? required_argument : no_argument;
      o.flag = nullptr;
      o.val = val;

      longopts.push_back(o);
      // Map the return value of getopt_long to the index in the argument array
      valToIndex.emplace(val, i);
    }

    // getopt_long expects an all-zero terminator
    longopts.push_back(option {nullptr, 0, nullptr, 0});

    // Reset getopt globals
    opterr = 0;  // Disable error output from getopt
    optind = 0;  // Reset getopt_long parser state to index 0

    while (true) {
      int optionIndex = 0;
      const int tokenIndex = optind;
      optopt = 0;
      const int c = ::getopt_long(argc, argv, shortopts.c_str(), longopts.data(), &optionIndex);
      if (c == -1) {
        // Done
        break;
      }

      if (c == '?' || c == ':') {
        std::string offending;
        if (tokenIndex >= 0 && tokenIndex < argc && argv[tokenIndex]) {
          offending = argv[tokenIndex];
        } else if (optopt != 0) {
          offending = "-" + std::string(1, static_cast<char>(optopt));
        } else {
          offending = "<unknown>";
        }

        if (c == '?') {
          throw cta::exception::UserError("Unknown option: \"" + offending + "\". Use --help to see valid options.");
        } else {
          throw cta::exception::UserError("Missing required argument for: \"" + offending
                                          + "\". Use --help to see valid options.");
        }
      }

      const auto it = valToIndex.find(c);
      if (it == valToIndex.end()) {
        throw cta::exception::Exception("Internal error: parsed option not mapped to a spec.");
      }

      std::optional<std::string_view> arg;
      if (optarg) {
        arg = std::string_view {optarg};
      }
      // Dispatch
      const ArgSpec& spec = m_supportedArgs[it->second];
      spec.apply(arg);
    }

    // If we ever want to add positional arguments, do that here:
    // for (int i = optind; i < argc; ++i) { ... }

    if (m_options.showHelp) {
      std::cout << usageString() << std::endl;
      std::exit(EXIT_SUCCESS);
    }
    if (m_options.showVersion) {
      std::cout << versionString() << std::endl;
      std::exit(EXIT_SUCCESS);
    }
    return m_options;
  }

  std::string usageString() const {
    auto formatOption = [](const ArgSpec& s) -> std::string {
      std::string option;

      if (s.shortFlag.has_value()) {
        option += "-" + std::string(1, s.shortFlag.value()) + ", ";
      }
      option += "--" + s.longFlag;

      if (s.argName && !s.argName->empty()) {
        option += " " + *s.argName;
      }

      return option;
    };

    std::string usage = "Usage: " + m_appName + " [OPTIONS]\n\n";

    if (!m_appDescription.empty()) {
      usage += m_appDescription + "\n\n";
    }

    usage += "Options:";
    for (const auto& s : m_supportedArgs | std::views::reverse) {
      usage += "\n  " + formatOption(s) + "\n      " + s.description;
    }
    return usage;
  }

  std::string versionString() const {
    return m_appName + " " + std::string(CTA_VERSION) + "\nCopyright (C) 2026 CERN\nLicense GPL-3.0-or-later";
  }

private:
  // Uses assertions as its aim is to prevent the developer from making a mistake
  void assertCorrectArgSpec(const ArgSpec& arg) const {
    assert(!arg.longFlag.empty());
    assert(!arg.description.empty());
    if (arg.shortFlag) {
      assert(*arg.shortFlag != '\0');
    }
    if (arg.argName) {
      assert(!arg.argName->empty());
    }
    //uniqueness checks
    for (const auto& existing : m_supportedArgs) {
      assert(existing.longFlag != arg.longFlag);
      if (arg.shortFlag && existing.shortFlag) {
        assert(*existing.shortFlag != *arg.shortFlag);
      }
    }
  }

  const std::string m_appName;
  std::string m_appDescription = "";
  std::vector<ArgSpec> m_supportedArgs;
  T m_options;
};

}  // namespace cta::runtime
