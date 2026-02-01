/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "CommonCliOptions.hpp"
#include "common/exception/UserError.hpp"

#include <algorithm>
#include <cassert>
#include <cstdlib>
#include <functional>
#include <getopt.h>
#include <iostream>
#include <optional>
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
// TODO example with custom args
// no support for positional arguments yet, but should not be too complicated to add
template<class T>
class ArgParser {
public:
  ArgParser(const std::string& appName)
    requires HasRequiredCliOptions<T>
      : m_appName(appName) {
    withBoolArg(&T::showHelp, "help", 'h', "Show help");
    withStringArg(&T::logFilePath, "log-file", 'l', "path", "Path to log file");
    withStringArg(&T::configFilePath, "config", 'c', "config-file", "Configuration file");
    withBoolArg(&T::configStrict, "config-strict", std::nullopt, "Enable strict config checking");
    withBoolArg(&T::configCheck,
                "config-check",
                std::nullopt,
                "Instead of running the application, performs validation of the config file");
  }

  void withStringArg(std::string T::* field,
                     const std::string& longFlag,
                     std::optional<char> shortFlag,
                     const std::string& argName,
                     const std::string& description) {
    // assert the long flag and short flags are unique
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
    // assert the long flag and short flags are unique
    ArgSpec arg;
    arg.longFlag = longFlag;
    arg.shortFlag = shortFlag;
    arg.description = description;
    // Assert bool is false
    arg.apply = [this, field](std::optional<std::string_view>) { m_options.*field = true; };
    assertCorrectArgSpec(arg);
    m_supportedArgs.push_back(arg);
  }

  T parse(const int argc, char** const argv) {
    // Build short option string and long option array
    std::string shortopts;
    shortopts.reserve(m_supportedArgs.size() * 2);
    std::vector<option> longopts;
    longopts.reserve(m_supportedArgs.size() + 1);

    // Map getopt_long return value -> spec index
    std::unordered_map<int, std::size_t> valToIndex;
    valToIndex.reserve(m_supportedArgs.size());

    int nextLongOnlyVal = 256;  // avoid collision with ASCII short flags

    for (std::size_t i = 0; i < m_supportedArgs.size(); ++i) {
      const ArgSpec& s = m_supportedArgs[i];

      int val = 0;
      if (s.shortFlag) {
        val = static_cast<unsigned char>(*s.shortFlag);

        shortopts.push_back(static_cast<char>(val));
        if (s.argName.has_value()) {
          shortopts.push_back(':');
        }
      } else {
        val = nextLongOnlyVal++;
      }

      option o {};
      o.name = s.longFlag.c_str();
      o.has_arg = s.argName.has_value();
      o.flag = nullptr;
      o.val = val;

      longopts.push_back(o);
      valToIndex.emplace(val, i);
    }

    longopts.push_back(option {nullptr, 0, nullptr, 0});

    // Reset getopt globals
    opterr = 0;  // suppress getopt's own error output
    optind = 1;

    while (true) {
      int optionIndex = 0;
      const int c = ::getopt_long(argc, argv, shortopts.c_str(), longopts.data(), &optionIndex);
      if (c == -1) {
        break;
      }

      if (c == '?' || c == ':') {
        // Unknown option or missing required argument.
        // optopt is set for short options; for long options, it's less helpful.

        // TODO: better error message?
        throw cta::exception::UserError("Invalid option. Use --help to see valid options.");
      }

      const auto it = valToIndex.find(c);
      if (it == valToIndex.end()) {
        throw cta::exception::Exception("Internal error: parsed option not mapped to a spec.");
      }

      const ArgSpec& spec = m_supportedArgs[it->second];

      std::optional<std::string_view> arg;
      if (optarg) {
        arg = std::string_view {optarg};
      }

      if (spec.argName.has_value() && !arg) {
        throw cta::exception::UserError("Missing argument for --" + spec.longFlag);
      }

      // Dispatch
      spec.apply(arg);
    }

    // If we ever want to add positional arguments, do that here:
    // for (int i = optind; i < argc; ++i) { ... }

    if (m_options.showHelp) {
      std::cout << usageString() << std::endl;
      std::exit(EXIT_SUCCESS);
    }
    return m_options;
  }

private:
  std::string usageString() const {
    std::string usage = "Usage: " + m_appName
                        + " [options]\n\n"
                          "Options:\n\n";

    auto formatLeft = [](const ArgSpec& s) -> std::string {
      std::string left;

      if (s.shortFlag) {
        left += "-";
        left += *s.shortFlag;
        left += ", ";
      }

      left += "--";
      left += s.longFlag;

      if (s.argName && !s.argName->empty()) {
        left += " <";
        left += *s.argName;
        left += ">";
      }

      return left;
    };

    std::size_t leftWidth = 0;
    std::vector<std::pair<std::string, std::string>> rows;
    rows.reserve(m_supportedArgs.size());

    for (const auto& s : m_supportedArgs) {
      std::string left = formatLeft(s);
      leftWidth = std::max(leftWidth, left.size());
      rows.emplace_back(std::move(left), s.description);
    }

    for (const auto& [left, desc] : rows) {
      usage += "  ";
      usage += left;

      if (!desc.empty()) {
        if (left.size() < leftWidth) {
          usage.append(leftWidth - left.size(), ' ');
        }
        usage += "  ";
        usage += desc;
      }

      usage += "\n";
    }

    return usage;
  }

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
  std::vector<ArgSpec> m_supportedArgs;
  T m_options;
};

}  // namespace cta::runtime
