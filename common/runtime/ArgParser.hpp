/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <algorithm>
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

struct ArgumentSpec {
  std::optional<char> shortFlag;       // e.g. "c" (no dash), nullopt => no shorthand
  std::string longFlag;                // e.g. "help". Used to show --help
  std::optional<std::string> argName;  // e.g. "config-file". Used to show --config <config-file>
  std::string description;             // Description for the flag

  std::function<void(std::optional<std::string_view>)> apply;
};

template<class T>
concept HasRequiredCliOptions = requires(T& opts) {
  { opts.showHelp } -> std::convertible_to<bool>;
  { opts.configStrict } -> std::convertible_to<bool>;
  requires std::convertible_to<decltype(opts.configFilePath), const std::string&>;
  requires std::convertible_to<decltype(opts.logFilePath), const std::string&>;
};

// TODO: better exception handling
// Basic usage
//  runtime::CommonCliOptions opts;
//  runtime::ArgParser argParser(appName, opts);
//  argParser.parser(argc, argv);
// TODO example with custom args
template<class T>
class ArgParser {
public:
  ArgParser(const std::string& appName, T& options)
    requires HasRequiredCliOptions<T>
      : m_appName(appName),
        m_options(options) {
    withBoolArgument(m_options.showHelp,
                     "help",
                     'h',
                     "Print this help and exit",
                     [this](std::optional<std::string_view>) {
                       std::cout << usageString() << std::endl;
                       std::exit(EXIT_SUCCESS);
                     });
    withBoolArgument(m_options.configStrict, "config-strict", std::nullopt, "Enable strict config checking");
    withStringArgument(m_options.configFilePath, "config", 'c', "config-file", "Configuration file");
    withStringArgument(m_options.logFilePath, "log-file", 'l', "path", "Path to log file");
  }

  void withStringArgument(std::string T::* field,
                          const std::string& longFlag,
                          std::optional<char> shortFlag,
                          const std::string& argName,
                          const std::string& description) {
    // assert the long flag and short flags are unique
    ArgumentSpec arg;
    arg.longFlag = longFlag;
    arg.shortFlag = shortFlag;
    arg.argName = argName;
    arg.description = description;
    arg.apply = [this, field](std::optional<std::string_view> arg) {
      if (!arg.has_value() || arg.value().empty()) {
        throw std::runtime_error("missing required argument");
      }
      m_options.*field = std::string(arg.value());
    };
    m_arguments.push_back(arg);
  }

  void withBoolArgument(bool TOptions::* field,
                        const std::string& longFlag,
                        std::optional<char> shortFlag,
                        const std::string& description) {
    // Assert bool is false
    withBoolArgument(flag, longFlag, shortFlag, description, [this, field](std::optional<std::string_view>) {
      m_options.*field = true;
    });
  }

  void withBoolArgument(const std::string& longFlag,
                        std::optional<char> shortFlag,
                        const std::string& description,
                        std::function<void(std::optional<std::string_view>)> apply) {
    ArgumentSpec arg;
    arg.longFlag = longFlag;
    arg.shortFlag = shortFlag;
    arg.argName = {};
    arg.description = description;
    arg.apply = apply;
    m_arguments.push_back(arg);
  }

  void parseOptions(const int argc, char** const argv) {
    // Build short option string and long option array
    std::string shortopts;
    shortopts.reserve(m_arguments.size() * 2);

    std::vector<option> longopts;
    longopts.reserve(m_arguments.size() + 1);

    // Map getopt_long return value -> spec index
    std::unordered_map<int, std::size_t> valToIndex;
    valToIndex.reserve(m_arguments.size());

    int nextLongOnlyVal = 256;  // avoid collision with ASCII short flags

    for (std::size_t i = 0; i < m_arguments.size(); ++i) {
      const ArgumentSpec& s = m_arguments[i];

      int val = 0;
      if (s.shortFlag) {
        val = static_cast<unsigned char>(*s.shortFlag);

        shortopts.push_back(static_cast<char>(val));
        if (s.argName.has_value()) {
          shortopts.push_back(':');
        }
        // if you add Optional later: shortopts.append("::");
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
        throw std::runtime_error("Invalid option. Use --help to see valid options.");
      }

      const auto it = valToIndex.find(c);
      if (it == valToIndex.end()) {
        throw std::runtime_error("Internal error: parsed option not mapped to a spec");
      }

      const ArgumentSpec& spec = m_arguments[it->second];

      std::optional<std::string_view> arg;
      if (optarg) {
        arg = std::string_view {optarg};
      }

      if (spec.argKind == ArgKind::Required && !arg) {
        throw std::runtime_error("Missing argument for --" + spec.longFlag);
      }

      // Dispatch
      spec.apply(arg);
    }

    // Optional: treat remaining args as positional
    // for (int i = optind; i < argc; ++i) { ... }
  }

private:
  std::string usageString() const {
    std::string usage = "Usage: " + m_appName
                        + " [options]\n\n"
                          "Options:\n\n";

    auto formatLeft = [](const ArgumentSpec& s) -> std::string {
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
    rows.reserve(m_arguments.size());

    for (const auto& s : m_arguments) {
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

  const std::string m_appName;
  std::vector<ArgumentSpec> m_arguments;
  T& m_options;
}

}  // namespace cta::runtime
