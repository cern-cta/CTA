/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "CommonCliOptions.hpp"
#include "common/exception/UserError.hpp"
#include "common/utils/utils.hpp"
#include "version.h"

#include <algorithm>
#include <cassert>
#include <concepts>
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

template<class T>
concept HasPositionalArgs = requires(const T& options) {
  requires std::same_as<std::remove_cvref_t<decltype(options.positionalArgs)>, std::vector<std::string>>;
};

/**
 * @brief Specifies what a single argument/flag looks like.
 * Can be used to describe that the parser should parse e.g. --config or --config-strict
 * and how to handle the argument if found via the apply() function.
 *
 * @tparam T The type of the options object
 */
template<class T>
struct ArgSpec {
  std::optional<char> shortFlag;       // e.g. "c" (no dash), nullopt => no shorthand
  std::string longFlag;                // e.g. "help". Used to show --help
  std::optional<std::string> argName;  // e.g. "PATH". Used to show --config PATH
  std::string description;             // Description for the flag

  /**
   * @brief If the argument is found, this function will be called to apply the argument on the options object.
   * In other words, this is used to populate options for a single argument.
   *
   */
  std::function<void(T& options, std::optional<std::string_view>)> apply;
};

/**
 * @brief
 *
 * Example usage:
 *
 *
 *
 * @tparam T The struct to populate when parsing.
 */
template<class T>
class ArgParser {
public:
  explicit ArgParser(const std::string& appName)
    requires HasRequiredCliOptions<T>
      : m_appName(appName) {
    // The help message displays flags in reverse order of how they are registered
    // so that --help always shows last and custom added arguments show first.
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
                  "Path to the main configuration file. Defaults to " + defaultConfigPath() + " if not provided.");
    withStringArg(&T::logFilePath, "log-file", 'l', "PATH", "Write logs to PATH (defaults to stdout/stderr).");
  }

  /**
   * @brief Adds a description to the help output.
   *
   * @param description The description to add. May contain newlines (text is not automatically wrapped).
   * @return The ArgParser object. Allows for chaining of with* calls.
   */
  ArgParser& withDescription(std::string_view description) {
    m_appDescription = description;
    return *this;
  }

  /**
   * @brief Adds a string argument to the parser. A string argument is an input flag that itself takes an argument.
   * For example --config PATH.
   *
   * @param field The field to populate. For example, in --config PATH, this would be populated with the value of PATH.
   * @param longFlag The name of the flag. For example, in --config, this would be "config".
   * @param shortFlag The name of the short flag (optional). For example, in -c, --config, this would be "c".
   * @param argName The name of the flag argument. For example, in --config PATH, this would be "PATH".
   * @param description The description of the flag.
   * @return The ArgParser object. Allows for chaining of with* calls.
   */
  ArgParser& withStringArg(std::string T::* field,
                           const std::string& longFlag,
                           std::optional<char> shortFlag,
                           const std::string& argName,
                           const std::string& description) {
    ArgSpec<T> arg;
    arg.longFlag = longFlag;
    arg.shortFlag = shortFlag;
    arg.argName = argName;
    arg.description = description;
    arg.apply = [field, longFlag](T& options, std::optional<std::string_view> argVal) {
      if (!argVal.has_value() || argVal.value().empty()) {
        throw cta::exception::UserError("Missing required argument for option: --" + longFlag);
      }
      options.*field = std::string(argVal.value());
    };
    assertCorrectArgSpec(arg);
    m_supportedArgs.push_back(arg);
    return *this;
  }

  /**
   * @brief Adds a boolean argument to the parser. A boolean argument is an input flag that does not take any argument.
   * For example, --config-strict.
   *
   * @param field The field to populate. For all boolean flags, this value will be set to true if the flag is found.
   * As such, the initial value of this MUST be false, or you will get unexpected behaviour.
   * @param longFlag The name of the flag. For example, in --config-strict, this would be "config-strict".
   * @param shortFlag The name of the short flag (optional). For example, in -s, --config-strict this would be "s".
   * @param description The description of the flag.
   * @return The ArgParser object. Allows for chaining of with* calls.
   */
  ArgParser& withBoolArg(bool T::* field,
                         const std::string& longFlag,
                         std::optional<char> shortFlag,
                         const std::string& description) {
    ArgSpec<T> arg;
    arg.longFlag = longFlag;
    arg.shortFlag = shortFlag;
    arg.description = description;
    // We set it to true instead of toggling, since toggling gives unexpected behaviour when you provide a boolean flag twice.
    arg.apply = [field](T& options, std::optional<std::string_view>) { options.*field = true; };
    assertCorrectArgSpec(arg);
    m_supportedArgs.push_back(arg);
    return *this;
  }

  /**
   * @brief Adds support for positional arguments. Note that for now the implementation is really simplistic.
   * It simply populates an array of positional arguments. This means, it is not possible to add flags
   * specific to certain positional arguments.
   * In the future, if more sophisticated behaviour is desired, then this should be handled by subparsers.
   *
   * @return The ArgParser object. Allows for chaining of with* calls.
   */
  ArgParser& withPositionalArgs()
    requires HasPositionalArgs<T>
  {
    // In theory we could auto-detect whether T has positional argument and enable parsing based on this.
    // However, we want to make this explicit, so that it is less magic for the developers.
    // By enabling this flag, developers will get a clear compile error if their options struct doesn't support it.
    m_parsePositionalArgs = true;
    return *this;
  }

  T parse(const int argc, char** const argv) {
    if (m_hasParsed) {
      // In theory parsing again should be fine, but we want to prevent developers from doing this as it's not good practice.
      // For example, one may call parse() and then afterwards call one of the with* functions and the calling parse() again.
      // Not only would that be spaghetti, it would also give unexpected results for the first parse() result, as all options
      // must be registered before parse() is called.
      throw exception::Exception(
        "ArgParser has already parsed. Parsing again may give unexpected results and is not supported.");
    }
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
      const ArgSpec<T>& s = m_supportedArgs[i];

      // The value that will be returned by getopt_long
      int val = 0;
      if (s.shortFlag.has_value()) {
        // For short options, just return the corresponding ASCII value
        val = static_cast<unsigned char>(*s.shortFlag);

        shortopts.push_back(static_cast<char>(val));
        // Flag expects an argument
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
    longopts.emplace_back(nullptr, 0, nullptr, 0);

    // Reset getopt globals
    opterr = 0;  // Disable error output from getopt
    optind = 0;  // Reset getopt_long parser state to index 0

    // The actual options object we will be constructing
    T options;
    // Parse the arguments and populate the options object
    while (true) {
      int optionIndex = 0;
      const int tokenIndex = (optind == 0 ? 1 : optind);
      optopt = 0;
      const int c = ::getopt_long(argc, argv, shortopts.c_str(), longopts.data(), &optionIndex);
      if (c == -1) {
        // Done
        break;
      }

      // Handle user errors
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
        throw exception::Exception("Internal error: parsed option not mapped to a spec.");
      }

      std::optional<std::string_view> arg;
      if (optarg) {
        arg = std::string_view {optarg};
      }
      // Dispatch
      const ArgSpec<T>& spec = m_supportedArgs[it->second];
      spec.apply(options, arg);
    }

    std::vector<std::string> positionalArgs;
    for (int i = optind; i < argc; i++) {
      positionalArgs.emplace_back(argv[i]);
    }
    if (m_parsePositionalArgs) {
      if constexpr (HasPositionalArgs<T>) {
        options.positionalArgs = positionalArgs;
      }
    } else if (!positionalArgs.empty()) {
      throw exception::UserError("Found unexpected positional arguments: " + utils::joinCommaSeparated(positionalArgs));
    }

    if (options.configFilePath.empty()) {
      options.configFilePath = defaultConfigPath();
    }
    m_hasParsed = true;
    return options;
  }

  /**
   * @brief Constructs the usage string from the app name, the description (if present) and the registered arguments.
   *
   * @return std::string The usage string.
   */
  std::string usageString() const {
    auto formatOption = [](const ArgSpec<T>& s) {
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

  /**
   * @brief Prints a version string based on the app name.
   *
   * @return std::string The version string.
   */
  std::string versionString() const {
    return m_appName + " " + std::string(CTA_VERSION) + "\nCopyright (C) 2026 CERN\nLicense GPL-3.0-or-later";
  }

private:
  /**
   * @brief Runtime sanity check for developers to ensure they registered the correct commandflags.
   * Will check whether the new argument spec they want to register is valid.
   * For example, a long flag cannot be empty, description cannot be empty, flags must be unique etc.
   *
   * @param arg The argument spec.
   */
  void assertCorrectArgSpec(const ArgSpec<T>& arg) const {
    assert(arg.longFlag.size() > 1);
    assert(!arg.description.empty());
    if (arg.shortFlag.has_value()) {
      assert(arg.shortFlag.value() != '\0');
    }
    if (arg.argName.has_value()) {
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

  std::string defaultConfigPath() const { return "/etc/cta/" + m_appName + ".toml"; }

  const std::string m_appName;
  std::string m_appDescription = "";
  std::vector<ArgSpec<T>> m_supportedArgs;

  bool m_hasParsed = false;
  bool m_parsePositionalArgs = false;
};

}  // namespace cta::runtime
