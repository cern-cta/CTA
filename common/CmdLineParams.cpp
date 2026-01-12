/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "common/CmdLineParams.hpp"

#include "common/exception/Exception.hpp"

#include <getopt.h>
#include <iostream>
#include <string.h>

namespace cta::common {

CmdLineParams::CmdLineParams(int argc, char** argv, const std::string& procName) {
  //------------------------------------------------------------------------------
  // The help string
  //------------------------------------------------------------------------------
  std::string gHelpString = "Usage: " + procName
                            + " [options]\n"
                              "\n"
                              "where options can be:\n"
                              "\n";

  if (procName == "cta-taped") {
    gHelpString += "\t-f, --foreground            \tRemain in the Foreground\n";
  }

  gHelpString += "\t-s, --stdout                \tPrint logs to standard output. Requires --foreground. Logging to "
                 "stdout is the default, but this option is kept for compatibility reasons\n"
                 "\t-l --log-to-file <log-file> \tLogs to a given file (instead of default stdout)\n"
                 "\t-o, --log-format <format>   \tOutput format for log messages (default or json)\n"
                 "\t-c, --config <config-file>  \tConfiguration file\n"
                 "\t-h, --help                  \tPrint this help and exit\n";

  struct ::option longopts[] = {
    // { .name, .has_args, .flag, .val } (see getopt.h))
    {"foreground",  no_argument,       nullptr, 'f' },
    {"stdout",      no_argument,       nullptr, 's' },
    {"log-to-file", required_argument, nullptr, 'l' },
    {"log-format",  required_argument, nullptr, 'o' },
    {"config",      required_argument, nullptr, 'c' },
    {"help",        no_argument,       nullptr, 'h' },
    {nullptr,       0,                 nullptr, '\0'}
  };

  char c;
  // Reset getopt's global variables to make sure we start fresh
  optind = 0;
  // Prevent getopt from printing out errors on stdout
  opterr = 0;
  // We ask getopt to not reshuffle argv ('+')
  while ((c = getopt_long(argc, argv, "+fsc:l:h", longopts, nullptr)) != -1) {
    switch (c) {
      case 'f':
        foreground = true;
        break;
      case 's':
        logToStdout = true;
        break;
      case 'c':
        configFileLocation = optarg;
        break;
      case 'h':
        std::cout << gHelpString << std::endl;
        std::exit(EXIT_SUCCESS);
      case 'l':
        logFilePath = optarg;
        logToFile = true;
        break;
      case 'o':
        logFormat = optarg;
        break;
      default:
        break;
    }
  }

  const std::string errPrefix = "Failed to interpret the command line parameters: In CmdLineParams::CmdLineParams(): ";
  if (logToStdout && !foreground) {
    std::cerr << errPrefix << "cannot log to stdout without running in the foreground";
    std::exit(EXIT_FAILURE);
  }
  if (logToFile && logToStdout) {
    std::cerr << errPrefix << "cannot log to both stdout and file";
    std::exit(EXIT_FAILURE);
  }
}

std::vector<cta::log::Param> CmdLineParams::toLogParams() const {
  std::vector<cta::log::Param> ret;
  ret.emplace_back("foreground", foreground);
  ret.emplace_back("logToStdout", logToStdout);
  ret.emplace_back("logToFile", logToFile);
  ret.emplace_back("logFilePath", logFilePath);
  ret.emplace_back("logFormat", logFormat);
  ret.emplace_back("configFileLocation", configFileLocation);
  ret.emplace_back("helpRequested", helpRequested);
  return ret;
}

}  // namespace cta::common
