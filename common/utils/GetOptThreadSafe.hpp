/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "common/process/threading/Mutex.hpp"

#include <getopt.h>
#include <string>
#include <unistd.h>
#include <vector>

namespace cta::utils {

/**
 * A thread safe (but serialized) wrapper to getopt.
 */

class GetOpThreadSafe {
public:
  struct Request {
    std::vector<std::string> argv;
    std::string optstring;
    const struct ::option* longopts;
  };

  struct FoundOption {
    std::string option;
    std::string parameter;
  };

  struct Reply {
    std::vector<FoundOption> options;
    std::vector<std::string> remainder;
  };

  static Reply getOpt(const Request& request);

private:
  static threading::Mutex gMutex;
};

}  // namespace cta::utils
