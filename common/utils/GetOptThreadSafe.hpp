/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#include "common/threading/Mutex.hpp"

#include <vector>
#include <string>
#include <unistd.h>
#include <getopt.h>

namespace cta { namespace utils {

/**
 * A thread safe (but serialized) wrapper to getopt.
 */

class GetOpThreadSafe {
public:
  struct Request {
    std::vector<std::string> argv;
    std::string optstring;
    const struct ::option *longopts;
  };
  struct FoundOption {
    std::string option;
    std::string parameter;
  };
  struct Reply {
    std::vector<FoundOption> options;
    std::vector<std::string> remainder;
  };
  static Reply getOpt (const Request & request);
private:
  static threading::Mutex gMutex;
};

}} // namespace cta::utils
