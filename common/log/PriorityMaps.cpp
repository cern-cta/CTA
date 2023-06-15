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

#include <sys/syslog.h>

#include "PriorityMaps.hpp"
#include "Constants.hpp"
#include "common/exception/Exception.hpp"

namespace cta {
namespace log {

const std::map<int, std::string> PriorityMaps::c_priorityToTextMap = {
  {LOG_EMERG,  "EMERG" },
  {ALERT,      "ALERT" },
  {LOG_CRIT,   "CRIT"  },
  {ERR,        "ERROR" },
  {WARNING,    "WARN"  },
  {LOG_NOTICE, "NOTICE"},
  {INFO,       "INFO"  },
  {DEBUG,      "DEBUG" },
};

const std::map<std::string, int> PriorityMaps::c_configTextToPriorityMap = {
  {"LOG_EMERG",  LOG_EMERG },
  {"ALERT",      ALERT     },
  {"LOG_CRIT",   LOG_CRIT  },
  {"ERR",        ERR       },
  {"WARNING",    WARNING   },
  {"LOG_NOTICE", LOG_NOTICE},
  {"INFO",       INFO      },
  {"DEBUG",      DEBUG     },
};

std::string PriorityMaps::getPriorityText(const int priority) {
  std::string ret = "";
  try {
    ret = c_priorityToTextMap.at(priority);
  }
  catch (const std::exception& ex) {
    //if no corresponding priority, we return an empty string
  }
  return ret;
}

}  // namespace log
}  // namespace cta
