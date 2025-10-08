/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */


#include <sys/syslog.h>

#include "PriorityMaps.hpp"
#include "Constants.hpp"
#include "common/exception/Exception.hpp"

namespace cta::log {

  const std::map<int,std::string> PriorityMaps::c_priorityToTextMap = {
    {LOG_EMERG,"EMERG"},
    {ALERT,"ALERT"},
    {LOG_CRIT,"CRIT"},
    {ERR,"ERROR"},
    {WARNING,"WARN"},
    {LOG_NOTICE,"NOTICE"},
    {INFO,"INFO"},
    {DEBUG,"DEBUG"},
  };

  const std::map<std::string,int> PriorityMaps::c_configTextToPriorityMap = {
    {"LOG_EMERG",LOG_EMERG},
    {"ALERT",ALERT},
    {"LOG_CRIT",LOG_CRIT},
    {"ERR",ERR},
    {"WARNING",WARNING},
    {"LOG_NOTICE",LOG_NOTICE},
    {"INFO",INFO},
    {"DEBUG",DEBUG},
  };

  std::string PriorityMaps::getPriorityText(const int priority){
    std::string ret = "";
    try {
      ret = c_priorityToTextMap.at(priority);
    } catch(const std::exception & ex){
      //if no corresponding priority, we return an empty string
    }
    return ret;
  }

} // namespace cta::log

