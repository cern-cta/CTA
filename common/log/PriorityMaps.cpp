/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2015-2021 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */


#include <sys/syslog.h>

#include "PriorityMaps.hpp"
#include "Constants.hpp"
#include "common/exception/Exception.hpp"

namespace cta {
namespace log {

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

}}

