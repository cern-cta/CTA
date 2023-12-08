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

#include "common/log/Logger.hpp"
#include "common/log/LogLevel.hpp"
#include "common/utils/utils.hpp"
#include "common/exception/Exception.hpp"
#include "PriorityMaps.hpp"
#include <sys/time.h>
#include <sys/syscall.h>


#include <ctime>
#include <iostream>
#include <chrono>
#include <iomanip> 

namespace cta::log {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
Logger::Logger(std::string_view hostName, std::string_view programName, const int logMask):
  m_hostName(hostName), m_programName(programName), m_logMask(logMask),
  m_priorityToText(generatePriorityToTextMap()) {}

//------------------------------------------------------------------------------
// getProgramName
//------------------------------------------------------------------------------
const std::string &Logger::getProgramName() const {
  return m_programName;
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
Logger::~Logger() {
}

//-----------------------------------------------------------------------------
// operator()
//-----------------------------------------------------------------------------
void Logger::operator() (int priority, std::string_view msg, const std::list<Param>& params) {
  //const std::string Time1= "Time: ";
  //const std::string Utc1= "UTC: ";
  
  //char epoch_time[]="1111111111111111111";//"1702006478.962950144";
  //const float epoch_time;
  time_t now;
  time(&now);
  char local_time[sizeof "2011-10-08T07:07:09Z"];
  //strftime(local_time, sizeof local_time, "%FT%TZ", gmtime(&now));
  strftime(local_time, sizeof local_time, "%FT%TZ", localtime(&now));

  const float epoch_time_def = std::chrono::duration_cast<std::chrono::nanoseconds>
        (std::chrono::system_clock::now().time_since_epoch()).count();
  const float epoch_time= (epoch_time_def/1000000000.0);
  //std::cout<<epoch_time_def << std::endl;
  //const std::string input = epoch_time;
  //const std::string epoch_time_formated = format("{0:.2s}.{0:2s.}",input);
  //sprintf(epoch_time,"%f.9f",epoch_time_def);
  //std::cout<<epoch_time << std::endl;
  const std::string rawParams;
  //struct timeval timeStamp;
  //gettimeofday(&timeStamp, nullptr);
  const int pid = getpid();

  // Ignore messages whose priority is not of interest
  if(priority > m_logMask) {
    return;
  }

  // Try to find the textual representation of the syslog priority
  std::map<int, std::string>::const_iterator priorityTextPair =
    m_priorityToText.find(priority);

  // Do nothing if the log priority is not valid
  if(m_priorityToText.end() == priorityTextPair) {
    return;
  }

  // Safe to get a reference to the textual representation of the priority
  const std::string &priorityText = priorityTextPair->second;
  const std::string header = createMsgHeader(epoch_time, local_time, m_hostName, m_programName, pid);
  const std::string body = createMsgBody(epoch_time, local_time, priority, priorityText, msg, params, rawParams, pid);

  writeMsgToUnderlyingLoggingSystem(header, body);
}

//-----------------------------------------------------------------------------
// cleanString
//-----------------------------------------------------------------------------
std::string Logger::cleanString(const std::string &s,
  const bool replaceUnderscores) {
  // Trim both left and right white-space
  std::string result = utils::trimString(s);

  for (std::string::iterator it = result.begin(); it != result.end(); ++it) {

    // Replace double quote with single quote
    if ('"' == *it) {
      *it = '\'';
    }

    // Replace newline and tab with a space
    if ('\t' == *it || '\n' == *it) {
      *it = ' ';
    }

    // If requested, replace spaces with underscores
    if(replaceUnderscores && ' ' == *it) {
      *it = '_';
    }
  }

  return result;
}

//------------------------------------------------------------------------------
// generatePriorityToTextMap
//------------------------------------------------------------------------------
std::map<int, std::string>
  Logger::generatePriorityToTextMap() {
  return PriorityMaps::c_priorityToTextMap;
}

//------------------------------------------------------------------------------
// generateConfigTextToPriorityMap
//------------------------------------------------------------------------------
std::map<std::string, int>
  Logger::generateConfigTextToPriorityMap() {
  return PriorityMaps::c_configTextToPriorityMap;
}

//------------------------------------------------------------------------------
// setLogMask
//------------------------------------------------------------------------------
void Logger::setLogMask(std::string_view logMask) {
  try {
    setLogMask(toLogLevel(logMask));
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string("Failed to set log mask: ") + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// setLogMask
//------------------------------------------------------------------------------
void Logger::setLogMask(const int logMask) {
  m_logMask = logMask;
}

//-----------------------------------------------------------------------------
// createMsgHeader
//-----------------------------------------------------------------------------
std::string Logger::createMsgHeader(
  //const struct timeval& timeStamp,
  //const std::string Time1,
  //std::time_t epoch_time,
  //char epoch_time[],
  const float epoch_time,
  //const std::string Utc1,
  char* local_time,
  std::string_view hostName,
  std::string_view programName,
  const int pid) {
  std::ostringstream os;
  //char buf[80];
  //int bufLen = sizeof(buf);
  //int len = 0;

 // struct tm localTime;
  //localtime_r(&(timeStamp.tv_sec), &localTime);
  //len += strftime(buf, bufLen, "%b %e %T", &localTime);
  //len += snprintf(buf + len, bufLen - len, ".%06lu ", static_cast<unsigned long>(timeStamp.tv_usec));
  //buf[sizeof(buf) - 1] = '\0';
  os<<"'time:' "<< std::setprecision(20)<< epoch_time <<" local_time: "<<local_time<< " hostName: "<<hostName << " programName: " << programName << ": ";
  return os.str();
}

//-----------------------------------------------------------------------------
// createMsgBody
//-----------------------------------------------------------------------------
std::string Logger::createMsgBody(
  //std::time_t epoch_time,
  //char epoch_time[],
  const float epoch_time,
  char* local_time,
  const int priority,
  std::string_view priorityText, std::string_view msg,
  const std::list<Param> &params, std::string_view rawParams, int pid) {
  std::ostringstream os;

  const int tid = syscall(__NR_gettid);

  // Append the log level, the thread id and the message text
 // os << "Time=\""<< epoch_time << "\" UTC=\""  <<local_time <<"\" LVL=\"" << priorityText << "\" PID=\"" << pid << "\" TID=\"" << tid << "\" MSG=\"" <<
  //  msg << "\" ";

  os << " LVL=\"" << priorityText << "\" PID=\"" << pid << "\" TID=\"" << tid << "\" MSG=\"" <<
    msg << "\" ";

  // Process parameters
  for(auto itor = params.cbegin(); itor != params.cend(); itor++) {
    const Param &param = *itor;

    // Check the parameter name, if it's an empty string set the value to
    // "Undefined".
    const std::string name = param.getName() == "" ? "Undefined" :
      cleanString(param.getName(), true);

    // Process the parameter value
    const std::string value = cleanString(param.getValue(), false);

    // Write the name and value to the buffer
    os << name << "=\"" << value << "\" ";
  }

  // Append raw parameters
  os << rawParams;

  return os.str();
}

} // namespace cta::log
