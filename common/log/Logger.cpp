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

#include <iomanip>

#include "common/log/Logger.hpp"
#include "common/log/LogLevel.hpp"
#include "common/utils/utils.hpp"
#include "common/exception/Exception.hpp"
#include "PriorityMaps.hpp"
#include <sys/time.h>
#include <sys/syscall.h>

namespace cta::log {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
Logger::Logger(std::string_view hostName, std::string_view programName, int logMask) :
  m_hostName(hostName), m_programName(programName), m_logMask(logMask),
  m_priorityToText(generatePriorityToTextMap()) { }

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
Logger::~Logger() = default;

//-----------------------------------------------------------------------------
// operator()
//-----------------------------------------------------------------------------
void Logger::operator() (int priority, std::string_view msg, const std::list<Param>& params) noexcept {
  // Get current time with high precision
  auto timeStamp = std::chrono::system_clock::now();
  const int pid = getpid();

  // Ignore messages whose priority is not of interest
  if(priority > m_logMask) return;

  // Try to find the textual representation of the syslog priority
  std::map<int, std::string>::const_iterator priorityTextPair =
    m_priorityToText.find(priority);

  // Ignore messages where log priority is not valid
  if(m_priorityToText.end() == priorityTextPair) return;

  const std::string header = createMsgHeader(timeStamp);
  const std::string body = createMsgBody(priorityTextPair->second, msg, params, pid);

  writeMsgToUnderlyingLoggingSystem(header, body);
}

//-----------------------------------------------------------------------------
// cleanString
//-----------------------------------------------------------------------------
std::string Logger::cleanString(std::string_view s, bool replaceUnderscores) {
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
    m_logMask = toLogLevel(logMask);
  } catch(exception::Exception& ex) {
    throw exception::Exception("Failed to set log mask: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// setLogFormat
//------------------------------------------------------------------------------
void Logger::setLogFormat(std::string_view logFormat) {
  if(logFormat == "default") {
    m_logFormat = LogFormat::DEFAULT;
  } else if(logFormat == "json") {
    m_logFormat = LogFormat::JSON;
  } else {
    throw exception::Exception("Log format value \"" + std::string(logFormat) + "\" is invalid.");
  }
}

//------------------------------------------------------------------------------
// setStaticParams
//------------------------------------------------------------------------------
void Logger::setStaticParams(const std::map<std::string, std::string> &staticParams) {
  // Build the static params string
  std::ostringstream os;
  if (!m_staticParamsStr.empty()) {
    throw exception::Exception("Static log params have already been set.");
  }
  switch(m_logFormat){
  case LogFormat::DEFAULT:
    for(const auto& [headerKey, headerValue] : staticParams){
      os << headerKey << "=\""  << headerValue << "\" ";
    }
    break;
  case LogFormat::JSON:
    for(const auto& [headerKey, headerValue] : staticParams){
      os << ",\"" << headerKey << "\":\"" << headerValue << "\"";
    }
    break;
  }
  m_staticParamsStr = os.str();
}

//-----------------------------------------------------------------------------
// createMsgHeader
//-----------------------------------------------------------------------------
std::string Logger::createMsgHeader(const TimestampT& timeStamp) const {
  using namespace std::chrono;
  std::ostringstream os;

  // Get second part
  auto ts_s_fraction = duration_cast<seconds>(timeStamp.time_since_epoch()).count();
  // Get nanoseconds part
  auto ts_ns_fraction = duration_cast<nanoseconds>(timeStamp.time_since_epoch()).count() % 1000000000;

  // Convert to time_t for compatibility with ctime functions
  time_t ts_t = system_clock::to_time_t(timeStamp);

  // Convert to localtime
  struct tm localTime;
  localtime_r(&ts_t, &localTime);

  switch(m_logFormat) {
    case LogFormat::DEFAULT:
      os << std::put_time(&localTime, "%b %e %T")
         << '.' << std::setfill('0') << std::setw(9) << ts_ns_fraction << ' '
         << m_hostName << " "
         << m_programName << ": ";
      break;
    case LogFormat::JSON:
      os << R"("epoch_time":)" << ts_s_fraction
         << '.' << std::setfill('0') << std::setw(9) << ts_ns_fraction << R"(,)"
         << R"("local_time":")" << std::put_time(&localTime, "%FT%T%z") << R"(",)"
         << R"("hostname":")" << m_hostName << R"(",)"
         << R"("program":")" << m_programName << R"(",)";
  }
  return os.str();
}

//-----------------------------------------------------------------------------
// createMsgBody
//-----------------------------------------------------------------------------
std::string Logger::createMsgBody(std::string_view logLevel, std::string_view msg,
  const std::list<Param>& params, int pid) const {
  std::ostringstream os;

  const int tid = syscall(__NR_gettid);

  // Append the log level, the thread id and the message text
  switch(m_logFormat) {
    case LogFormat::DEFAULT:
      os << R"(LVL=")" << logLevel << R"(" PID=")" << pid << R"(" TID=")" << tid << R"(" MSG=")" << msg << R"(" )";
      break;
    case LogFormat::JSON:
        os << R"("log_level":")" << logLevel << R"(",)"
           << R"("pid":)" << pid << R"(,)"
           << R"("tid":)" << tid << R"(,)"
           << R"("message":")" << msg << R"(")";
  }

  // Print static params
  os << m_staticParamsStr;

  // Process parameters
  for(auto& param : params) {
    // Write the name and value to the buffer
    switch(m_logFormat) {
      case LogFormat::DEFAULT:
        {
          // If parameter name is an empty string, set the value to "Undefined"
          const std::string name_str = param.getName() == "" ? "Undefined" : cleanString(param.getName(), true);
          const std::string value_str = cleanString(param.getValueStr(), false);
          os << name_str << "=\"" << value_str << "\" ";
        }
        break;
      case LogFormat::JSON:
        os << "," << param.getKeyValueJSON();
    }
  }

  return os.str();
}

} // namespace cta::log
