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

#include "common/log/JSONLogger.hpp"

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
  
  //const float epoch_time = std::chrono::duration_cast<std::chrono::seconds>
     //   (std::chrono::system_clock::now().time_since_epoch()).count();
   // const float epoch_time_nano = std::chrono::duration_cast<std::chrono::nanoseconds>
      //  (std::chrono::system_clock::now().time_since_epoch()).count();
  //const float nano = epoch_time_nano-epoch_time;
    uint64_t nanoTime = std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    uint64_t nanoseconds =     nanoTime/1000000000;
    uint64_t seconds = nanoTime%1000000000;
  //const float epoch_time= (epoch_time_def/1000000000.0);
  //std::cout<<epoch_time_def << std::endl;
  //const std::string input = epoch_time;
  //const std::string epoch_time_formated = format("{0:.2s}.{0:2s.}",input);
  //sprintf(epoch_time,"%f.9f",epoch_time_def);
  //std::cout<<nano << std::endl;
  
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
  std::string strJson = "{\"mykey\" : \"myvalue\"}";
  //const std::string s = jsonSetValue(rawParams);
  //const std::string s1 = jsonGetValue(rawParams);
  //cta::utils::json::object::JSONCObject JsonClass;
 
   
  const std::string header = createMsgHeader(nanoTime, nanoseconds, seconds, local_time, m_hostName, m_programName, pid);
  const std::string body = createMsgBody(local_time, priority, priorityText, msg, params, rawParams, pid);
  const std::string jsonOut = createMsgJsonOut(nanoTime, nanoseconds, seconds, local_time, m_hostName, m_programName, pid, priority, priorityText, msg, params, rawParams); 
 
  writeMsgToUnderlyingLoggingSystem(header, body);
  writeMsgToUnderlyingLoggingSystemJson(jsonOut);
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
  //const float epoch_time,
  //const float epoch_time_nano, 
  //const float nano,
  uint64_t nanoTime,
  uint64_t seconds,
  uint64_t nanoseconds,
  //const std::string Utc1,
  char* local_time,
  std::string_view hostName,
  std::string_view programName,
  const int pid) {
  std::ostringstream os;
  //cta::utils::json::object::JSONCObject jsonObject;
  //char buf[80];
  //int bufLen = sizeof(buf);
  //int len = 0;

 // struct tm localTime;
  //localtime_r(&(timeStamp.tv_sec), &localTime);
  //len += strftime(buf, bufLen, "%b %e %T", &localTime);
  //len += snprintf(buf + len, bufLen - len, ".%06lu ", static_cast<unsigned long>(timeStamp.tv_usec));
  //buf[sizeof(buf) - 1] = '\0';
  //uint64_t jsontime= 
  //JSONLogger->addToObject("Time", nanoTime);
  /*
  os<<nanoTime;
  m_jsonLog.addToObject("Time", os.str());
  os.str(std::string());
  os<< seconds << "." << std::setw(9) << std::setfill('0') << nanoseconds;
  m_jsonLog.addToObject("EpochTime", os.str());
  os.str(std::string());
  os<<local_time;
  m_jsonLog.addToObject("local_time", os.str());
  os.str(std::string());
  m_jsonLog.addToObject("hostName", hostName);
  os.str(std::string());
  //jsonObject.jsonSetValue("hostName", hostName);
  os<< m_jsonLog.getJSON();
  */
  os<<"{\"time\":\""<<std::setprecision(20)<<nanoTime <<"\", \"EpochTime\":\""<< seconds << "." << std::setw(9) << std::setfill('0') << nanoseconds <<"\", \"local_time\":\""<<local_time<< "\", \"hostName\":\""<<hostName << "\", \"programName\":\" " << programName <<"\", ";
  return os.str();
}

//-----------------------------------------------------------------------------
// createMsgBody
//-----------------------------------------------------------------------------
std::string Logger::createMsgBody(
  //const float epoch_time,
  char* local_time,
  const int priority,
  std::string_view priorityText, std::string_view msg,
  const std::list<Param> &params, std::string_view rawParams, int pid) {
  std::ostringstream os;

  const int tid = syscall(__NR_gettid);

  // Append the log level, the thread id and the message text
 // os << "Time=\""<< epoch_time << "\" UTC=\""  <<local_time <<"\" LVL=\"" << priorityText << "\" PID=\"" << pid << "\" TID=\"" << tid << "\" MSG=\"" <<
  //  msg << "\" ";

  os << " \"LVL\":\"" << priorityText << "\", \"PID\":\"" << pid << "\", \"TID\":\"" << tid << "\", \"MSG\":\"" <<
    msg << "\"";

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
    os << ", \""<< name<<"\"" << ":\"" << value << "\"";
  }

  // Append raw parameters
  os << rawParams <<"}";

  return os.str();
}
std::string Logger::createMsgJsonOut(
    uint64_t nanoTime,
    uint64_t nanoseconds,
    uint64_t seconds,
    char* local_time,
    const std::string &hostName,
    const std::string &programName,
    const int pid,
    const int priority,
    const std::string &priorityText,
    const std::string &msg,
    const std::list<Param> &params,
    const std::string &rawParams) {
  std::ostringstream os;

  const int tid = syscall(__NR_gettid);

  os<<nanoTime;
  m_jsonLog.addToObject("Time", os.str());
  os.str(std::string());
  os<< seconds << "." << std::setw(9) << std::setfill('0') << nanoseconds;
  m_jsonLog.addToObject("EpochTime", os.str());
  os.str(std::string());
  os<<local_time;
  m_jsonLog.addToObject("local_time", os.str());
  os.str(std::string());
  m_jsonLog.addToObject("hostName", hostName);
  os.str(std::string());
  m_jsonLog.addToObject("programName", programName);
  m_jsonLog.addToObject("LVL", priorityText);
  os<< pid;
  m_jsonLog.addToObject("PID", os.str());
  os.str(std::string());
  os<< tid;
  m_jsonLog.addToObject("TID", os.str());
  os.str(std::string());
  m_jsonLog.addToObject("MSG", msg);
  // Process parameters
    for(auto itor = params.cbegin(); itor != params.cend(); itor++) {
    const Param &param = *itor;
    const std::string name = param.getName() == "" ? "Undefined" :
      cleanString(param.getName(), true);
    const std::string value = cleanString(param.getValue(), false);
   m_jsonLog.addToObject(name, value);
}
  m_jsonLog.addToObject("", rawParams);
  os<< m_jsonLog.getJSON();
  return os.str();
}

} // namespace cta::log
