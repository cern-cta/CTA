#pragma once

#include <sstream>
#include <string>
#include <sys/syscall.h>
#include <ctime>

class AgentId {
public:
  AgentId(std::string agentType): m_nextId(0) {
    std::stringstream aid;
    // Get time
    time_t now = time(0);
    struct tm localNow;
    localtime_r(&now, &localNow);
    // Get hostname
    char host[200];
    cta::exception::Errnum::throwOnMinusOne(gethostname(host, sizeof(host)),
      "In AgentId::AgentId:  failed to gethostname");
    // gettid is a safe system call (never fails)
    aid << agentType << "-" << host << "-" << syscall(SYS_gettid) << "-"
      << 1900 + localNow.tm_year
      << std::setfill('0') << std::setw(2) << 1 + localNow.tm_mon
      << localNow.tm_mday << "-"
      << localNow.tm_hour << ":"
      << localNow.tm_min << ":"
      << localNow.tm_sec;
    m_agentId = aid.str();
  }
  std::string name() {
    return m_agentId;
  }
  std::string nextId() {
    std::stringstream id;
    id << m_agentId << "-" << m_nextId++;
    return id.str();
  }
private:
  std::string m_agentId;
  uint64_t m_nextId;
};