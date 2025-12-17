/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "castor/log/log.hpp"

#include "castor/exception/Exception.hpp"

/**
 * The logger to be used by the CTA logging systsem
 */
static cta::log::Logger* s_logger = nullptr;

//------------------------------------------------------------------------------
// init
//------------------------------------------------------------------------------
void cta::log::init(cta::log::Logger* logger) {
  if (s_logger) {
    throw cta::exception::Exception("Failed to initialise logging system"
                                    ": Logging system already initialised");
  }

  s_logger = logger;
}

//------------------------------------------------------------------------------
// shutdown
//------------------------------------------------------------------------------
void cta::log::shutdown() {
  delete s_logger;
  s_logger = nullptr;
}

//------------------------------------------------------------------------------
// instance
//------------------------------------------------------------------------------
cta::log::Logger& cta::log::instance() {
  if (nullptr == s_logger) {
    throw cta::exception::Exception("Failed to get CTA logger: Logger does not exist");
  }
  return *s_logger;
}

//------------------------------------------------------------------------------
// prepareForFork
//------------------------------------------------------------------------------
void cta::log::prepareForFork() {
  try {
    instance().prepareForFork();
  } catch (cta::exception::Exception& ex) {
    throw cta::exception::Exception(std::string("Failed to prepare logger for call to fork(): ")
                                    + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// write
//------------------------------------------------------------------------------
void cta::log::write(const int priority, std::string_view msg, const std::list<cta::log::Param>& params) {
  if (s_logger) {
    (*s_logger)(priority, msg, params);
  }
}

//------------------------------------------------------------------------------
// write
//------------------------------------------------------------------------------
void cta::log::write(const int priority,
                     std::string_view msg,
                     std::string_view rawParams,
                     const struct timeval& timeStamp,
                     std::string_view progName,
                     const int pid) {
  const std::list<Param> params;
  if (s_logger) {
    (*s_logger)(priority, msg, params, rawParams, timeStamp, progName, pid);
  }
}

//------------------------------------------------------------------------------
// getProgramName
//------------------------------------------------------------------------------
std::string cta::log::getProgramName() {
  if (s_logger) {
    return (*s_logger).getProgramName();
  } else {
    return "";
  }
}

//------------------------------------------------------------------------------
// operator<<
//------------------------------------------------------------------------------
std::ostream& operator<<(std::ostream& out, const Cuuid_t& uuid) {
  char uuidstr[CUUID_STRING_LEN + 1];
  memset(uuidstr, '\0', CUUID_STRING_LEN + 1);
  Cuuid2string(uuidstr, CUUID_STRING_LEN + 1, &uuid);
  out << uuidstr;
  return out;
}
