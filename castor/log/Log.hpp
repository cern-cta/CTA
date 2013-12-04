/******************************************************************************
 *                      Log.hpp
 *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2003  CERN
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 *
 *
 * Interface to the CASTOR logging system
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef CASTOR_LOG_LOG_HPP
#define CASTOR_LOG_LOG_HPP 1

// Include Files
#include "castor/log/Param.hpp"

#include <syslog.h>
#include <sys/time.h>

/* Priority levels */
#define LOG_LVL_EMERGENCY     LOG_EMERG   /* System is unusable */
#define LOG_LVL_ALERT         LOG_ALERT   /* Action must be taken immediately */
#define LOG_LVL_CRIT          LOG_CRIT    /* Critical conditions */
#define LOG_LVL_ERROR         LOG_ERR     /* Error conditions */
#define LOG_LVL_WARNING       LOG_WARNING /* Warning conditions */
#define LOG_LVL_USER_ERROR    LOG_NOTICE  /* Normal but significant condition */
#define LOG_LVL_AUTH          LOG_NOTICE
#define LOG_LVL_SECURITY      LOG_NOTICE
#define LOG_LVL_SYSTEM        LOG_INFO    /* Informational */
#define LOG_LVL_DEBUG         LOG_DEBUG   /* Debug-level messages */

namespace castor {
namespace log {

/**
 * Initialize the CASTOR logging interface.
 *
 * @param progname The name of the program to be prepended to every log
 *                 message.
 *
 * @return On success zero is returned, On error, -1 is returned, and errno is
 *         set appropriately.
 *
 *         Possible errors include:
 *          - EPERM  The interface is already initialized!
 *          - EINVAL Invalid argument (refers to errors in parsing the LogMask
 *                   configuration option in castor.conf for the daemon)
 *
 * @see openlog(), setlogmask()
 */
int initLog (const char *progname);

/**
 * Writes a message into the CASTOR logging system. Note that no exception
 * will ever be thrown in case of failure. Failures will actually be
 * silently ignored in order to not impact the processing.
 *
 * Note that this version of writeMsg() allows the caller to specify the
 * time stamp of the log message.
 *
 * @param priority the priority of the message.
 * @param msg the message.
 * @param numParams the number of parameters in the message.
 * @param params the parameters of the message, given as an array.
 * @param timeStamp the time stamp of the log message.
 */
void writeMsg(const int priority,
              const std::string &msg,
              const int numParams,
              const Param params[],
              const struct timeval &timeStamp) throw();

/**
 * A template function that wraps writeMsg in order to get the compiler
 * to automatically determine the size of the params parameter, therefore
 *
 * Note that this version of writeMsg() allows the caller to specify the
 * time stamp of the log message.
 *
 * @param msg the message.
 * @param params the parameters of the message, given as an array.
 * @param timeStamp the time stamp of the log message.
 */
template<int numParams>
void writeMsg(const int priority,
              const std::string &msg,
              castor::log::Param(&params)[numParams],
              const struct timeval &timeStamp) throw() {
  writeMsg(priority, msg, numParams, params, timeStamp);
}

/**
 * Writes a message into the CASTOR logging system. Note that no exception
 * will ever be thrown in case of failure. Failures will actually be
 * silently ignored in order to not impact the processing.
 *
 * Note that this version of writeMsg() implicitly uses the current time as
 * the time stamp of the message.
 *
 * @param priority the priority of the message.
 * @param msg the message.
 * @param numParams the number of parameters in the message.
 * @param params the parameters of the message, given as an array.
 */
void writeMsg(const int priority,
              const std::string &msg,
              const int numParams,
              const castor::log::Param params[]) throw();

/**
 * A template function that wraps writeMsg in order to get the compiler
 * to automatically determine the size of the params parameter, therefore
 * removing the need for the devloper to provide it explicity.
 *
 * Note that this version of writeMsg() implicitly uses the current time as
 * the time stamp of the message.
 *
 * @param priority the priority of the message.
 * @param msg the message.
 * @param params the parameters of the message, given as an array.
 */
template<int numParams>
void writeMsg(const int priority,
              const std::string &msg,
              castor::log::Param(&params)[numParams]) throw() {
  writeMsg(priority, msg, numParams, params);
}

} // namespace log
} // namespace castor

#endif // CASTOR_LOG_LOG_HPP
