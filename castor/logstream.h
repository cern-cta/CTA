/******************************************************************************
 *                      logstream.h
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
 * @(#)$RCSfile: logstream.h,v $ $Revision: 1.13 $ $Release$ $Date: 2005/01/24 14:48:23 $ $Author: sponcec3 $
 *
 * A generic logstream for castor, handling IP addresses
 * and timestamps
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef CASTOR_LOGSTREAM_H
#define CASTOR_LOGSTREAM_H 1

// Include Files
#include <time.h>
#include <string>
#include <ostream>
#include <iomanip>
#include "Cuuid.h"
#include "osdep.h"
#include "castor/ObjectSet.hpp"
#include "castor/logbuf.h"

#define OPERATOR(T)                             \
  logstream& operator<< (T var) {               \
    *((std::ostream*)this) << var;              \
    return *this;                               \
  }

#define OPERATORINT(T)                          \
  logstream& operator<< (T var) {               \
    if (m_isIP) {                               \
      m_isIP = false;                           \
      printIP(var);                             \
    } else if (m_isTimeStamp) {                 \
      m_isTimeStamp = false;                    \
      printTimeStamp(var);                      \
    } else {                                    \
      this->std::ostream::operator<<(var);      \
    }                                           \
    return *this;                               \
  }

#define MANIPULATOR(T) castor::logstream& T(castor::logstream& s);

namespace castor {

  class logstream : virtual public std::ostream {

  public:

    /**
     * constructor
     */
    explicit logstream(std::string name) :
      std::ostream(0),
      m_logbuf(0),
      m_isIP(false),
      m_isTimeStamp(false) {
    }

      /**
       * destructor
       */
      ~logstream() {
        if (0 != m_logbuf) {
          m_logbuf->sync();
          delete m_logbuf;
        }
      }

      /**
       *  @brief  Close the file.
       *
       *  Calls @c std::basic_filebuf::close().  If that function
       *  fails, @c failbit is set in the stream's error state.
       */
      void close() {
        if (0 != m_logbuf) {
          m_logbuf->sync();
        }
      }

      /**
       * sets a log buffer for this stream
       */
      void setBuffer(castor::logbuf *buf) {
        if (0 != m_logbuf) {
          m_logbuf->sync();
          delete m_logbuf;
        }
        m_logbuf = buf;
        this->init(buf);
      }

  public:
      /**
       * Set of overwritten operators of this stream
       */
      OPERATOR(char);
      OPERATOR(unsigned char);
      OPERATOR(signed char);
      OPERATOR(short);
      OPERATOR(unsigned short);
      OPERATOR(const char*);
      OPERATOR(const void*);
      OPERATOR(std::string);
      OPERATOR(bool);
      OPERATOR(float);
      OPERATOR(double);
      OPERATOR(u_signed64);
      OPERATORINT(int);
      OPERATORINT(unsigned int);
      OPERATORINT(long);
      OPERATORINT(unsigned long);

      /**
       * This operator deals with UUIDs
       */
      logstream& operator<< (Cuuid_t var) {
        m_logbuf->setUuid(var);
        return *this;
      }

      /**
       * This operator deals with manipulators specific to
       * castor::logstream
       */
      logstream& operator<< (logstream& (&f)(logstream&)) {
        return f(*this);
      }

      /**
       * This operator deals with manipulators specific to
       * std::ostream
       */
      logstream& operator<< (std::ostream& (&f)(std::ostream&)) {
        if (0 != m_logbuf) {
          if (&f == (std::ostream& (&)(std::ostream&))std::endl) {
            *((std::ostream*)this) << '\n';
            m_logbuf->sync();
          } else if (&f == (std::ostream& (&)(std::ostream&))std::flush) {
            m_logbuf->sync();
          }
        }
        return *this;
      }

      /**
       * set current output level
       */
      void setLevel(logbuf::Level l) {
        if (0 != m_logbuf) {
          m_logbuf->setLevel(l);
        }
      }

      /**
       * set isIp
       */
      void setIsIP(bool i) { m_isIP = i; }

      /**
       * set isTimeStamp
       */
      void setIsTimeStamp(bool i) { m_isTimeStamp = i; }

  private:

      /**
       * prints an IP address to the stream
       */
      void printIP(const unsigned int ip) {
        *((std::ostream*)this)
          << ((ip & 0xFF000000) >> 24) << "."
          << ((ip & 0x00FF0000) >> 16) << "."
          << ((ip & 0x0000FF00) >> 8) << "."
          << ((ip & 0x000000FF));
      }

      /**
       * prints a timeStamp to the stream
       */
      void printTimeStamp(time_t t) {
        struct tm tmstruc;
        localtime_r (&t, &tmstruc);
        *((std::ostream*)this)
          << std::setw(2) << tmstruc.tm_mon+1
          << "/" << tmstruc.tm_mday
          << " " << tmstruc.tm_hour
          << ":" << tmstruc.tm_min
          << ":" << tmstruc.tm_sec;
      }

  private:

      /**
       * The log buffer used on top of the file buffer for
       * prefixing the logs with timestamps
       */
      castor::logbuf *m_logbuf;

      /**
       * Whether next int should be printed as IP addresses
       */
      bool m_isIP;

      /**
       * Whether next int should be printed as a timestamp
       */
      bool m_isTimeStamp;

  };

  /**
   * Manipulators that allow to set the priority
   * of the next messages given to a logstream to VERBOSE
   */
  MANIPULATOR(VERBOSE);
  MANIPULATOR(DEBUG);
  MANIPULATOR(INFO);
  MANIPULATOR(WARNING);
  MANIPULATOR(ERROR);
  MANIPULATOR(FATAL);
  MANIPULATOR(ALWAYS);
  MANIPULATOR(ip);
  MANIPULATOR(trace);
  MANIPULATOR(timeStamp);

} // End of namespace Castor

#endif // CASTOR_LOGSTREAM_H
