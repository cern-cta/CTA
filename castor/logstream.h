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
 * @(#)$RCSfile: logstream.h,v $ $Revision: 1.1.1.1 $ $Release$ $Date: 2004/05/12 12:13:34 $ $Author: sponcec3 $
 *
 *
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef CASTOR_LOGSTREAM_H
#define CASTOR_LOGSTREAM_H 1

// Include Files
#include <string>
#include <fstream>
#include "osdep.h"

#define OPERATOR(T)                             \
  logstream& operator<< (T var) {               \
    if (m_minLevel <= m_curLevel) {             \
      *((std::ostream*)this) << var;            \
    }                                           \
    return *this;                               \
  }

#define OPERATORINT(T)                          \
  logstream& operator<< (T var) {               \
    if (m_minLevel <= m_curLevel) {             \
      if (m_isIP) {                             \
        m_isIP = false;                         \
        printIP(var);                           \
      } else {                                  \
        this->std::ofstream::operator<<(var);   \
      }                                         \
    }                                           \
    return *this;                               \
  }

#define MANIPULATOR(T) castor::logstream& T(castor::logstream& s);

namespace castor {

  class logstream : virtual public std::ofstream {

  public:

    /**
     * The different possible level of output
     */
    typedef enum _Level_ {
      NIL = 0,
      VERBOSE,
      DEBUG,
      INFO,
      WARNING,
      ERROR,
      FATAL,
      ALWAYS,
      NUM_LEVELS
    } Level;

  public:

    /**
     * constructor
     */
    explicit logstream(const char* p, Level l = INFO) :
      std::ofstream(p, std::ios::app), m_minLevel(l),
      m_curLevel(INFO), m_isIP(false) {
      printf("logstream name : %s\n", p);
    }

  public:
    /**
     * Set of operators of this stream
     */
    OPERATOR(char);
    OPERATOR(unsigned char);
    OPERATOR(signed char);
    OPERATOR(short);
    OPERATOR(unsigned short);
    OPERATOR(const char*);
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
     * This operator deals with manipulators specific to
     * castor::logstream
     */
    logstream& operator<< (logstream& (&f)(logstream&)) {
      return f(*this);
    }

    /**
     * This operator deals with manipulators specific to
     * castor::logstream
     */
    logstream& operator<< (std::ostream& (&f)(std::ostream&)) {
      f(*this);
      return *this;
    }

    /**
     * set current output level
     */
    void setLevel(Level l) { m_curLevel = l; }

    /**
     * set isIp
     */
    void setIsIP(bool i) { m_isIP = i; }

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

  private:

    /**
     * The current minimum level of output for the stream
     * everything under it will not be output
     */
    Level m_minLevel;

    /**
     * The current level of output for the stream.
     * Next calls to << will use this level
     */
    Level m_curLevel;

    /**
     * Whether int should be printed as IP addresses
     */
    bool m_isIP;

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

} // End of namespace Castor

#endif // CASTOR_LOGSTREAM_H
