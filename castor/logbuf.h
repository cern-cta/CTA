/******************************************************************************
 *                      logbuf.h
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
 * @(#)$RCSfile: logbuf.h,v $ $Revision: 1.4 $ $Release$ $Date: 2004/07/12 14:19:03 $ $Author: sponcec3 $
 *
 * An abstract string buffer for the log that is able
 * to handle levels of output
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef CASTOR_LOGBUF_H
#define CASTOR_LOGBUF_H 1

// Include Files
#include <sstream>

namespace castor {

  class logbuf : public std::stringbuf {

  public:

    /**
     * The different possible levels of output
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
     * Constructor
     */
    logbuf() throw() : m_curLevel(INFO),
      std::stringbuf(std::ios_base::out) {}

    /**
     * Sets current output level
     */
    void setLevel(logbuf::Level l) throw() { m_curLevel = l; }

    /**
     * Gets the current output level
     */
    logbuf::Level level() throw() { return m_curLevel; }

  public:
    
    /**
     * Synchronizes the buffer
     */
    virtual int sync() throw() = 0;
    
  private:

    /**
     * The current level of output for the stream.
     * Next calls to << will use this level
     */
    logbuf::Level m_curLevel;

  };


} // End of namespace Castor

#endif // CASTOR_LOGBUF_H
