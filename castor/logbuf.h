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
 * @(#)$RCSfile: logbuf.h,v $ $Revision: 1.2 $ $Release$ $Date: 2004/07/07 16:01:08 $ $Author: sponcec3 $
 *
 *
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef CASTOR_LOGBUF_H
#define CASTOR_LOGBUF_H 1

// Include Files
#include <dlf_api.h>
#include "Cuuid.h"
#include <sstream>
#include <string>
#include <serrno.h>
#include <iostream>

namespace castor {

  class logbuf : public std::stringbuf {

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
     * Constructor
     */
    logbuf(std::string &name) :
      std::stringbuf(std::ios_base::out),
      m_curLevel(INFO) {
      int e;
      if ((e = dlf_init(name.c_str())) < 0) {
        std::cerr << "Unable to initialize DLF :"
                  << std::endl << sstrerror(e)
                  << std::endl;
      }
    }

      /**
       * set current output level
       */
      void setLevel(logbuf::Level l) { m_curLevel = l; }

  public:

      /**
       * Synchronizes the buffer with DLF
       */
      virtual int sync() {
        if (0 == str().size()) return 0;
        // Write current message to DLF
        Cuuid_t cuuid;
        std::string msg = str();
        // Compute DLF level for the message
        int level;
        switch (m_curLevel) {
        case (VERBOSE) :
        case (DEBUG) :
          level = DLF_LVL_DEBUG;
          break;
        case (INFO) :
          level = DLF_LVL_USAGE;
          break;
        case (WARNING) :
          level = DLF_LVL_WARNING;
          break;
        case (ERROR) :
          level = DLF_LVL_ERROR;
          break;
        case (FATAL) :
          level = DLF_LVL_ALERT;
          break;
        case (ALWAYS) :
          level = DLF_LVL_EMERGENCY;
          break;
        }
        // Take care of long messages
        if (str().size() <= DLF_MAXSTRVALLEN) {
          dlf_write(cuuid, m_curLevel, 0, 0, 1,
                    "MESSAGE", DLF_MSG_PARAM_STR, msg.c_str());
        } else {
          // Message too long, cut it into pieces
          const char* longmsg = msg.c_str();
          int size = msg.size();
          char buffer[DLF_MAXSTRVALLEN+1];
          strncpy(buffer, longmsg, DLF_MAXSTRVALLEN);
          buffer[DLF_MAXSTRVALLEN] = 0;
          dlf_write(cuuid, level, 0, 0, 1,
                    "MESSAGE", DLF_MSG_PARAM_STR, buffer);
          int index = DLF_MAXSTRVALLEN;
          while (index < size) {
            int bitLength = DLF_MAXSTRVALLEN;
            if (size - index < DLF_MAXSTRVALLEN) {
              bitLength = size - index;
            }
            strncpy(buffer, longmsg, bitLength);
            buffer[bitLength] = 0;
            dlf_write(cuuid, level, 0, 0, 1,
                      "CONTINUATION", DLF_MSG_PARAM_STR, buffer);
            index = index + bitLength;
          }
        }
        // Erase buffer
        str("");
        return 0;
      }

  private:

      /**
       * The current level of output for the stream.
       * Next calls to << will use this level
       */
      logbuf::Level m_curLevel;

  };


} // End of namespace Castor

#endif // CASTOR_LOGBUF_H
