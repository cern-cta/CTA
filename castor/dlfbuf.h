/******************************************************************************
 *                      dlfbuf.h
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
 * @(#)$RCSfile: dlfbuf.h,v $ $Revision: 1.9 $ $Release$ $Date: 2005/09/12 16:23:35 $ $Author: sponcec3 $
 *
 * A string buffer for logging into dlf
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef CASTOR_DLFBUF_H
#define CASTOR_DLFBUF_H 1

// Include Files
#include <string>
#include <serrno.h>
#include <dlf_api.h>
#include <Cthread_api.h>
#include "castor/logbuf.h"
#include "castor/BaseObject.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/exception/Exception.hpp"

namespace castor {

  class dlfbuf : public castor::logbuf {

  public:

    /**
     * Constructor
     */
    dlfbuf(const std::string &name)
      throw (castor::exception::Exception) :
      castor::logbuf() {
      // If dlf_seterrbuf was no tyet called in this thread
      // then create a buffer and call dlf_seterrbuf
      char** errBuf = errorBuffer();
      if (0 == *errBuf) {
        *errBuf = (char*) malloc(LOGBUFSZ);
        int rc = dlf_seterrbuf(*errBuf, LOGBUFSZ);
        if (rc < 0) {
          castor::exception::Internal e;
          e.getMessage()
            << "Unable to initialize error buffer of DLF :"
            << std::endl
            << sstrerror(serrno);
          throw e;
        }
      }
      if (!s_dlfInitCalled) {
        // If dlf_init not already called,
        // try to call it. But thread safety
        // needs a lock here and a double check
        Cthread_mutex_lock(&s_lock);
        // It could be that somebody was quicker
        if (!s_dlfInitCalled) {
          int rc;
          if ((rc = dlf_init(name.c_str())) != 0) {
            castor::exception::Internal e;
            if (rc > 0) {
              e.getMessage()
                << "The log destination used in dlf_init ("
                << name
                << ") cannot be found in the configuration file.\n"
                << "Please check your configuration.";
            } else {
              e.getMessage() << "Unable to initialize DLF :\n"
                             << sstrerror(serrno) << "\n"
                             << errorBuffer();
            }
            // Don't forget the mutex !
            Cthread_mutex_unlock(&s_lock);
            throw e;
          }
          s_dlfInitCalled = true;
        }
        Cthread_mutex_unlock(&s_lock);
      }
    }

  public:

    /**
     * Synchronizes the buffer with DLF
     */
    virtual int sync() throw() {
      if (0 == str().size()) return 0;
      // Write current message to DLF
      std::string msg = str();
      // Take care of long messages
      if (str().size() <= DLF_MAXSTRVALLEN) {
        dlf_write_internal("MESSAGE", msg.c_str());
      } else {
        // Message too long, cut it into pieces
        const char* longmsg = msg.c_str();
        int size = msg.size();
        char buffer[DLF_MAXSTRVALLEN+1];
        strncpy(buffer, longmsg, DLF_MAXSTRVALLEN);
        buffer[DLF_MAXSTRVALLEN] = 0;
        dlf_write_internal("MESSAGE", buffer);
        int index = DLF_MAXSTRVALLEN;
        while (index < size) {
          int bitLength = DLF_MAXSTRVALLEN;
          if (size - index < DLF_MAXSTRVALLEN) {
            bitLength = size - index;
          }
          strncpy(buffer, longmsg, bitLength);
          buffer[bitLength] = 0;
          dlf_write_internal("CONTINUATION", buffer);
          index = index + bitLength;
        }
      }
      // Erase buffer
      str("");
      return 0;
    }

  private:

    /**
     * get the thread specific error buffer for DLF
     */
    char** errorBuffer() throw() {
      static int C_dlfbuf_TLSkey = -1;
      char** buffer;
      castor::BaseObject::getTLS(&C_dlfbuf_TLSkey,
                                 (void **)&buffer);
      return buffer;
    }

    /**
     * write something to DLF and checks for errors.
     * If any, throws an exception
     */
    void dlf_write_internal(const char* name, const char* msg)
      throw(castor::exception::Exception) {
      // XXX For the time being, we ignore completely
      // XXX errors of dlf_write. Shall we syslog them ?
      //int rc =
      dlf_write (m_uuid, level(), 0, m_fileId, 1,
                 name, DLF_MSG_PARAM_STR, msg);
      //if (0 != rc) {
      //  castor::exception::Internal e;
      //  if (rc > 0) {
      //    e.getMessage()
      //      << "The log destination used in dlf_write) "
      //      << "cannot be found in the configuration file.\n"
      //      << "Please check your configuration.";
      //  } else {
      //    e.getMessage() << "Unable to initialize DLF :\n"
      //                   << sstrerror(serrno) << "\n"
      //                   << errorBuffer();
      //  }
      //  throw e;
      //}
    }

  private:

    /**
     * Whether dlf_init was already called or not
     */
    static bool s_dlfInitCalled;

    /**
     * A lock to ensure a unique call to dlf_init
     */
    static int s_lock;

  };


} // End of namespace Castor

#endif // CASTOR_DLFBUF_H
