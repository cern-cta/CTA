/******************************************************************************
 *                      dlf_write.hpp
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
 * @(#)$RCSfile: Dlf.hpp,v $ $Revision: 1.12 $ $Release$ $Date: 2009/08/18 09:42:51 $ $Author: waldron $
 *
 * C++ interface to DLF
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef DLF_DLF_WRITE_HPP
#define DLF_DLF_WRITE_HPP 1

// Include Files
#include "dlf_api.h"
#include "castor/dlf/Message.hpp"
#include "castor/dlf/Param.hpp"
#include "castor/exception/Exception.hpp"
#include <vector>

/**
 * Logs a message and adds the context information of file, line and function
 * as three parameters of the message.
 */
#define CASTOR_DLF_WRITEC(uuid, severity, message_no) \
  castor::dlf::dlf_writepc( \
    __FILE__, \
    __LINE__, \
    __PRETTY_FUNCTION__, \
    uuid, \
    severity, \
    message_no)

/**
 * Logs a message with a set of parameters and automatically generates and
 * adds the context information of file, line and function to the parameters.
 */
#define CASTOR_DLF_WRITEPC(uuid, severity, message_no, params) \
  castor::dlf::dlf_writepc( \
    __FILE__, \
    __LINE__, \
    __PRETTY_FUNCTION__, \
    uuid, \
    severity, \
    message_no, \
    params)

namespace castor {

  namespace dlf {

    /**
     * Vector of messages that should be declared
     * at initialization. These messages were sent to
     * dlf_addMessage before it was initialized and
     * thus have to wait here.
     */
    std::vector<std::pair<int, castor::dlf::Message*> >&
    dlf_getPendingMessages() throw();

    /**
     * Initialization of the DLF logging system
     * @param facilityName name of the DLF facility to use
     * @param messages array of messages to declare in the
     * facility. The end of the array is marked by a
     * message with negative number.
     * @param throws a CASTOR exception in case of failure
     */
    void dlf_init(const char* facilityName,
                  Message messages[])
      throw (castor::exception::Exception);

    /**
     * Adds messages to the current DLF facility.
     * This method can be called before the initialization of DLF
     * if needed. In such a case, the messages will be stored
     * and added at initialization time.
     * Note that no exception will ever be thrown in case of failure.
     * Failures will actually be silently ignored in order to not
     * impact the processing.
     * @param offset the offset to add to each message number.
     * This is to avoid collisions with previously added messages
     * @param messages array of messages to decalre in the
     * facility. The end of the array is marked by a
     * message with negative number.
     */
    void dlf_addMessages(int offset,
                         Message messages[]) throw();

    /**
     * prints a message into dlf. Note that no exception will ever
     * be thrown in case of failure. Failures will actually be silently
     * ignored in order to not impact the processing.
     * @param uuid the uuid of the component issuing the message
     * @param message_no the message number in the facility.
     * @param severity the severity of the message.
     * @param numparams the number of parameters in the message
     * @param params the parameters of the message, given as an array
     * @ns_invariant the castor file concerned by the message
     * (if any), given as a name server fileId.
     */
    void dlf_writep (Cuuid_t uuid,
                     int severity,
                     int message_no,
                     int numparams = 0,
                     castor::dlf::Param params[] = 0,
                     struct Cns_fileid *ns_invariant = 0) throw();

    /**
     * A template function that wraps dlf_writep in order to get the compiler
     * to automatically determine the size of the params parameter, therefore
     * removing the need for the devloper to provide it explicity.
     */
    template<int numparams>
    void dlf_writep (Cuuid_t uuid,
                     int severity,
                     int message_no,
                     castor::dlf::Param (&params)[numparams],
                     struct Cns_fileid *ns_invariant = 0) throw() {
      dlf_writep(uuid, severity, message_no, numparams, params, ns_invariant);
    }

    /**
     * prints a message together with the context information file, line and
     * function into dlf. Note that no exception will ever
     * be thrown in case of failure. Failures will actually be silently
     * ignored in order to not impact the processing.
     * @param file the name of the file where dlf_writepc was called.
     * @param line the number of the line where dlf_writepc was called.
     * @param function the name of the function where dlf_writepc was called.
     * @param uuid the uuid of the component issuing the message
     * @param message_no the message number in the facility.
     * @param severity the severity of the message.
     * @param numparams the number of parameters in the message
     * @param params the parameters of the message, given as an array
     * @ns_invariant the castor file concerned by the message
     * (if any), given as a name server fileId.
     */
    void dlf_writepc (const char *file,
                      const int line,
                      const char *function,
                      Cuuid_t uuid,
                      int severity,
                      int message_no,
                      int numparams = 0,
                      castor::dlf::Param params[] = 0,
                      struct Cns_fileid *ns_invariant = 0) throw();

    /**
     * A template function that wraps dlf_writepc in order to get the compiler
     * to automatically determine the size of the params parameter, therefore
     * removing the need for the devloper to provide it explicity.
     */
    template<int numparams>
    void dlf_writepc (const char *file,
                      const int line,
                      const char *function,
                      Cuuid_t uuid,
                      int severity,
                      int message_no,
                      castor::dlf::Param (&params)[numparams],
                      struct Cns_fileid *ns_invariant = 0) throw() {
      dlf_writepc(file, line, function, uuid, severity, message_no, numparams,
        params, ns_invariant);
    }

    /**
     * wrapper to dlf_writte but it compounds the struct Cns_fileId
     * ns_invariantfrom the prints a message into dlf. Note that no
     * exception will ever
     * be thrown in case of failure. Failures will actually be silently
     * ignored in order to not impact the processing.
     * @param uuid the uuid of the component issuing the message
     * @param message_no the message number in the facility.
     * @param severity the severity of the message.
     * @param numparams the number of parameters in the message
     * @param params the parameters of the message, given as an array
     * @param fileId the castor file id
     * @param nsHost the name server
     */
    void dlf_writep (Cuuid_t uuid,
                     int severity,
                     int message_no,
                     u_signed64 fileId,
                     std::string nsHost,
                     int numparams = 0,
                     castor::dlf::Param params[] = 0) throw();

    /**
     * A template function that wraps dlf_writep in order to get the compiler
     * to automatically determine the size of the params parameter, therefore
     * removing the need for the devloper to provide it explicity.
     */
    template<int numparams>
    void dlf_writep (Cuuid_t uuid,
                     int severity,
                     int message_no,
                     u_signed64 fileId,
                     std::string nsHost,
                     castor::dlf::Param (&params)[numparams]) throw() {
      dlf_writep(uuid, severity, message_no, fileId, nsHost, numparams,
        params);
    }

  } // end of namespace dlf

} // end of namespace castor


#endif // DLF_DLF_WRITE_HPP
