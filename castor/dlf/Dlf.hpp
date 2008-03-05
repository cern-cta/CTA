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
 * @(#)$RCSfile: Dlf.hpp,v $ $Revision: 1.6 $ $Release$ $Date: 2008/03/05 16:46:47 $ $Author: riojac3 $
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
    void dlf_init(char* facilityName,
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
                     u_signed64 fileId ,
                     std::string nsHost,
                     int numparams = 0,
                     castor::dlf::Param params[] = 0) throw();

  } // end of namespace dlf

} // end of namespace castor


#endif // DLF_DLF_WRITE_HPP
