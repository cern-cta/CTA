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
 * @(#)$RCSfile: Dlf.hpp,v $ $Revision: 1.1 $ $Release$ $Date: 2005/04/05 11:51:33 $ $Author: sponcec3 $
 *
 * C++ interface to DLF
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef DLF_DLF_WRITE_HPP 
#define DLF_DLF_WRITE_HPP 1

// Include Files
#include "dlf_constants.h"
#include "castor/dlf/Message.hpp"
#include "castor/dlf/Param.hpp"

namespace castor {

  namespace dlf {

    /**
     * Initialization of the DLF logging system
     * @param facilityName name of the DLF facility to use
     * @param messages array of messages to decalre in the
     * facility. The end of the array is marked by a
     * message with negative number.
     */
    void dlf_init(char* facilityName,
                  Message messages[]);

    /**
     * prints a message into dlf.
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
                     struct Cns_fileid *ns_invariant = 0);

 } // end of namespace dlf

} // end of namespace castor


#endif // DLF_DLF_WRITE_HPP
