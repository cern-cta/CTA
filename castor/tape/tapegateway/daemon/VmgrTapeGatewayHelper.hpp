/******************************************************************************
 *                      VmgrTapeGatewayHelper.hpp
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
 * @(#)$RCSfile: VmgrTapeGatewayHelper.hpp,v $ $Revision: 1.9 $ $Release$ $Date: 2009/05/18 13:52:38 $ $Author: gtaur $
 *
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/
#ifndef TAPEGATEWAY_VMGRTAPEGATEWAYHELPER_HPP
#define TAPEGATEWAY_VMGRTAPEGATEWAYHELPER_HPP 

// Include Files
#include "osdep.h"
#include "vmgr_api.h"
#include "castor/exception/Exception.hpp"
#include "castor/tape/utils/BoolFunctor.hpp"

namespace castor {
  namespace tape {
    namespace tapegateway {

      /* Small local struct storing tape information */
      struct TapeInfo {
        /* constructor */
        TapeInfo() {
            memset(&vmgrTapeInfo, 0, sizeof(vmgrTapeInfo));
            memset(dgnBuffer, 0, sizeof(dgnBuffer));
        }
        struct vmgr_tape_info_byte_u64 vmgrTapeInfo;
        char dgnBuffer[8];
      };

      namespace VmgrTapeGatewayHelper {

        /** Gets a tape to be used to migrate data
         * @param initialSizeToTransfer the amount of data to migrate
         * @param tapepoolName the tapepool in which the tape should reside
         * @param vid this parameter will be filled with the name of the tape to be used
         * @param startFseq this parameter will be filled with the fseq where
         * one should start writing on the selected tape
         * @param shuttingDown a functor telling whether we are shutting down
         * @exception throws CASTOR exceptions in case of error
         */
        void getTapeForMigration(const u_signed64 initialSizeToTransfer,
                                 const std::string& tapepoolName,
                                 std::string &vid,
                                 int& startFseq,
                                 const utils::BoolFunctor &shuttingDown)
          throw (castor::exception::Exception);

        /** unbusy a tape in VMGR
         * @param vid the vid of the tape to reset
         * @param shuttingDown a functor telling whether we are shutting down
         * @exception throws CASTOR exceptions in case of error
         */
        void resetBusyTape(const std::string &vid,
                                 const utils::BoolFunctor &shuttingDown)
          throw (castor::exception::Exception);
      
        void bulkUpdateTapeInVmgr(u_signed64 filesCount, signed64 highestFseq, u_signed64 totalBytes,
            u_signed64 totalCompressedBytes, const std::string& vid, const utils::BoolFunctor &shuttingDown)
          throw (castor::exception::Exception);
      
        void setTapeAsFull(const std::string &vid, const utils::BoolFunctor &shuttingDown)
          throw (castor::exception::Exception);

        void setTapeAsReadonlyAndUnbusy(const  std::string &vid, const utils::BoolFunctor &shuttingDown)
          throw (castor::exception::Exception);

        int maxFseqFromLabel(const char* label);

        /* get information concerning a tape from VMGR
         * @param vid the concerned tape
         * @return a struct containing the vmgr information
         * @exception throws CASTOR exceptions in case of error
         */
        TapeInfo getTapeInfo(const std::string &vid, const utils::BoolFunctor &shuttingDown)
          throw (castor::exception::Exception);
        
        /* get information concerning a tape from VMGR and checks
         * that the tape is available
         * @param vid the concerned tape
         * @return a struct containing the vmgr information
         * @exception throws CASTOR exceptions in case of error, in particular
         * if the tape is not available
         */
        TapeInfo getTapeInfoAssertAvailable(const std::string &vid, const utils::BoolFunctor &shuttingDown)
          throw (castor::exception::Exception);

      } // end of namespace VmgrTapeGatewayHelper
    } // end of namespace tapegateway
  } // end of namespace tape
}  // end of namespace castor
#endif // TAPEGATEWAY_VMGRTAPEGATEWAYHELPER_HPP 
