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
#include "castor/stager/Tape.hpp"
#include "castor/tape/tapegateway/FileMigratedNotification.hpp"
#include "castor/tape/utils/BoolFunctor.hpp"

namespace castor {
  namespace tape {
    namespace tapegateway {
      class VmgrTapeGatewayHelper {
        public:
	void getTapeForMigration(const u_signed64 initialSizeToTransfer, const std::string& tapepoolName,
	    int& startFseq, castor::stager::Tape& tapeToUse, const utils::BoolFunctor &shuttingDown) throw (castor::exception::Exception);

	void getDataFromVmgr(castor::stager::Tape& tape, const utils::BoolFunctor &shuttingDown)
	throw (castor::exception::Exception);
	
	void resetBusyTape(const castor::stager::Tape& tape, const utils::BoolFunctor &shuttingDown)
	throw (castor::exception::Exception);

	void updateTapeInVmgr(const castor::tape::tapegateway::FileMigratedNotification& file,
	    const std::string& vid, const utils::BoolFunctor &shuttingDown) throw (castor::exception::Exception);

	void setTapeAsFull(const castor::stager::Tape& tapem, const utils::BoolFunctor &shuttingDown)
	throw (castor::exception::Exception);

        void setTapeAsReadonlyAndUnbusy(const castor::stager::Tape& tape, const utils::BoolFunctor &shuttingDown)
        throw (castor::exception::Exception);

        int maxFseqFromLabel(const char* label);

        /* This class will extract the tape information from the vdqm at construction time.
         * It can loop for a while in case of retries, or throw an exception at construction
         * in case of failure */
        private:
        class TapeInfo {
        public:
          struct vmgr_tape_info_byte_u64 vmgrTapeInfo;
          char dgnBuffer[8];
          TapeInfo(const castor::stager::Tape& tape, const utils::BoolFunctor &shuttingDown)
          throw (castor::exception::Exception);
          virtual ~TapeInfo () throw () {};
        protected:
          std::string m_vid;
          int m_side;
        };
        /* Same as the previous one, but will make sure the tape is not ARCHIVED, EXPORTED or DISABLED
         * In such a case an extra exception will be thrown. */
        class TapeInfoAssertAvailable: public TapeInfo {
        public:
          TapeInfoAssertAvailable(const castor::stager::Tape& tape, const utils::BoolFunctor &shuttingDown)
          throw (castor::exception::Exception);
          virtual ~TapeInfoAssertAvailable () throw () {};
        };
      };
    } // end of namespace tapegateway
  } // end of namespace tape
}  // end of namespace castor
#endif // TAPEGATEWAY_VMGRTAPEGATEWAYHELPER_HPP 
