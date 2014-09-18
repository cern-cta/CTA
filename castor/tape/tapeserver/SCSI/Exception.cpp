/******************************************************************************
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
 * 
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#include "Exception.hpp" 

void castor::tape::SCSI::ExceptionLauncher(const SCSI::Structures::LinuxSGIO_t & sgio, std::string context) {
  if (SCSI::Status::GOOD != sgio.status) {
    if (SCSI::Status::CHECK_CONDITION == sgio.status) {  
      unsigned char senseKey;   
      castor::tape::SCSI::Structures::senseData_t<255> * sense = 
        (SCSI::Structures::senseData_t<255> *)sgio.sbp;   
      try {
        senseKey = sense->getSenseKey();
      } catch (...) {
        throw Exception(sgio.status,
          (SCSI::Structures::senseData_t<255> *)sgio.sbp, context);
      }
      switch (senseKey) {
        case castor::tape::SCSI::senseKeys::notReady :
          throw NotReadyException(sgio.status,
            (SCSI::Structures::senseData_t<255> *)sgio.sbp, context); 
        case castor::tape::SCSI::senseKeys::unitAttention :
          throw UnitAttentionException(sgio.status,
            (SCSI::Structures::senseData_t<255> *)sgio.sbp, context);    
        default:
          throw Exception(sgio.status,
            (SCSI::Structures::senseData_t<255> *)sgio.sbp, context); 
      }
    } else {
      throw Exception(sgio.status,
        (SCSI::Structures::senseData_t<255> *)sgio.sbp, context);
    }
  }
}
