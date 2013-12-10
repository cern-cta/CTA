/******************************************************************************
 *                      SCSI/Exception.hpp
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

#include "../exception/Exception.hpp"
#include "Structures.hpp"
#include "Constants.hpp"

#include <sstream>

namespace castor {
namespace tape {
namespace SCSI {
  /**
   * An exception class turning SCSI sense data into a loggable string
   */
  class Exception: public castor::tape::Exception {
  public:
    Exception(unsigned char status, castor::tape::SCSI::Structures::senseData_t<255> * sense,
            const std::string & context = ""):
        castor::tape::Exception("") {
      std::stringstream w;
      w << context << (context.size()?" ":"") 
              << "SCSI command failed with status "
              << SCSI::statusToString(status);
      if (SCSI::Status::CHECK_CONDITION == status) {
        try {
          w << ": " << sense->getACSString();
        } catch (Exception &ex) {
          w << ": In addition, failed to get ACS string: "
                  << ex.getMessage();
        }
      }
      setWhat(w.str());
    }
    virtual ~Exception() throw () {}
  };
  /**
   * Automated exception launcher in case of SCSI command error. Does nothing 
   * in the absence of errors.
   * @param sgio the sgio struct.
   */
  void ExceptionLauncher (const SCSI::Structures::LinuxSGIO_t & sgio, std::string context = "");
} // namespace SCSI
} // namespace tape
} // namespace castor
