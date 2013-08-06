// ----------------------------------------------------------------------
// File: SCSI/Exception.hh
// Author: Eric Cano - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * Tape Server                                                          *
 * Copyright (C) 2013 CERN/Switzerland                                  *
 *                                                                      *
 * This program is free software: you can redistribute it and/or modify *
 * it under the terms of the GNU General Public License as published by *
 * the Free Software Foundation, either version 3 of the License, or    *
 * (at your option) any later version.                                  *
 *                                                                      *
 * This program is distributed in the hope that it will be useful,      *
 * but WITHOUT ANY WARRANTY; without even the implied warranty of       *
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the        *
 * GNU General Public License for more details.                         *
 *                                                                      *
 * You should have received a copy of the GNU General Public License    *
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
 ************************************************************************/

#include "../Exception/Exception.hh"
#include "Structures.hh"
#include "Constants.hh"

#include <sstream>

namespace SCSI {
  /**
   * An exception class turning SCSI sense data into a loggable string
   */
  class Exception: public Tape::Exception {
  public:
    Exception(unsigned char status, SCSI::Structures::senseData_t<255> * sense,
            const std::string & context = ""):
        Tape::Exception("") {
      std::stringstream w;
      w << context << (context.size()?" ":"") 
              << "SCSI command failed with status "
              << SCSI::statusToString(status);
      if (SCSI::Status::CHECK_CONDITION == status) {
        try {
          w << ": " << sense->getACSString();
        } catch (Tape::Exception &ex) {
          w << ": In addition, failed to get ACS string: "
                  << ex.shortWhat();
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
}
