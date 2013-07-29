// ----------------------------------------------------------------------
// File: SCSI/Constants.hh
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

#include "Constants.hh"
#include <sstream>

std::string SCSI::tapeAlertToString(uint16_t parameterCode)
{
  std::stringstream ret;
  ret << std::hex << std::nouppercase << std::showbase;
  if (parameterCode < 1 || parameterCode > 64) {
    ret << "Unexpected tapeAlert code: " << parameterCode;
    return ret.str();
  } else if (parameterCode >= 0x28 && parameterCode <= 0x2e) {
    ret << "Obsolete tapeAlert code: " << parameterCode;
    return ret.str();
  }  
  switch(parameterCode) {
    /* This is generated with the following small perl and a copy-paste from SSC-3: 
       * #!/usr/bin/perl -w
       * 
       * my $step=0;
       * while (<>) {
       *         chomp;
       *         if ($step == 0) {
       *                 s/h//;
       *                 print "  case 0x".$_.":\n";
       *         } elsif ($step == 1) {
       *                 print "    return \"".$_."\";\n";
       *         }
       *         $step = ( $step + 1 ) % 4;
       * }
       */
    case 0x01:
      return "Read warning";
    case 0x02:
      return "Write warning";
    case 0x03:
      return "Hard error";
    case 0x04:
      return "Medium";
    case 0x05:
      return "Read failure";
    case 0x06:
      return "Write failure";
    case 0x07:
      return "Medium life";
    case 0x08:
      return "Not data grade";
    case 0x09:
      return "Write protect";
    case 0x0A:
      return "Volume removal prevented";
    case 0x0B:
      return "Cleaning volume";
    case 0x0C:
      return "Unsupported format";
    case 0x0D:
      return "Recoverable mechanical cartridge failure";
    case 0x0E:
      return "Unrecoverable mechanical cartridge failure";
    case 0x0F:
      return "Memory chip in cartridge failure";
    case 0x10:
      return "Forced eject";
    case 0x11:
      return "Read only format";
    case 0x12:
      return "Tape directory corrupted on load";
    case 0x13:
      return "Nearing medium life";
    case 0x14:
      return "Cleaning required";
    case 0x15:
      return "Cleaning requested";
    case 0x16:
      return "Expired cleaning volume";
    case 0x17:
      return "Invalid cleaning volume";
    case 0x18:
      return "Retension requested";
    case 0x19:
      return "Multi-port interface error on a primary port";
    case 0x1A:
      return "Cooling fan failure";
    case 0x1B:
      return "Power supply failure";
    case 0x1C:
      return "Power consumption";
    case 0x1D:
      return "Drive preventive maintenance required";
    case 0x1E:
      return "Hardware A";
    case 0x1F:
      return "Hardware B";
    case 0x20:
      return "Primary interface";
    case 0x21:
      return "Eject volume";
    case 0x22:
      return "Microcode update fail";
    case 0x23:
      return "Drive humidity";
    case 0x24:
      return "Drive temperature";
    case 0x25:
      return "Drive voltage";
    case 0x26:
      return "Predictive failure";
    case 0x27:
      return "Diagnostics required";
    case 0x2F:
      return "External data encryption control - communication failure";
    case 0x30:
      return "External data encryption control - key manager returned an error";
    case 0x31:
      return "Diminished native capacity";
    case 0x32:
      return "Lost statistics";
    case 0x33:
      return "Tape directory invalid at unload";
    case 0x34:
      return "Tape system area write failure";
    case 0x35:
      return "Tape system area read failure";
    case 0x36:
      return "No start of data";
    case 0x37:
      return "Loading or threading failure";
    case 0x38:
      return "Unrecoverable unload failure";
    case 0x39:
      return "Automation interface failure";
    case 0x3A:
      return "Microcode failure";
    case 0x3B:
      return "WORM volume - integrity check failed";
    case 0x3C:
      return "WORM volume - overwrite attempted";      
    default:
      ret << "Reserved tapeAlert code: " << parameterCode;
      return ret.str();
  }
}

/**
 * Turn a SCSI status code into a string
 * @param status
 * @return 
 */
std::string SCSI::statusToString(unsigned char status)
{
  switch (status) {
    case 0x00:
      return "GOOD";
    case 0x02:
      return "CHECK CONDITION";
    case 0x04:
      return "CONDITION MET";
    case 0x08:
      return "BUSY";
    case 0x18:
      return "Reservation conflict";
    case 0x28:
      return "TASK SET FULL";
    case 0x30:
      return "ACA ACTIVE";
    case 0x40:
      return "TASK ABORTED";
    default:
      std::stringstream ret;
      ret << "Reserved of obsolete code: "
          << std::hex << std::nouppercase << std::showbase
          << status;
      return ret.str();
  }
}
