/******************************************************************************
 *                 castor/tapelabel/LabelCmd.cpp
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
 * @author Steven.Murray@cern.ch
 *****************************************************************************/

#include "castor/tape/label/LabelCmd.hpp"
#include "castor/tape/label/ParsedTpLabelCommandLine.hpp"

#include <getopt.h>

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::tape::label::LabelCmd::LabelCmd(std::istream &inStream,
  std::ostream &outStream, std::ostream &errStream,
  legacymsg::TapeserverProxy &tapeserver) throw():
  m_programName("castor-tape-label"),
  m_in(inStream), m_out(outStream), m_err(errStream), m_tapeserver(tapeserver),
  m_debugBuf(outStream), m_dbg(&m_debugBuf) {
}

//------------------------------------------------------------------------------
// parseCommandLine
//------------------------------------------------------------------------------
castor::tape::label::ParsedTpLabelCommandLine
  castor::tape::label::LabelCmd::parseCommandLine(const int argc,
  char **argv) throw(castor::exception::Exception) {
  ParsedTpLabelCommandLine cmdLine;

  static struct option longopts[] = {
    {"drive", 0, NULL, 'D'},
    {"density", 1, NULL, 'd'},
    {"dgn", 0, NULL, 'g'},
    {"vid", 1, NULL, 'V'},
    {"help", 0, NULL, 'h'},
    {NULL, 0, NULL,  0 }
  };

  optind = 1;
  opterr = 0;

  char c;

  while((c = getopt_long(argc, argv, "D:d:g:V:h", longopts, NULL)) != -1) {

    switch (c) {
    case 'D':
      cmdLine.driveIsSet = true;
      try {
        castor::utils::copyString(cmdLine.drive, optarg);
      } catch(castor::exception::Exception &ex) {
        castor::exception::Internal ex;
        ex.getMessage() <<
          "Failed to copy the argument of the drive command-line option"
          " into the internal data structures"
          ": " << ex.getMessage().str();
        throw ex;
      }
      break;

    case 'd':
      cmdLine.densityIsSet = true;
      try {
        castor::utils::copyString(cmdLine.density, optarg);
      } catch(castor::exception::Exception &ex) {
        castor::exception::Internal ex;
        ex.getMessage() <<
          "Failed to copy the argument of the density command-line option"
          " into the internal data structures"
          ": " << ex.getMessage().str();
        throw ex;
      }
      break;

    case 'g':
      cmdLine.dgnIsSet = true;
      try {
        castor::utils::copyString(cmdLine.dgn, optarg);
      } catch(castor::exception::Exception &ex) {
        castor::exception::Internal ex;
        ex.getMessage() <<
          "Failed to copy the argument of the dgn command-line option"
          " into the internal data structures"
          ": " << ex.getMessage().str();
        throw ex;
      }
      break;

    case 'V':
      cmdLine.vidIsSet = true;
      try {
        utils::checkVidSyntax(optarg);
      } catch(castor::exception::InvalidArgument &ex) {
        castor::exception::InvalidArgument ex2;
        ex2.getMessage() <<
          "First command-line argument must be a valid VID: "
          << ex.getMessage().str();
        throw ex2;
      }
      try {
        castor::utils::copyString(cmdLine.vid, optarg);
      } catch(castor::exception::Exception &ex) {
        castor::exception::Internal ex;
        ex.getMessage() <<
          "Failed to copy the argument of the vid command-line option"
          " into the internal data structures"
          ": " << ex.getMessage().str();
        throw ex;
      }
      break;

    case 'h':
      cmdLine.helpIsSet = true;
      break;

    case ':':
      {
        castor::exception::InvalidArgument ex;
        ex.getMessage() << "The -" << (char)optopt
          << " option requires a parameter";
        throw ex;
      }
      break;

    case '?':
      {
        castor::exception::InvalidArgument ex;

        if(optopt == 0) {
          ex.getMessage() << "Unknown command-line option";
        } else {
          ex.getMessage() << "Unknown command-line option: -" << (char)optopt;
        }
        throw ex;
      }
      break;

    default:
      {
        castor::exception::Internal ex;
        ex.getMessage() <<
          "getopt_long returned the following unknown value: 0x" <<
          std::hex << (int)c;
        throw ex;
      }
    }
  }

  // There is no need to continue parsing when the help option is set
  if(cmdLine.helpIsSet) {
    return cmdLine;
  }

  if(!cmdLine.allCompulsoryOptionsSet()) {
    castor::exception::Internal ex;
    ex.getMessage() << "Not all compulsory options have been set";
    throw ex;
  }

  return cmdLine;
}

//------------------------------------------------------------------------------
// usage
//------------------------------------------------------------------------------
void castor::tape::label::LabelCmd::usage(std::ostream &os) const throw() {
  os <<
    "Usage:\n"
    "\t" << m_programName << " -D drive -d density -g dgn -V vid [-h]\n"
    "\n"
    "Where:\n"
    "\n"
    "\t-D, --drive   <drive>   Explicit name of drive to be used.\n"
    "\t-d, --density <density> Tape density.\n"
    "\t-g, --dgn     <dgn>     Device group name of the tape.\n"
    "\t-V, --vid     <vid>     Volume ID of the tape.\n"
    "\t-h, --help              Print this help message and exit.\n"
    "\n"
    "Constraints:\n"
    "\n"
    "\tAll arguments, except -h, are mandatory!\n"
    "\n"
    "Example:\n"
    "\n"
    "\t" << m_programName << " -D T10D6515 -d 8000GC -g T10KD6 -V T54321\n"
    "\n"
    "Comments to: Castor.Support@cern.ch" << std::endl;
}

//------------------------------------------------------------------------------
// bool2Str
//------------------------------------------------------------------------------
std::string castor::tape::label::LabelCmd::bool2Str(const bool value) const throw() {
  return value ? "TRUE" : "FALSE";
}
