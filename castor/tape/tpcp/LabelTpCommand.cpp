/******************************************************************************
 *                 castor/tape/tpcp/LabelTpCommand.cpp
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
 * @author dkruse@cern.ch
 *****************************************************************************/
 
#include "castor/Constants.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/io/io.hpp"
#include "castor/tape/tpcp/LabelTpCommand.hpp"
#include "castor/tape/utils/utils.hpp"
#include "castor/utils/utils.hpp"
#include "h/Ctape_constants.h"
#include "ParsedTpLabelCommandLine.hpp"

#include <errno.h>
#include <getopt.h>
#include <memory>
#include <sstream>
#include <time.h>
#include <sys/stat.h>


//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::tape::tpcp::LabelTpCommand::LabelTpCommand() throw() :
  m_umask(umask(0)) {
  // Restore original umask
  umask(m_umask);
}


//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::tape::tpcp::LabelTpCommand::~LabelTpCommand() throw() {
  // Do nothing
}


//------------------------------------------------------------------------------
// usage
//------------------------------------------------------------------------------
void castor::tape::tpcp::LabelTpCommand::usage(std::ostream &os) const throw() {
  os <<
    "Usage:\n"
    "\t" << m_programName << " -D drive -d density -g dgn -V vid\n"
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
// parseCommandLine
//------------------------------------------------------------------------------
void castor::tape::tpcp::LabelTpCommand::parseCommandLine(const int argc,
  char **argv) throw(castor::exception::Exception) {

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
      m_cmdLine.drive_is_set = true;
      try {
        castor::utils::copyString(m_cmdLine.drive, optarg);
      } catch(castor::exception::Exception &ex) {
        TAPE_THROW_EX(castor::exception::Internal,
          ": Failed to copy the argument of the drive command-line option"
          " into the internal data structures"
          ": " << ex.getMessage().str());
      }
      break;

    case 'd':
      m_cmdLine.density_is_set = true;
      try {
        castor::utils::copyString(m_cmdLine.density, optarg);
      } catch(castor::exception::Exception &ex) {
        TAPE_THROW_EX(castor::exception::Internal,
          ": Failed to copy the argument of the density command-line option"
          " into the internal data structures"
          ": " << ex.getMessage().str());
      }
      break;

    case 'g':
      m_cmdLine.dgn_is_set = true;
      try {
        castor::utils::copyString(m_cmdLine.dgn, optarg);
      } catch(castor::exception::Exception &ex) {
        TAPE_THROW_EX(castor::exception::Internal,
          ": Failed to copy the argument of the dgn command-line option"
          " into the internal data structures"
          ": " << ex.getMessage().str());
      }
      break;

    case 'V':
      m_cmdLine.vid_is_set = true;
      try {
        utils::checkVidSyntax(optarg);
      } catch(castor::exception::InvalidArgument &ex) {
        castor::exception::InvalidArgument ex2;
        ex2.getMessage() <<
          "\tFirst command-line argument must be a valid VID:\n"
          "\t" << ex.getMessage().str();
        throw ex2;
      }
      try {
        castor::utils::copyString(m_cmdLine.vid, optarg);
      } catch(castor::exception::Exception &ex) {
        TAPE_THROW_EX(castor::exception::Internal,
          ": Failed to copy the argument of the vid command-line option"
          " into the internal data structures"
          ": " << ex.getMessage().str());
      }
      break;

    case 'h':
      m_cmdLine.help_needed = true;
      break;

    case ':':
      {
        castor::exception::InvalidArgument ex;
        ex.getMessage() << "\tThe -" << (char)optopt
          << " option requires a parameter";
        throw ex;
      }
      break;

    case '?':
      {
        castor::exception::InvalidArgument ex;

        if(optopt == 0) {
          ex.getMessage() << "\tUnknown command-line option";
        } else {
          ex.getMessage() << "\tUnknown command-line option: -" << (char)optopt;
        }
        throw ex;
      }
      break;

    default:
      {
        castor::exception::Internal ex;
        ex.getMessage() <<
          "\tgetopt_long returned the following unknown value: 0x" <<
          std::hex << (int)c;
        throw ex;
      }
    }
  }

  // There is no need to continue parsing when the help option is set
  if(m_cmdLine.help_needed) {
    return;
  }

  // If not all compulsory arguments have been set give help
  if(!m_cmdLine.is_all_set()) {
    m_cmdLine.help_needed = true;
    return;
  }
}
