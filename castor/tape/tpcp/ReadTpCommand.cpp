/******************************************************************************
 *                 castor/tape/tpcp/ReadTpCommand.cpp
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
 * @author Nicola.Bessone@cern.ch Steven.Murray@cern.ch
 *****************************************************************************/
 
#include "castor/tape/tpcp/ReadTpCommand.hpp"
#include "castor/tape/tpcp/TapeFileSequenceParser.hpp"

#include <getopt.h>


//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::tape::tpcp::ReadTpCommand::ReadTpCommand() throw() {
  // Do nothing
}


//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::tape::tpcp::ReadTpCommand::~ReadTpCommand() throw() {
  // Do nothing
}


//------------------------------------------------------------------------------
// usage
//------------------------------------------------------------------------------
void castor::tape::tpcp::ReadTpCommand::usage(std::ostream &os,
  const char *const programName) throw() {
  os <<
    "Usage:\n"
    "\t" << programName << " VID [OPTIONS] [FILE]...\n"
    "\n"
    "Where:\n"
    "\n"
    "\tVID  The VID of the tape to be read from.\n"
    "\tFILE A filename in RFIO notation [host:]local_path.\n"
    "\n"
    "Options:\n"
    "\n"
    "\t-d, --debug             Print debug information.\n"
    "\t-h, --help              Print this help and exit.\n"
    "\t-s, --server server     Specifies the tape server to be used therefore\n"
    "\t                        overriding the drive scheduling of the VDQM.\n"
    "\t-f, --filelist          File containing a list of filenames in RFIO\n"
    "\t                        notation [host:]local_path\n"
    "\t-q, --sequence sequence The tape file sequences.\n"
    "\n"
    "Constraints:\n"
    "\n"
    "\tThe [FILE].. command-line arguments and the \"-f, --filelist\" option\n"
    "\tare mutually exclusive\n"
    "\n"
    "Comments to: Castor.Support@cern.ch" << std::endl;
}


//------------------------------------------------------------------------------
// parseCommandLine
//------------------------------------------------------------------------------
void castor::tape::tpcp::ReadTpCommand::parseCommandLine(const int argc,
  char **argv) throw(castor::exception::Exception) {

  m_cmdLine.action = Action::read;

  static struct option longopts[] = {
    {"debug"   , 0, NULL, 'd'},
    {"filelist", 1, NULL, 'f'},
    {"help"    , 0, NULL, 'h'},
    {"sequence", 1, NULL, 'q'},
    {"server"  , 1, NULL, 's'},
    {NULL      , 0, NULL,  0 }
  };

  optind = 1;
  opterr = 0;

  char c;

  while((c = getopt_long(argc, argv, ":df:hq:s:", longopts, NULL)) != -1) {

    switch (c) {
    case 'd':
      m_cmdLine.debugSet = true;
      break;

    case 'f':
      m_cmdLine.fileListSet = true;
      m_cmdLine.fileListFilename  = optarg;
      break;

    case 'h':
      m_cmdLine.helpSet = true;
      break;

    case 'q':
      TapeFileSequenceParser::parse(optarg, m_cmdLine.tapeFseqRanges);
      break;

    case 's':
      m_cmdLine.serverSet = true;
      try {
        utils::copyString(m_cmdLine.server, optarg);
      } catch(castor::exception::Exception &ex) {
        TAPE_THROW_EX(castor::exception::Internal,
          ": Failed to copy the argument of the server command-line option"
          " into the internal data structures"
          ": " << ex.getMessage().str());
      }
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
        ex.getMessage()
          << "\tgetopt_long returned the following unknown value: 0x"
          << std::hex << (int)c;
        throw ex;
      }
    } // switch (c)
  } // while ((c = getopt_long(argc, argv, "h", longopts, NULL)) != -1)

  // There is no need to continue parsing when the help option is set
  if( m_cmdLine.helpSet) {
    return;
  }

  // The minimum number of command-line arguments is 1 because the VID must be
  // present
  const int expectedMinArgs = 1;

  // Check the minimum number of command-line arguments are present
  if(argc-optind < expectedMinArgs){
    castor::exception::InvalidArgument ex;

    ex.getMessage() << "\tWrong number of command-line arguments: Actual=" <<
      argc-optind << " Expected minimum=" << expectedMinArgs;

    throw ex;
  }

  const int nbFilenamesOnCommandLine = argc - optind - expectedMinArgs;

  // Filenames on the command-line and the "-f, --filelist" option are mutually
  // exclusive
  if(nbFilenamesOnCommandLine > 0 && m_cmdLine.fileListSet) {
    castor::exception::InvalidArgument ex;

    ex.getMessage() << "\t[FILE].. command-line arguments and the"
       " \"-f, --filelist\" option are\n\tmutually exclusive";

    throw ex;
  }

  // Check the first command-line argument is syntactically a valid VID
  try {
    utils::checkVidSyntax(argv[optind]);
  } catch(castor::exception::InvalidArgument &ex) {
    castor::exception::InvalidArgument ex2;

    ex2.getMessage() << "\tFirst command-line argument must be a valid VID:\n"
      "\t" << ex.getMessage().str();

    throw ex2;
  }

  // Parse the VID command-line argument
  try {
    utils::copyString(m_cmdLine.vid, argv[optind]);
  } catch(castor::exception::Exception &ex) {
    TAPE_THROW_EX(castor::exception::Internal,
      ": Failed to copy VID comand-line argument into the internal data"
      " structures"
      ": " << ex.getMessage().str());
  }

  // Move on to the next command-line argument (there may not be one)
  optind++;

  // Parse any filenames on the command-line
  while(optind < argc) {
    m_cmdLine.filenames.push_back(argv[optind++]);
  }

  // Check that there is at least one file to be recalled
  if(m_cmdLine.tapeFseqRanges.size() == 0) {
    castor::exception::InvalidArgument ex;

    ex.getMessage()
      << "\tThere must be at least one tape file sequence number";

    throw ex;
  }

  // Check that there is no more than one tape file sequence range which goes
  // to END of the tape
  const unsigned int nbRangesWithEnd = countNbRangesWithEnd();
  if(nbRangesWithEnd > 1) {
    castor::exception::InvalidArgument ex;

    ex.getMessage()
      << "\tThere cannot be more than one tape file sequence range whose upper "
         "boundary is end of tape";

    throw ex;
  }
}


//------------------------------------------------------------------------------
// countNbRangesWithEnd
//------------------------------------------------------------------------------
unsigned int castor::tape::tpcp::ReadTpCommand::countNbRangesWithEnd()
  throw (castor::exception::Exception) {

  unsigned int count = 0;

  // Loop through all ranges
  for(TapeFseqRangeList::const_iterator itor=
    m_cmdLine.tapeFseqRanges.begin();
    itor!=m_cmdLine.tapeFseqRanges.end(); itor++) {

    if(itor->upper == 0 ){
        count++;
    }
  }
  return count;
}
