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

#include "castor/common/CastorConfiguration.hpp"
#include "castor/io/io.hpp"
#include "castor/legacymsg/CommonMarshal.hpp"
#include "castor/legacymsg/GenericMarshal.hpp"
#include "castor/legacymsg/TapeMarshal.hpp"
#include "castor/legacymsg/TapeLabelRqstMsgBody.hpp"
#include "castor/tape/label/LabelCmd.hpp"
#include "castor/tape/label/ParsedTpLabelCommandLine.hpp"
#include "castor/tape/tapeserver/daemon/Constants.hpp"
#include "castor/tape/tpcp/Constants.hpp"
#include "Ctape.h"
#include "Ctape_api.h"
#include "serrno.h"

#include <getopt.h>
#include <iostream>
#include <sstream>

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::tape::label::LabelCmd::LabelCmd() throw():
m_userId(getuid()),
m_groupId(getgid()),
m_programName("castor-tape-label") {
  castor::utils::setBytes(m_dgn, '\0');
}

//------------------------------------------------------------------------------
// parseCommandLine
//------------------------------------------------------------------------------
void castor::tape::label::LabelCmd::parseCommandLine(const int argc, char **argv)  {

  static struct option longopts[] = {
    {"unitname", 1, NULL, 'u'},
    {"vid", 1, NULL, 'V'},
    {"help", 0, NULL, 'h'},
    {"force", 0, NULL, 'f'},
    {"debug", 0, NULL, 'd'},
    {NULL, 0, NULL,  0 }
  };

  optind = 1;
  opterr = 0;

  char c;

  while((c = getopt_long(argc, argv, "u:V:hfd", longopts, NULL)) != -1) {

    switch (c) {
    case 'u':
      m_cmdLine.driveIsSet = true;
      try {
        castor::utils::copyString(m_cmdLine.drive, optarg);
      } catch(castor::exception::Exception &ex) {
        castor::exception::Exception ex;
        ex.getMessage() <<
          "Failed to copy the argument of the unit name command-line option"
          " into the internal data structures"
          ": " << ex.getMessage().str();
        throw ex;
      }
      break;

    case 'V':
      m_cmdLine.vidIsSet = true;
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
        castor::utils::copyString(m_cmdLine.vid, optarg);
      } catch(castor::exception::Exception &ex) {
        castor::exception::Exception ex;
        ex.getMessage() <<
          "Failed to copy the argument of the vid command-line option"
          " into the internal data structures"
          ": " << ex.getMessage().str();
        throw ex;
      }
      break;

    case 'h':
      m_cmdLine.helpIsSet = true;
      break;
      
    case 'f':
      m_cmdLine.forceIsSet = true;
      break;
      
    case 'd':
      m_cmdLine.debugIsSet = true;
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
        castor::exception::Exception ex;
        ex.getMessage() <<
          "getopt_long returned the following unknown value: 0x" <<
          std::hex << (int)c;
        throw ex;
      }
    }
  }
}

//------------------------------------------------------------------------------
// usage
//------------------------------------------------------------------------------
void castor::tape::label::LabelCmd::usage(std::ostream &os) const throw() {
  os <<
    "Usage:\n"
    "\t" << m_programName << " [-u unitname] -V vid [-h] [-f] [-d]\n"
    "\n"
    "Where:\n"
    "\n"
    "\t-u, --unitname <unitname>   Name of drive to be used.\n"
    "\t-V, --vid      <vid>        Volume ID of the tape.\n"
    "\t-h, --help                  Print this help message and exit.\n"
    "\t-f, --force                 Use this option to label a non-blank tape\n"
    "\t-d, --debug                 Debug mode on (default off).\n"
    "\n"
    "Constraints:\n"
    "\n"
    "\tThe <vid> argument is mandatory! The <unitname> is mandatory only if TPCONFIG has more than 1 drive\n"
    "\n"
    "Example:\n"
    "\n"
    "\t" << m_programName << " -u T10D6515 -V T54321 -f\n"
    "\n"
    "Comments to: Castor.Support@cern.ch" << std::endl;
}

//------------------------------------------------------------------------------
// main
//------------------------------------------------------------------------------
int castor::tape::label::LabelCmd::main(const int argc, char **argv) throw() {

  // Parse the command line
  try {
    parseCommandLine(argc, argv);
  } catch (castor::exception::Exception &ex) {
    std::cerr
      << std::endl
      << "Failed to parse the command-line:\n\n"
      << ex.getMessage().str() << std::endl
      << std::endl;
    usage(std::cerr);
    std::cerr << std::endl;
    return 1;
  }

  // If debug, then display parsed command-line arguments
  if(m_cmdLine.debugIsSet) {
    std::cout << "Parsed command-line =\n" 
            << "m_cmdLine.drive: " << m_cmdLine.drive << std::endl
            << "m_cmdLine.vid: " << m_cmdLine.vid << std::endl 
            << "m_cmdLine.debugIsSet: " << m_cmdLine.debugIsSet << std::endl 
            << "m_cmdLine.forceIsSet: " << m_cmdLine.forceIsSet << std::endl
            << "m_cmdLine.helpIsSet: " << m_cmdLine.helpIsSet << std::endl;
  }

  // Display usage message and exit if help option found on command-line
  if(m_cmdLine.helpIsSet) {
    std::cout << std::endl;
    usage(std::cout);
    std::cout << std::endl;
    return 0;
  }
  
  // The VID should always be specified
  if(!m_cmdLine.vidIsSet) {
    std::cout << std::endl;
    usage(std::cout);
    std::cout << std::endl;
    return 0;
  }
  
  // Execute tpstat to find out the DGN and the number of drives defined in TPCONFIG
  struct drv_status drv_status[CA_MAXNBDRIVES];
  char hostname[CA_MAXHOSTNAMELEN+1];
  const int nbentries = CA_MAXNBDRIVES;
  
  if(0 > gethostname(hostname, sizeof(hostname))) {
    displayErrorMsgAndExit("Call to gethostname() failed", SYERR);
  } 
  
  const int nbDrives = Ctape_status(hostname, drv_status, nbentries);
  if(0 > nbDrives) {
    displayErrorMsgAndExit("Call to Ctape_status() failed", SYERR);
  }
  
  if(!m_cmdLine.driveIsSet && nbDrives!=1) {
    displayErrorMsgAndExit("TPCONFIG contains more than one drive and the unitname option (-u) has not been specified. Please specify which drive to use", 1);    
  }
  else if(!m_cmdLine.driveIsSet && nbDrives==1) {
    castor::utils::copyString(m_dgn, sizeof(m_dgn), drv_status[0].dgn);
    castor::utils::copyString(m_cmdLine.drive, sizeof(m_cmdLine.drive), drv_status[0].drive);
  }
  else { //m_cmdLine.driveIsSet==true
    bool dgn_found=false;
    for(int i=0; i<nbDrives; i++) {
      if(!strcmp(drv_status[i].drive,m_cmdLine.drive)) {
        castor::utils::copyString(m_dgn, sizeof(m_dgn), drv_status[i].dgn);
        dgn_found=true;
      }
    }
    if(!dgn_found) {
      std::ostringstream message;
      message << "Drive " << m_cmdLine.drive << " not found in TPCONFIG";
      displayErrorMsgAndExit(message.str(), 1);
    }
  }

  int rc = 1;
  
  // Execute the command
  try {
    rc = executeCommand();
  } catch(castor::exception::Exception &ex) {
    displayErrorMsgAndExit(ex.getMessage().str().c_str(), 1);
  } catch(std::exception &se) {
    displayErrorMsgAndExit(se.what(), 1);
  } catch(...) {
    displayErrorMsgAndExit("Caught unknown exception", 1);
  }

  return rc;
}

//------------------------------------------------------------------------------
// executeCommand
//------------------------------------------------------------------------------
int castor::tape::label::LabelCmd::executeCommand()  {
  // This command cannot be ran as root
  if(m_userId == 0 && m_groupId == 0) {
    castor::exception::Exception ex(ECANCELED);
    ex.getMessage() <<
      m_programName << " cannot be ran as root";
    throw ex;
  }
  
  const int writeRequestTimeout = 10;
  const int readReplyTimeout = 300; // 5 minutes
  
  writeTapeLabelRequest(writeRequestTimeout);
  legacymsg::GenericReplyMsgBody replymsg = readTapeLabelReply(readReplyTimeout);
  
  if(0 != replymsg.status) {
    displayErrorMsgAndExit(replymsg.errorMessage, replymsg.status);
  }

  return replymsg.status;
}

//------------------------------------------------------------------------------
// writeTapeLabelRequest
//------------------------------------------------------------------------------
void castor::tape::label::LabelCmd::writeTapeLabelRequest(const int timeout) {
  common::CastorConfiguration &castorConf =
    common::CastorConfiguration::getConfig();
  const unsigned short tapeServerLabelPort = castorConf.getConfEntInt(
    "TapeServer", "LabelPort", tapeserver::daemon::TAPESERVER_LABEL_PORT);
  castor::legacymsg::TapeLabelRqstMsgBody body;
  body.uid = m_userId;
  body.gid = m_groupId;
  body.force = m_cmdLine.forceIsSet ? 1 : 0;
  castor::utils::copyString(body.drive, sizeof(body.drive), m_cmdLine.drive);
  castor::utils::copyString(body.dgn, sizeof(body.dgn), m_dgn);
  castor::utils::copyString(body.vid, sizeof(body.vid), m_cmdLine.vid);
  
  char buf[REQBUFSZ];
  const size_t len = castor::legacymsg::marshal(buf, sizeof(buf), body);

  m_smartClientConnectionSock.reset(castor::io::connectWithTimeout("127.0.0.1",
    tapeServerLabelPort, timeout));
  
  try {
    castor::io::writeBytes(m_smartClientConnectionSock.get(), timeout, len, buf);
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to write label request message: " <<
      ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// readTapeLabelReply
//------------------------------------------------------------------------------
castor::legacymsg::GenericReplyMsgBody castor::tape::label::LabelCmd::readTapeLabelReply(const int timeout) {
  
  
  size_t headerBufLen = 12; // 12 bytes of header
  char headerBuf[12];
  memset(headerBuf, '\0', headerBufLen);
  
  try {
    castor::io::readBytes(m_smartClientConnectionSock.get(), timeout, headerBufLen, headerBuf);
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to read label reply message: " <<
      ne.getMessage().str();
    throw ex;
  }
  
  const char *headerBufPtr = headerBuf;
  castor::legacymsg::MessageHeader replyHeader;
  memset(&replyHeader, '\0', sizeof(replyHeader));
  castor::legacymsg::unmarshal(headerBufPtr, headerBufLen, replyHeader);
  
  if(MSG_DATA != replyHeader.reqType || TPMAGIC != replyHeader.magic) {
    castor::exception::Exception ex;
    ex.getMessage() << "Wrong reply type or magic # in label reply message. replymsg.reqType: " << replyHeader.reqType << " (expected: " << MSG_DATA << ") replymsg.magic: " << replyHeader.magic << " (expected: " << TPMAGIC << ")";
    throw ex;
  }
  
  size_t bodyBufLen = 4+CA_MAXLINELEN+1; // 4 bytes of return code + max length of error message
  char bodyBuf[4+CA_MAXLINELEN+1];
  memset(bodyBuf, '\0', bodyBufLen);
  int actualBodyLen = replyHeader.lenOrStatus;
  
  try {
    castor::io::readBytes(m_smartClientConnectionSock.get(), timeout, actualBodyLen, bodyBuf);
  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to read label reply message: " <<
      ne.getMessage().str();
    throw ex;
  }
  
  const char *bodyBufPtr = bodyBuf;
  castor::legacymsg::GenericReplyMsgBody replymsg;
  memset(&replymsg, '\0', sizeof(replymsg));
  castor::legacymsg::unmarshal(bodyBufPtr, bodyBufLen, replymsg);
  
  return replymsg;
}

//------------------------------------------------------------------------------
// displayErrorMsgAndExit
//------------------------------------------------------------------------------
void castor::tape::label::LabelCmd::displayErrorMsgAndExit(
  const std::string &msg, const int errnum) const {

  // Display error message
  {
    const time_t now = time(NULL);
    castor::utils::writeTime(std::cerr, now, castor::tape::tpcp::TIMEFORMAT);
    std::cerr << " " << m_programName << " failed: " << msg << std::endl <<
      "Exit code: " << errnum << std::endl;
  }
  exit(errnum); // Error
}
