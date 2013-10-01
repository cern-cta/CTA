/******************************************************************************
 *                 castor/tape/mediachanger/QueryVolumeAcsCmd.cpp
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

#include "castor/tape/mediachanger/QueryVolumeAcsCmd.hpp"
#include "castor/tape/utils/utils.hpp"

#include <getopt.h>
#include <iostream>
#include <string.h>
 
//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::tape::mediachanger::QueryVolumeAcsCmd::QueryVolumeAcsCmd(
  std::istream &inStream, std::ostream &outStream, std::ostream &errStream,
  Acs &acs) throw():
  AcsCmd(inStream, outStream, errStream, acs), m_defaultQueryInterval(1),
  m_defaultTimeout(20) {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::tape::mediachanger::QueryVolumeAcsCmd::~QueryVolumeAcsCmd() throw() {
  // Do nothing
}

//------------------------------------------------------------------------------
// main
//------------------------------------------------------------------------------
int castor::tape::mediachanger::QueryVolumeAcsCmd::main(const int argc,
  char *const *const argv) throw() {
  try {
    m_cmdLine = parseCmdLine(argc, argv);
  } catch(castor::exception::InvalidArgument &ia) {
    m_err << "Aborting: Invalid command-line: " << ia.getMessage().str() <<
      std::endl;
    m_err << std::endl;
    usage(m_err);
    return 1;
  } catch(castor::exception::MissingOperand &mo) {
    m_err << "Aborting: Missing operand: " << mo.getMessage().str() <<
      std::endl;
    m_err << std::endl;
    usage(m_err);
    return 1;
  } catch(castor::exception::Internal &ie) {
    m_err << "Aborting: Internal error: " << ie.getMessage().str() <<
      std::endl;
    return 1;
  }

  // Display the usage message to standard out and exit with success if the
  // user requested help
  if(m_cmdLine.help) {
    usage(m_out);
    return 0;
  }

  if(m_cmdLine.debug) {
    m_out << "DEBUG: Using a query interval of " << m_cmdLine.queryInterval << 
      " seconds" << std::endl;
    m_out << "DEBUG: Using a timeout of " << m_cmdLine.timeout << " seconds" << 
      std::endl;
    m_out << "DEBUG: Using volume identifier " << m_cmdLine.volId.external_label
      << std::endl;
  }

  try {
    const QU_VOL_STATUS volStatus = syncQueryVolume();
    writeVolumeStatus(volStatus);
  } catch(castor::exception::QueryVolumeFailed &ex) {
    m_err << "Aborting: " << ex.getMessage().str() << std::endl;
    return 1;
  }

  return 0;
}

//------------------------------------------------------------------------------
// parseCmdLine
//------------------------------------------------------------------------------
castor::tape::mediachanger::QueryVolumeAcsCmdLine
  castor::tape::mediachanger::QueryVolumeAcsCmd::parseCmdLine(
  const int argc, char *const *const argv)
  throw(castor::exception::Internal, castor::exception::InvalidArgument,
    castor::exception::MissingOperand) {

  static struct option longopts[] = {
    {"debug", 0, NULL, 'd'},
    {"help" , 0, NULL, 'h'},
    {"query" , required_argument, NULL, 'q'},
    {"timeout" , required_argument, NULL, 't'},
    {NULL, 0, NULL, 0}
  };
  QueryVolumeAcsCmdLine cmdLine;
  char c;

  // Set the query option to the default value
  cmdLine.queryInterval = m_defaultQueryInterval;

  // Set timeout option to the default value
  cmdLine.timeout = m_defaultTimeout;

  // Prevent getopt() from printing an error message if it does not recognize
  // an option character
  opterr = 0;
  while((c = getopt_long(argc, argv, ":dhq:t:", longopts, NULL)) != -1) {

    switch (c) {
    case 'd':
      cmdLine.debug = TRUE;
      break;
    case 'h':
      cmdLine.help = TRUE;
      break;
    case 'q':
      cmdLine.queryInterval = atoi(optarg);
      if(0 >= cmdLine.queryInterval) {
        castor::exception::InvalidArgument ex;
        ex.getMessage() << "Query value must be an integer greater than 0"
          ": value=" << cmdLine.queryInterval;
        throw ex;
      }
      break;
    case 't':
      cmdLine.timeout = atoi(optarg);
      if(0 >= cmdLine.timeout) {
        castor::exception::InvalidArgument ex;
        ex.getMessage() << "Timeout value must be an integer greater than 0"
          ": value=" << cmdLine.timeout;
        throw ex;
      }
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
    } // switch (c)
  } // while ((c = getopt_long( ... )) != -1)

  // There is no need to continue parsing when the help option is set
  if(cmdLine.help) {
    return cmdLine;
  }

  // Calculate the number of non-option ARGV-elements
  const int nbArgs = argc - optind;

  // Check that VID has been specified
  if(nbArgs < 1) {
    castor::exception::MissingOperand ex;
    ex.getMessage() << "VID must be specified";
    throw ex;
  }

  // Parse the VID command-line argument
  cmdLine.volId = m_acs.str2Volid(argv[optind]);

  return cmdLine;
}

//------------------------------------------------------------------------------
// usage
//------------------------------------------------------------------------------
void castor::tape::mediachanger::QueryVolumeAcsCmd::usage(std::ostream &os)
  const throw() {
  os <<
  "Usage:\n"
  "  queryvolumeacs [options] VID\n"
  "\n"
  "Where:\n"
  "\n"
  "  VID    The VID of the volume to be queried.\n"
  "\n"
  "Options:\n"
  "\n"
  "  -d|--debug            Turn on the printing of debug information.\n"
  "  -h|--help             Print this help message and exit.\n"
  "  -q|--query SECONDS    Time to wait between queries to ACS for responses.\n"
  "                        SECONDS must be an integer value greater than 0.\n"
  "                        The default value of SECONDS is "
    << m_defaultQueryInterval << ".\n"
  "  -t|--timeout SECONDS  Time to wait for the query to conclude. SECONDS\n"
  "                        must be an integer value greater than 0.  The\n"
  "                        default value of SECONDS in "
    << m_defaultTimeout << ".\n"
  "\n"
  "Comments to: Castor.Support@cern.ch" << std::endl;
}

//------------------------------------------------------------------------------
// syncQueryVolume
//------------------------------------------------------------------------------
QU_VOL_STATUS castor::tape::mediachanger::QueryVolumeAcsCmd::syncQueryVolume()
  throw(castor::exception::QueryVolumeFailed) {
  std::ostringstream action;
  action << "query volume " << m_cmdLine.volId.external_label;

  const SEQ_NO mountSeqNumber = 1;
  if(m_cmdLine.debug) m_out << "DEBUG: Calling Acs::queryVolume()" << std::endl;
  VOLID volId[1] = {m_cmdLine.volId};
  const STATUS queryStatus = m_acs.queryVolume(mountSeqNumber, volId, 1);
  if(m_cmdLine.debug) m_out << "DEBUG: Acs::queryVolume() returned " <<
    acs_status(queryStatus) << std::endl;
  if(STATUS_SUCCESS != queryStatus) {
    castor::exception::QueryVolumeFailed ex;
    ex.getMessage() << "Failed to " << action << ": " <<
      acs_status(queryStatus);
    throw ex;
  }

  // Get all responses until RT_FINAL
  ALIGNED_BYTES responseBuf[MAX_MESSAGE_SIZE / sizeof(ALIGNED_BYTES)];
  SEQ_NO responseSeqNumber = (SEQ_NO)0;
  REQ_ID reqId = (REQ_ID)0;
  ACS_RESPONSE_TYPE responseType = RT_NONE;
  int elapsedMountTime = 0;
  do {
    if(m_cmdLine.debug) m_out << "DEBUG: Calling Acs::response()" << std::endl;
    const int remainingTime = m_cmdLine.timeout - elapsedMountTime;
    const int responseTimeout = remainingTime > m_cmdLine.queryInterval ?
      m_cmdLine.queryInterval : remainingTime;
    const time_t startTime = time(NULL);
    const STATUS responseStatus = m_acs.response(responseTimeout,
      responseSeqNumber, reqId, responseType, responseBuf);
    elapsedMountTime += time(NULL) - startTime;
    if(STATUS_SUCCESS != responseStatus && STATUS_PENDING != responseStatus) {
      castor::exception::QueryVolumeFailed ex;
      ex.getMessage() << "Failed to " << action.str() <<
        ": Failed to request library response: " << acs_status(responseStatus);
      throw ex;
    }
    if(m_cmdLine.debug && STATUS_SUCCESS == responseStatus &&
      RT_ACKNOWLEDGE == responseType) {
      m_out << "DEBUG: Received RT_ACKNOWLEDGE: responseSeqNumber=" <<
        responseSeqNumber << " reqId=" << reqId << std::endl;
    }
    if(m_cmdLine.debug) m_out << "DEBUG: Acs::response() returned " <<
      acs_status(responseStatus) << std::endl;

    if(elapsedMountTime >= m_cmdLine.timeout) {
      castor::exception::QueryVolumeFailed ex;
      ex.getMessage() << "Failed to " << action.str() << ": Timed out:"
        " mountTimeout=" << m_cmdLine.timeout << " seconds";
      throw(ex);
    }
  } while(RT_FINAL != responseType);
  if(m_cmdLine.debug) m_out << "DEBUG: Received RT_FINAL: responseSeqNumber=" <<
    responseSeqNumber << " reqId=" << reqId << std::endl;

  if(mountSeqNumber != responseSeqNumber) {
    castor::exception::QueryVolumeFailed ex;
    ex.getMessage() << "Failed to " << action.str() << 
      ": Invalid RT_FINAL message: Sequence number mismatch: mountSeqNumber=" 
      << mountSeqNumber << " responseSeqNumber=" << responseSeqNumber;
    throw(ex);
  }

  const ACS_QUERY_VOL_RESPONSE *const response =
    (ACS_QUERY_VOL_RESPONSE *)responseBuf;

  if(STATUS_SUCCESS != response->query_vol_status) {
    castor::exception::QueryVolumeFailed ex;
    ex.getMessage() << "Failed to " << action.str() << ": " <<
      acs_status(response->query_vol_status);
    throw(ex);
  }

  if((unsigned short)1 != response->count) {
    castor::exception::QueryVolumeFailed ex;
    ex.getMessage() << "Failed to " << action.str() <<
      ": Incorrect number of volume statuses"
      ": expected=1 actual=" << response->count;
    throw ex;
  }

  // count is 1 so it is safe to make a reference to the single volume status
  const QU_VOL_STATUS &volStatus = response->vol_status[0];

  if(strcmp(m_cmdLine.volId.external_label, volStatus.vol_id.external_label)) {
    castor::exception::QueryVolumeFailed ex;
    ex.getMessage() << "Failed to " << action.str() <<
      ": Volume identifier mismatch"
      ": requestVID=" << m_cmdLine.volId.external_label <<
      " statusVID=" << volStatus.vol_id.external_label;
    throw ex;
  }

  return volStatus;
}

//------------------------------------------------------------------------------
// writeVolumeStatus
//------------------------------------------------------------------------------
void castor::tape::mediachanger::QueryVolumeAcsCmd::writeVolumeStatus(
  const QU_VOL_STATUS &s) throw() {
  m_out << "Volume identifier: " << s.vol_id.external_label << std::endl;
  m_out << "Media type: " << (int)s.media_type << std::endl;

  switch(s.location_type) {
  case LOCATION_CELL: {
    m_out << "Location type: cell" << std::endl;
    const CELLID &cellId = s.location.cell_id;
    m_out << "ACS: " << (int)cellId.panel_id.lsm_id.acs << std::endl;
    m_out << "LSM: " << (int)cellId.panel_id.lsm_id.lsm << std::endl;
    m_out << "Panel: " << (int)cellId.panel_id.panel << std::endl;
    m_out << "Row: " << (int)cellId.row << std::endl;
    m_out << "Column: " << (int)cellId.col << std::endl;
    break;
  }
  case LOCATION_DRIVE: {
    m_out << "Location type: drive" << std::endl;
    const DRIVEID &driveId = s.location.drive_id;
    m_out << "ACS: " << (int)driveId.panel_id.lsm_id.acs << std::endl;
    m_out << "LSM: " << (int)driveId.panel_id.lsm_id.lsm << std::endl;
    m_out << "Panel: " << (int)driveId.panel_id.panel << std::endl;
    m_out << "Drive: " << (int)driveId.drive << std::endl;
    break;
  }
  default:
    m_out << "Location type: UNKNOWN" << std::endl;
    break;
  }

  m_out << "Status: " << acs_status(s.status) << std::endl;
}
