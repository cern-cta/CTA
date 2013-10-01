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

  m_dbg << "query = " << m_cmdLine.queryInterval << std::endl;
  m_dbg << "timeout = " << m_cmdLine.timeout << std::endl;
  m_dbg << "VID = " << m_cmdLine.volId.external_label << std::endl;

  try {
    const QU_VOL_STATUS volStatus = syncQueryVolume();
    writeVolumeStatus(m_out, volStatus);
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
      cmdLine.debug = true;
      break;
    case 'h':
      cmdLine.help = true;
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
  const SEQ_NO requestSeqNumber = 1;
  ALIGNED_BYTES buf[MAX_MESSAGE_SIZE / sizeof(ALIGNED_BYTES)];

  sendQueryVolumeRequest(requestSeqNumber);
  requestResponsesUntilFinal(requestSeqNumber, buf);
  return processQueryResponse(buf);
}

//------------------------------------------------------------------------------
// sendQueryVolumeRequest
//------------------------------------------------------------------------------
void castor::tape::mediachanger::QueryVolumeAcsCmd::sendQueryVolumeRequest(
  const SEQ_NO seqNumber) throw (castor::exception::QueryVolumeFailed) {
  VOLID volId[1] = {m_cmdLine.volId};

  m_dbg << "Calling Acs::queryVolume()" << std::endl;
  const STATUS s = m_acs.queryVolume(seqNumber, volId, 1);
  m_dbg << "Acs::queryVolume() returned " << acs_status(s) << std::endl;

  if(STATUS_SUCCESS != s) {
    castor::exception::QueryVolumeFailed ex;
    ex.getMessage() << "Failed to send query request for volume " <<
      m_cmdLine.volId.external_label << ": " << acs_status(s);
    throw ex;
  }
}

//------------------------------------------------------------------------------
// requestResponsesUntilFinal
//------------------------------------------------------------------------------
void castor::tape::mediachanger::QueryVolumeAcsCmd::requestResponsesUntilFinal(
  const SEQ_NO requestSeqNumber,
  ALIGNED_BYTES (&buf)[MAX_MESSAGE_SIZE / sizeof(ALIGNED_BYTES)])
  throw (castor::exception::QueryVolumeFailed) {
  ACS_RESPONSE_TYPE responseType = RT_NONE;
  int elapsedTime = 0;
  do {
    const int remainingTime = m_cmdLine.timeout - elapsedTime;
    const int responseTimeout = remainingTime > m_cmdLine.queryInterval ?
      m_cmdLine.queryInterval : remainingTime;

    const time_t startTime = time(NULL);
    responseType = requestResponse(responseTimeout, requestSeqNumber, buf);
    elapsedTime += time(NULL) - startTime;
  
    if(RT_ACKNOWLEDGE == responseType) {
      m_dbg << "Received RT_ACKNOWLEDGE" << std::endl;
    }
  
    if(elapsedTime >= m_cmdLine.timeout) {
      castor::exception::QueryVolumeFailed ex;
      ex.getMessage() << "Timed out after " << m_cmdLine.timeout << " seconds";
      throw(ex);
    }
  } while(RT_FINAL != responseType);

  m_dbg << "Received RT_FINAL" << std::endl;
}

//------------------------------------------------------------------------------
// requestResponse
//------------------------------------------------------------------------------
ACS_RESPONSE_TYPE castor::tape::mediachanger::QueryVolumeAcsCmd::
  requestResponse(const int timeout, const SEQ_NO requestSeqNumber,
  ALIGNED_BYTES (&buf)[MAX_MESSAGE_SIZE / sizeof(ALIGNED_BYTES)])
  throw(castor::exception::QueryVolumeFailed) {
  SEQ_NO responseSeqNumber = 0;
  REQ_ID reqId = (REQ_ID)0;
  ACS_RESPONSE_TYPE responseType = RT_NONE;

  m_dbg << "Calling Acs::response()" << std::endl;
  const STATUS s = m_acs.response(timeout, responseSeqNumber, reqId,
    responseType, buf);
  m_dbg << "Acs::response() returned " << acs_status(s) << std::endl;

  switch(s) {
  case STATUS_SUCCESS:
    checkResponseSeqNumber(requestSeqNumber, responseSeqNumber);
    return responseType;
  case STATUS_PENDING:
    return RT_NONE;
  default:
    castor::exception::QueryVolumeFailed ex;
    ex.getMessage() << "Failed to request response: " << acs_status(s);
    throw ex;
  }
}

//------------------------------------------------------------------------------
// checkSeqNumber
//------------------------------------------------------------------------------
void castor::tape::mediachanger::QueryVolumeAcsCmd::checkResponseSeqNumber(
  const SEQ_NO requestSeqNumber, const SEQ_NO responseSeqNumber)
  throw(castor::exception::QueryVolumeFailed) {
  if(requestSeqNumber != responseSeqNumber) {
    castor::exception::QueryVolumeFailed ex;
    ex.getMessage() <<  ": Sequence number mismatch: requestSeqNumber="
      << requestSeqNumber << " responseSeqNumber=" << responseSeqNumber;
    throw(ex);
  }
}

//------------------------------------------------------------------------------
// processQueryResponse
//------------------------------------------------------------------------------
QU_VOL_STATUS castor::tape::mediachanger::QueryVolumeAcsCmd::
  processQueryResponse(
  ALIGNED_BYTES (&buf)[MAX_MESSAGE_SIZE / sizeof(ALIGNED_BYTES)])
  throw(castor::exception::QueryVolumeFailed) {

  const ACS_QUERY_VOL_RESPONSE *const msg = (ACS_QUERY_VOL_RESPONSE *)buf;

  if(STATUS_SUCCESS != msg->query_vol_status) {
    castor::exception::QueryVolumeFailed ex;
    ex.getMessage() << "Status of query response is not success: " <<
      acs_status(msg->query_vol_status);
    throw(ex);
  }

  if((unsigned short)1 != msg->count) {
    castor::exception::QueryVolumeFailed ex;
    ex.getMessage() << "Query response does not contain a single volume: count="
      << msg->count;
    throw ex;
  }

  // count is 1 so it is safe to make a reference to the single volume status
  const QU_VOL_STATUS &volStatus = msg->vol_status[0];

  if(strcmp(m_cmdLine.volId.external_label, volStatus.vol_id.external_label)) {
    castor::exception::QueryVolumeFailed ex;
    ex.getMessage() <<
      "Volume identifier of query response does not match that of request"
      ": requestVID=" << m_cmdLine.volId.external_label <<
      " responseVID=" << volStatus.vol_id.external_label;
    throw ex;
  }

  return volStatus;
}

//------------------------------------------------------------------------------
// writeVolumeStatus
//------------------------------------------------------------------------------
void castor::tape::mediachanger::QueryVolumeAcsCmd::writeVolumeStatus(
  std::ostream &os, const QU_VOL_STATUS &s) throw() {
  os << "Volume identifier: " << s.vol_id.external_label << std::endl;
  os << "Media type (media_types.dat): " << (int)s.media_type << std::endl;

  switch(s.location_type) {
  case LOCATION_CELL: {
    os << "Location type: cell" << std::endl;
    const CELLID &cellId = s.location.cell_id;
    os << "ACS: " << (int)cellId.panel_id.lsm_id.acs << std::endl;
    os << "LSM: " << (int)cellId.panel_id.lsm_id.lsm << std::endl;
    os << "Panel: " << (int)cellId.panel_id.panel << std::endl;
    os << "Row: " << (int)cellId.row << std::endl;
    os << "Column: " << (int)cellId.col << std::endl;
    break;
  }
  case LOCATION_DRIVE: {
    os << "Location type: drive" << std::endl;
    const DRIVEID &driveId = s.location.drive_id;
    os << "ACS: " << (int)driveId.panel_id.lsm_id.acs << std::endl;
    os << "LSM: " << (int)driveId.panel_id.lsm_id.lsm << std::endl;
    os << "Panel: " << (int)driveId.panel_id.panel << std::endl;
    os << "Drive: " << (int)driveId.drive << std::endl;
    break;
  }
  default:
    os << "Location type: UNKNOWN" << std::endl;
    break;
  }

  os << "Status: " << acs_status(s.status) << std::endl;
}
