/******************************************************************************
 *                 castor/tape/tpcp/Recaller.cpp
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
 
#include "castor/exception/Internal.hpp"
#include "castor/tape/net/net.hpp"
#include "castor/tape/tapegateway/FileToRecallRequest.hpp"
#include "castor/tape/tapegateway/NoMoreFiles.hpp"
#include "castor/tape/tapegateway/NotificationAcknowledge.hpp"
#include "castor/tape/tapegateway/Volume.hpp"
#include "castor/tape/tpcp/Constants.hpp"
#include "castor/tape/tpcp/Recaller.hpp"
#include "castor/tape/utils/utils.hpp"

#include <errno.h>


//------------------------------------------------------------------------------
// run
//------------------------------------------------------------------------------
void castor::tape::tpcp::Recaller::run(const bool debug,
  TapeFseqRangeList &tapeFseqRanges, FilenameList &filenames,
  const vmgr_tape_info &vmgrTapeInfo, const char *dgn, const int volReqId,
  castor::io::ServerSocket &callbackSocket)
  throw(castor::exception::Exception) {

  // Process the tape file sequence numbers
  const ProcessTapeFseqsResult result = processTapeFseqs(debug, tapeFseqRanges,
    filenames, volReqId, callbackSocket);

  // Tell the aggregator that there are no more files to be processed
  tellAggregatorNoMoreFiles(debug, tapeFseqRanges, filenames, volReqId,
    callbackSocket);

  // Acknowledge the end of the session
  acknowledgeEndOfSession(debug, volReqId, callbackSocket);

  // Display the result
  {
    std::ostream &os = std::cout;

    os << "Recall result = ";

    switch(result) {
    case RESULT_SUCCESS:
      os << "Success";
      break;

    case RESULT_REACHED_END_OF_TAPE:
      os << "Reached end of tape";
      break;

    case RESULT_MORE_TFSEQS_THAN_FILENAMES:
      os << "There were more tape file sequence numbers than RFIO filenames";
      break;

    case RESULT_MORE_FILENAMES_THAN_TFSEQS:
      os << "There were more RFIO filename than  tape file sequence numbers";
      break;

    default:
      {
        TAPE_THROW_EX(castor::exception::Internal,
          ": Unknown result type: Result=" << result);
      }
    }

    os << std::endl;
  }
}


//------------------------------------------------------------------------------
// processTapeFseqs
//------------------------------------------------------------------------------
castor::tape::tpcp::Recaller::ProcessTapeFseqsResult
  castor::tape::tpcp::Recaller::processTapeFseqs(bool debug,
  castor::tape::tpcp::TapeFseqRangeList &tapeFseqRanges,
  castor::tape::tpcp::FilenameList &filenames, const int volReqId,
  castor::io::ServerSocket &callbackSocket)
  throw(castor::exception::Exception) {

  FilenameList::iterator filenamesItor=filenames.begin();

  // For each range
  for(TapeFseqRangeList::iterator itor=tapeFseqRanges.begin();
    itor!=tapeFseqRanges.end(); itor++) {

    TapeFseqRange &range = *itor;

    // "seq != range.upper" must be used as opposed to "seq <= range.upper"
    // in order to support {m-END} where END is in fact 0
    for(uint32_t tapeFseq=range.lower; tapeFseq != range.upper; tapeFseq++) {

      // If there are no more RFIO filenames
      if(filenamesItor == filenames.end()) {
        return RESULT_MORE_TFSEQS_THAN_FILENAMES;
      }

      // Get the next RFIO filename
      std::string &filename = *(filenamesItor++);

      try {
        processFile(debug, tapeFseq, filename, volReqId, callbackSocket);
      } catch(castor::exception::Exception &ex) {
        castor::exception::Exception ex2(ex.code());

        ex2.getMessage()
          << "Failed to process file"
             ": Filename=\"" << filename
          << ": " << ex.getMessage().str();

        throw ex2;
      }
    }
  }

  if(filenamesItor == filenames.end()) {
    return RESULT_SUCCESS;
  } else {
    return RESULT_MORE_FILENAMES_THAN_TFSEQS;
  }
}


//------------------------------------------------------------------------------
// processFile
//------------------------------------------------------------------------------
void castor::tape::tpcp::Recaller::processFile(const bool debug,
  const uint32_t tapeFseq, const std::string &filename, const int volReqId,
  castor::io::ServerSocket &callbackSocket)
  throw(castor::exception::Exception) {
}


//------------------------------------------------------------------------------
// tellAggregatorNoMoreFiles
//------------------------------------------------------------------------------
void castor::tape::tpcp::Recaller::tellAggregatorNoMoreFiles(const bool debug,
  TapeFseqRangeList &tapeFseqRanges, FilenameList &filenames,
  const int volReqId, castor::io::ServerSocket &callbackSocket)
  throw(castor::exception::Exception) {

  // Socket file descriptor for a callback connection from the aggregator
  int connectionSocketFd = 0;

  // Wait for a callback connection from the aggregator
  {
    bool waitForCallback    = true;
    while(waitForCallback) {
      try {
        connectionSocketFd = net::acceptConnection(callbackSocket.socket(),
          WAITCALLBACKTIMEOUT);

        waitForCallback = false;
      } catch(castor::exception::TimeOut &tx) {
        std::cout << "Waited " << WAITCALLBACKTIMEOUT << "seconds for a "
        "callback connection from the tape server." << std::endl
        << "Continuing to wait." <<  std::endl;
      }
    }
  }

  // If debug, then display a textual description of the aggregator
  // callback connection
  if(debug) {
    std::ostream &os = std::cout;

    utils::writeBanner(os, "Recaller: Aggregator connection");
    os << std::endl;
    net::writeSocketDescription(os, connectionSocketFd);
    os << std::endl;
    os << std::endl;
    os << std::endl;
  }

  // Wrap the connection socket descriptor in a CASTOR framework socket in
  // order to get access to the framework marshalling and un-marshalling
  // methods
  castor::io::AbstractTCPSocket callbackConnectionSocket(connectionSocketFd);

  // Read in the object sent by the aggregator
  std::auto_ptr<castor::IObject> obj(callbackConnectionSocket.readObject());

  // Pointer to the received object with the object's type
  tapegateway::FileToRecallRequest *fileToRecallRequest = NULL;

  // Cast the object to its type
  fileToRecallRequest =
    dynamic_cast<tapegateway::FileToRecallRequest*>(obj.get());
  if(fileToRecallRequest == NULL) {
    castor::exception::InvalidArgument ex;

    ex.getMessage()
     << "Received the wrong type of object from the aggregator"
     << ": Actual=" << utils::objectTypeToString(obj->type())
     << " Expected=FileToRecallRequest";

    throw ex;
  }

  // If debug, then display reception of the FileToRecallRequest message
  if(debug) {
    std::ostream &os = std::cout;

    utils::writeBanner(os, "Recaller: Received FileToRecallRequest from "
      "aggregator");
    os << std::endl;
    os << std::endl;
  }

  // Create the NoMoreFiles message for the aggregator
  castor::tape::tapegateway::NoMoreFiles noMore;
  noMore.setTransactionId(volReqId);

  // Send the volume message to the aggregator
  callbackConnectionSocket.sendObject(noMore);

  // Close the connection to the aggregator
  callbackConnectionSocket.close();

  // If debug, then display sending of the Volume message
  if(debug) {
    std::ostream &os = std::cout;

    utils::writeBanner(os, "Sent NoMoreFiles to aggregator");
    os << std::endl;
    os << std::endl;
  }
}
