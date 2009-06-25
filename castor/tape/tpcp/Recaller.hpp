/******************************************************************************
 *                 castor/tape/tpcp/Recaller.hpp
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

#ifndef CASTOR_TAPE_TPCP_RECALLER_HPP
#define CASTOR_TAPE_TPCP_RECALLER_HPP 1

#include "castor/tape/tpcp/ActionHandler.hpp"

namespace castor {
namespace tape   {
namespace tpcp   {

/**
 * Responsible for carrying out the action of recalling files from tape.
 */
class Recaller : public ActionHandler {
public:

  /**
   * See the header file of castor::tape::tpcp::ActionHandler for this method's
   * documentation.
   */
  void run(const bool debug, TapeFseqRangeList &tapeFseqRanges,
    FilenameList &filenames, const vmgr_tape_info &vmgrTapeInfo,
    const char *dgn, const int volReqId,
    castor::io::ServerSocket &callbackSocket)
    throw(castor::exception::Exception);


private:

  /**
   * The possible results of calling processTapeFseqs.
   */
  enum ProcessTapeFseqsResult {
    RESULT_SUCCESS,
    RESULT_REACHED_END_OF_TAPE,
    RESULT_MORE_TFSEQS_THAN_FILENAMES,
    RESULT_MORE_FILENAMES_THAN_TFSEQS
  };

  /**
   * Processes the specified list of tape file sequence ranges.
   *
   * @param debug True if debug messages should be displayed.
   * @param tapeFseqRanges The list of tape file sequence ranges to be
   * processed.
   * @param filenames The list of RFIO filenames to be processed.
   * @param volReqId The volume request ID returned by the VDQM in response to
   * to the request for a drive.
   * @param callbackSocket The aggregator callback socket.
   */
  ProcessTapeFseqsResult processTapeFseqs(const bool debug,
    TapeFseqRangeList &tapeFseqRanges, FilenameList &filenames,
    const int volReqId, castor::io::ServerSocket &callbackSocket)
    throw(castor::exception::Exception);

  /**
   * Processes the specified file.
   *
   * @param debug True if debug messages should be displayed.
   * @param tapeFseq The tape file sequence number to be processed.
   * @param filename The RFIO filename to be processed.
   * @param volReqId The volume request ID returned by the VDQM in response to
   * to the request for a drive.
   * @param callbackSocket The aggregator callback socket.
   */
  void processFile(const bool debug, const uint32_t tapeFseq,
    const std::string &filename, const int volReqId,
    castor::io::ServerSocket &callbackSocket)
    throw(castor::exception::Exception);

  /**
   * Tells the aggregator there are no more files to be processed.
   *
   * @param debug True if debug messages should be displayed.
   * @param tapeFseqRanges The list of tape file sequence ranges to be
   * processed.
   * @param filenames The list of RFIO filenames to be processed.
   * @param volReqId The volume request ID returned by the VDQM in response to
   * to the request for a drive.
   * @param callbackSocket The aggregator callback socket.
   */
  void tellAggregatorNoMoreFiles(bool debug, TapeFseqRangeList &tapeFseqRanges,
    FilenameList &filenames, const int volReqId,
    castor::io::ServerSocket &callbackSocket)
    throw(castor::exception::Exception);

}; // class Recaller

} // namespace tpcp
} // namespace tape
} // namespace castor


#endif // CASTOR_TAPE_TPCP_RECALLER_HPP
