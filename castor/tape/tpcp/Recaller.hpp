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

#include "castor/IObject.hpp"
#include "castor/io/AbstractSocket.hpp"
#include "castor/tape/tpcp/ActionHandler.hpp"
#include "castor/tape/tpcp/TapeFseqRangeListSequence.hpp"

namespace castor {
namespace tape   {
namespace tpcp   {

/**
 * Responsible for carrying out the action of recalling files from tape.
 */
class Recaller : public ActionHandler {
public:

  /**
   * Constructor calling the ActionHandler constructor.
   *
   * See the header file of castor::tape::tpcp::ActionHandler for a description
   * of the parameters.
   */
  Recaller(const bool debug, TapeFseqRangeList &tapeFseqRanges,
    FilenameList &filenames, const vmgr_tape_info &vmgrTapeInfo,
    const char *const dgn, const int volReqId,
    castor::io::ServerSocket &callbackSocket) throw();

  /**
   * Destructor.
   */
  virtual ~Recaller();

  /**
   * See the header file of castor::tape::tpcp::ActionHandler for this method's
   * documentation.
   */
  void run() throw(castor::exception::Exception);


private:

  /**
   * The sequence of tape file sequence numbers to be processed.
   */
  TapeFseqRangeListSequence m_tapeFseqSequence;

  /**
   * The number of successfully transfered files.
   */
  uint64_t m_nbRecalledFiles;

  /**
   * Structure used to remember a file that is currently being transfered.
   */
  struct FileTransfer {

    /**
     * The tape file sequence number.
     */
    uint32_t tapeFseq;

    /**
     * The RFIO filename.
     */
    std::string filename;
  };

  /**
   * Data type for a map of file transaction IDs to files currently being
   * transfered.
   */
  typedef std::map<uint64_t, FileTransfer> FileTransferMap;

  /**
   * Map of file transaction IDs to files currently being transfered.
   */
  FileTransferMap m_pendingFileTransfers;

  /**
   * FileToRecallRequest message handler.
   *
   * @param obj  The aggregator message to be processed.
   * @param sock The socket on which to reply to the aggregator.
   * @return     True if there is more work to be done else false.
   */
  bool handleFileToRecallRequest(castor::IObject *obj,
    castor::io::AbstractSocket &sock) throw(castor::exception::Exception);

  /**
   * FileRecalledNotification message handler.
   *
   * @param obj  The aggregator message to be processed.
   * @param sock The socket on which to reply to the aggregator.
   * @return     True if there is more work to be done else false.
   */
  bool handleFileRecalledNotification(castor::IObject *obj,
    castor::io::AbstractSocket &sock) throw(castor::exception::Exception);

  /**
   * EndNotification message handler.
   *
   * @param obj  The aggregator message to be processed.
   * @param sock The socket on which to reply to the aggregator.
   * @return     True if there is more work to be done else false.
   */
  bool handleEndNotification(castor::IObject *obj,
    castor::io::AbstractSocket &sock) throw(castor::exception::Exception);

  /**
   * EndNotificationErrorReport message handler.
   *
   * @param obj  The aggregator message to be processed.
   * @param sock The socket on which to reply to the aggregator.
   * @return     True if there is more work to be done else false.
   */
  bool handleEndNotificationErrorReport(castor::IObject *obj,
    castor::io::AbstractSocket &sock) throw(castor::exception::Exception);

}; // class Recaller

} // namespace tpcp
} // namespace tape
} // namespace castor


#endif // CASTOR_TAPE_TPCP_RECALLER_HPP
