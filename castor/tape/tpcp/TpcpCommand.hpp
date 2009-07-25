/******************************************************************************
 *                 castor/tape/tpcp/Tpcp.hpp
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

#ifndef CASTOR_TAPE_TPCP_TPCPCOMMAND_HPP
#define CASTOR_TAPE_TPCP_TPCPCOMMAND_HPP 1

#include "castor/BaseObject.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/exception/InvalidArgument.hpp"
#include "castor/io/ServerSocket.hpp"
#include "castor/tape/tpcp/Action.hpp"
#include "castor/tape/tpcp/FilenameList.hpp"
#include "castor/tape/tpcp/ParsedCommandLine.hpp"
#include "castor/tape/utils/utils.hpp"
#include "h/vmgr_api.h"

#include <iostream>
#include <list>
#include <map>
#include <stdint.h>
#include <stdlib.h>

namespace castor {
namespace tape   {
namespace tpcp   {

/**
 * This class carries out the following steps:
 * <ul>
 * <li>Delegates to a sub-class the parsing of the command-line.
 * <li>Gets the DGN of the tape to be used from the VMGR.
 * <li>Creates the aggregator callback socket.
 * <li>Sends the request for a drive to the VDQM.
 * <li>Waits for the callback from the aggregator on the chosen tape server.
 * <li>Receives and replies to the volume request from the aggregator.
 * <li>Delegates the tape transfer action (DUMP, READ, WRITE or VERIFY) to a
 * sub-class.
 * </ul>
 */
class TpcpCommand : public castor::BaseObject {
public:

  /**
   * Constructor.
   */
  TpcpCommand() throw();

  /**
   * Destructor.
   */
  virtual ~TpcpCommand() throw();

  /**
   * The entry function of the tpcp command.
   *
   * @param programName The program name.
   * @param argc        The number of command-line arguments including the
   *                    process name.
   * @param argv        The command-line arguments including the process name
   *                    (argv[0]).
   */
  int main(const char *const programName, const int argc, char **argv) throw();


protected:

  /**
   * To be implemented by sub-classes.
   *
   * Writes the command-line usage message of tpcp onto the specified output
   * stream.
   *
   * @param os Output stream to be written to.
   * @param programName The program name to be used in the message.
   */
  virtual void usage(std::ostream &os, const char *const programName) throw()
    = 0;

  /**
   * To be implemented by sub-classes.
   *
   * Parses the specified command-line arguments.
   *
   * @param argc Argument count from the executable's entry function: main().
   * @param argv Argument vector from the executable's entry function: main().
   */
  virtual void parseCommandLine(const int argc, char **argv)
    throw(castor::exception::Exception) = 0;

  /**
   * To be implemented by sub-classes.
   *
   * Performs the tape copy whether it be DUMP, READ, WRITE or VERIFY.
   */
  virtual void performTransfer() throw(castor::exception::Exception) = 0;

  /**
   * The results of parsing the command-line.
   */
  ParsedCommandLine m_cmdLine;

  /**
   * Vmgr error buffer.
   */
  static char vmgr_error_buffer[VMGRERRORBUFLEN];

  /**
   * The list of RFIO filenames to be processed by the request handlers.
   */
  FilenameList m_filenames;

  /**
   * Iterator pointing to the next RFIO filename;
   */
  FilenameList::const_iterator m_filenameItor;

  /**
   * Tape information retrieved from the VMGR about the tape to be used.
   */
  vmgr_tape_info m_vmgrTapeInfo;

  /**
   * The DGN of the tape to be used.
   */
  char m_dgn[CA_MAXDGNLEN + 1];

  /**
   * TCP/IP aggregator callback socket.
   */
  castor::io::ServerSocket m_callbackSock;

  /**
   * The volume request ID returned by the VDQM as a result of requesting a
   * drive.
   */
  int32_t m_volReqId;

  /**
   * The next file transaction ID.
   */
  uint64_t m_fileTransactionId;

  /**
   * Returns the port on which the server will listen for connections from the
   * VDQM.
   */
  int getVdqmListenPort() throw(castor::exception::Exception);

  /**
   * Calculate the minimum number of files specified in the tape file
   * ranges provided as a command-line parameter.
   *
   * @return The minimum number of files.
   */
  unsigned int calculateMinNbOfFiles() throw (castor::exception::Exception);

  /**
   * Retrieves information about the specified tape from the VMGR.
   *
   * This method is basically a C++ wrapper around the C VMGR function
   * vmgr_querytape.  This method converts the return value of -1 and the
   * serrno to an exception in the case of an error.
   &
   * @param vid The tape for which the information should be retrieved.
   * @param side The tape side.
   */
  void vmgrQueryTape(char (&vid)[CA_MAXVIDLEN+1], const int side)
    throw (castor::exception::Exception);

  /**
   * Creates, binds and sets to listening the callback socket to be used for
   * callbacks from the aggregator daemon.
   */
  void setupCallbackSock() throw(castor::exception::Exception);

  /**
   * Request a drive from the VDQM to mount the specified tape for the
   * specified access mode (read or write).
   *
   * @param mode   The access mode, either WRITE_DISABLE or WRITE_ENABLE.
   * @param server If not NULL then this parameter specifies the tape server to
   * be used, therefore overriding drive scheduling of the VDQM.
   */
  void requestDriveFromVdqm(const int mode, char *const server)
    throw(castor::exception::Exception);

  /**
   * Registers the specified Aggregator message handler member function.
   *
   * @param messageType The type of Aggregator message.
   * @param func        The member function to handle the Aggregator message.
   * @param obj         The object to handler the Aggregator message.
   */
  template<class T> void registerMsgHandler(
    const int messageType,
    bool (T::*func)(castor::IObject *, castor::io::AbstractSocket &),
    T *obj) throw() {

    m_msgHandlers[messageType] = new MsgHandler<T>(func, obj);
  }

  /**
   * Waits for and accepts an incoming aggregator connection, reads in the
   * aggregator message and then dispatches it to appropriate message handler
   * member function.
   *
   * @return True if there is more work to be done, else false.
   */
  bool dispatchMessage() throw(castor::exception::Exception);

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

  /**
   * Acknowledges the end of the session to the aggregator.
   */
  void acknowledgeEndOfSession() throw(castor::exception::Exception);

  /**
   * Convenience method for sending EndNotificationErrorReport messages to the
   * aggregator.
   *
   * Note that this method intentionally does not throw any exceptions.  The
   * method tries to send an EndNotificationErrorReport messages to the
   * aggregator and fails silently in the case it cannot.
   *
   * @param errorCode    The error code.
   * @param errorMessage The error message.
   * @param sock         The socket on which to reply to the aggregator.
   */
  void sendEndNotificationErrorReport(const int errorCode,
    const std::string &errorMessage, castor::io::AbstractSocket &sock) throw();

  /**
   * Convenience method that casts the specified CASTOR framework object into
   * the specified pointer to Gateway message.
   *
   * If the cast fails then an EndNotificationErrorReport is sent to the
   * Gateway and an appropriate exception is thrown.
   *
   * @param obj  The CASTOR framework object.
   * @param msg  Out parameter. The pointer to the Gateway massage that will be
   *             set by this method.
   * @param sock The socket on which to reply to the aggregator with an
   *             EndNotificationErrorReport message if the cast fails.
   */
  template<class T> void castMessage(castor::IObject *obj, T *&msg,
    castor::io::AbstractSocket &sock) throw() {
    msg = dynamic_cast<T*>(obj);

    if(msg == NULL) {
      std::stringstream oss;

      oss <<
        "Unexpected object type" <<
        ": Actual=" << utils::objectTypeToString(obj->type()) <<
        " Expected=" << utils::objectTypeToString(T().type());

      sendEndNotificationErrorReport(SEINTERNAL, oss.str(), sock);

      TAPE_THROW_EX(castor::exception::Internal, oss.str());
    }
  }


private:

  /**
   * An abstract functor to handle incomming Aggregator messages.
   */
  class AbstractMsgHandler {
  public:

    /**
     * operator().
     *
     * @param obj The aggregator message to be processed.
     * @param sock The socket on which to reply to the aggregator.
     * @return True if there is more work to be done else false.
     */
    virtual bool operator()(castor::IObject *obj,
      castor::io::AbstractSocket &sock) const
      throw(castor::exception::Exception) = 0;

    /**
     * Destructor.
     */
    virtual ~AbstractMsgHandler() {
      // DO nothing
    }
  };

  /**
   * Concrete Aggregator message handler template functor.
   */
  template<class T> class MsgHandler : public AbstractMsgHandler {
  public:

    /**
     * Constructor.
     */
    MsgHandler(bool (T::*const func)(castor::IObject *obj,
        castor::io::AbstractSocket &sock), T *const obj) :
      m_func(func), m_obj(obj) {
      // Do nothing
    }

    /**
     * operator().
     *
     * @param obj The aggregator message to be processed.
     * @param sock The socket on which to reply to the aggregator.
     * @return True if there is more work to be done else false.
     */
    virtual bool operator()(castor::IObject *obj,
      castor::io::AbstractSocket &sock) const
      throw(castor::exception::Exception) {

      return (m_obj->*m_func)(obj, sock);
    }

    /**
     * destructor.
     */
    virtual ~MsgHandler() {
      // Do nothing
    }

    /**
     * The member function of class T to be called by operator().
     */
    bool (T::*const m_func)(castor::IObject *obj,
      castor::io::AbstractSocket &sock);

    /**
     * The object on which the member function shall be called by operator().
     */
    T *const m_obj;
  };  // template<class T> class MsgHandler

  /**
   * Map from message type (int) to pointer message handler
   * (AbstractMsgHandler *).
   *
   * The destructor of this map is responsible for and will deallocate the
   * MsgHandlers it points to.
   */
  class MsgHandlerMap : public std::map<int, AbstractMsgHandler *> {
  public:

    /**
     * Destructor.
     *
     * Deallocates the MsgHandlers pointed to by this map.
     */
    ~MsgHandlerMap() {
      for(iterator itor=begin(); itor!=end(); itor++) {
        // Delete the MsgHandler
        delete(itor->second);
      }
    }
  }; // MsgHandlerMap

  /**
   * Map from message type (uint32_t) to pointer message handler
   * (AbstractMsgHandler *).
   *
   * The destructor of this map is responsible for and will deallocate the
   * MsgHandlers it points to.
   */
  MsgHandlerMap m_msgHandlers;

}; // class TpcpCommand

} // namespace tpcp
} // namespace tape
} // namespace castor


#endif // CASTOR_TAPE_TPCP_TPCPCOMMAND_HPP
