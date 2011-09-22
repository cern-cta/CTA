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
#include "castor/exception/PermissionDenied.hpp"
#include "castor/io/ServerSocket.hpp"
#include "castor/tape/tapegateway/FileErrorReportStruct.hpp"
#include "castor/tape/tpcp/Action.hpp"
#include "castor/tape/tpcp/FilenameList.hpp"
#include "castor/tape/tpcp/ParsedCommandLine.hpp"
#include "castor/tape/utils/utils.hpp"
#include "h/Castor_limits.h"
#include "h/vmgr_api.h"

#include <iostream>
#include <list>
#include <map>
#include <signal.h>
#include <stdint.h>
#include <stdlib.h>
#include <unistd.h>

#include <sys/types.h>
#include "h/rfio_api.h"

namespace castor {
namespace tape   {
namespace tpcp   {

/**
 * This class carries out the following steps:
 * <ul>
 * <li>Delegates to a sub-class the parsing of the command-line.
 * <li>Gets the DGN of the tape to be used from the VMGR.
 * <li>Creates the tape-bridge callback socket.
 * <li>Sends the request for a drive to the VDQM.
 * <li>Waits for the callback from the tape-bridge on the chosen tape server.
 * <li>Receives and replies to the volume request from the tape-bridge.
 * <li>Delegates the tape transfer action (DUMP, READ or WRITE) to a
 *     sub-class.
 * </ul>
 *
 * Sub-classes must do the following:
 * <ul>
 * <li>Implement the usage() method.
 * <li>Implement the parseCommandLine() mthod.
 * <li>Implement one message handling method for each type of message that is
 *     a part of the sub-class' protocol.  For example the DumpTpCommand
 *     sub-class should implement a message handler method for DumpNotification
 *     messages.
 * <li>Register in their constructor the message handling methods they have
 *     implemented by calling TpcpCommand::registerMsgHandler for each of their
 *     message handling methods.
 * </ul>
 *
 * Please note that TpcpCommand checks the mount transaction ID of all
 * incomming tape-bridge messages.  Sub-classes therefore do not need to check
 * the mount transaction ID in their message handler methods.
 */
class TpcpCommand : public castor::BaseObject {
public:

  /**
   * Constructor.
   *
   * @param programName The program name.
   */
  TpcpCommand(const char *const programName) throw();

  /**
   * Destructor.
   */
  virtual ~TpcpCommand() throw();

  /**
   * The entry function of the tpcp command.
   *
   * @param argc The number of command-line arguments.
   * @param argv The command-line arguments.
   */
  int main(const int argc, char **argv) throw();


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
  virtual void usage(std::ostream &os) throw() = 0;

  /**
   * To be implemented by sub-classes.
   *
   * Parses the specified command-line arguments.
   *
   * @param argc Argument count from the executable's main().
   * @param argv Argument vector from the executable's main().
   */
  virtual void parseCommandLine(const int argc, char **argv)
    throw(castor::exception::Exception) = 0;

  /**
   * To be implemented by sub-classes.
   *
   * Performs the tape copy whether it be DUMP, READ or WRITE.
   */
  virtual void performTransfer() throw(castor::exception::Exception) = 0;

  /**
   * The program name.
   */
  const char *const m_programName;

  /**
   * Initially set to false, but set to true if a SIGNINT interrupt is received
   * (control-C).
   */
  static bool s_receivedSigint;

  /**
   * The ID of the user running the tpcp command.
   */
  const uid_t m_userId;

  /**
   * The ID of default group of the user running the tpcp command.
   */
  const gid_t m_groupId;

  /**
   * Vmgr error buffer.
   */
  static char vmgr_error_buffer[VMGRERRORBUFLEN];

  /**
   * The parsed command-line.
   */
  ParsedCommandLine m_cmdLine;

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
  vmgr_tape_info_byte_u64 m_vmgrTapeInfo;

  /**
   * The DGN of the tape to be used.
   */
  char m_dgn[CA_MAXDGNLEN + 1];

  /**
   * TCP/IP tape-bridge callback socket.
   */
  castor::io::ServerSocket m_callbackSock;

  /**
   * True if the tpcp command has got a volume request ID from the VDQM.
   */
  bool m_gotVolReqId;

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
   * callbacks from the tape-bridge daemon.
   */
  void setupCallbackSock() throw(castor::exception::Exception);

  /**
   * Request a drive from the VDQM to mount the specified tape for the
   * specified access mode (read or write).
   *
   * @param accessMode The tape access mode, either WRITE_DISABLE or
   *                   WRITE_ENABLE.
   * @param tapeServer If not NULL then this parameter specifies the tape
   *                   server to be used, therefore overriding the drive
   *                   scheduling of the VDQM.
   */
  void requestDriveFromVdqm(const int accessMode, char *const tapeServer)
    throw(castor::exception::Exception);

  /**
   * Registers the specified tape-bridge message handler member function.
   *
   * @param messageType The type of tape-bridge message.
   * @param obj         The object to handle the tape-bridge message.
   * @param func        The member function to handle the tape-bridge message.
   */
  template<class T> void registerMsgHandler(
    const int messageType,
    T         *obj,
    bool (T::*func)(castor::IObject *, castor::io::AbstractSocket &))
    throw() {

    m_msgHandlers[messageType] = new MsgHandler<T>(obj, func);
  }

  /**
   * Waits for and accepts an incoming tape-bridge connection, reads in the
   * tape-bridge message and then dispatches it to appropriate message handler
   * member function.
   *
   * @return True if there is more work to be done, else false.
   */
  bool waitForAndDispatchMessage() throw(castor::exception::Exception);

  /**
   * PingNotification message handler.
   *
   * @param obj  The tape-bridge message to be processed.
   * @param sock The socket on which to reply to the tape-bridge.
   * @return     True if there is more work to be done else false.
   */
  bool handlePingNotification(castor::IObject *const obj,
    castor::io::AbstractSocket &sock) throw(castor::exception::Exception);

  /**
   * EndNotification message handler.
   *
   * @param obj  The tape-bridge message to be processed.
   * @param sock The socket on which to reply to the tape-bridge.
   * @return     True if there is more work to be done else false.
   */
  bool handleEndNotification(castor::IObject *const obj,
    castor::io::AbstractSocket &sock) throw(castor::exception::Exception);

  /**
   * EndNotificationErrorReport message handler.
   *
   * @param obj  The tape-bridge message to be processed.
   * @param sock The socket on which to reply to the tape-bridge.
   * @return     True if there is more work to be done else false.
   */
  bool handleEndNotificationErrorReport(castor::IObject *const obj,
    castor::io::AbstractSocket &sock) throw(castor::exception::Exception);

  /**
   * Handles the specified failed file-transfers.
   *
   * @param files The failed file transfers.
   */
  void handleFailedTransfers(
    const std::vector<tapegateway::FileErrorReportStruct*> &files)
    throw(castor::exception::Exception);

  /**
   * Handles the specified failed file-transfer.
   *
   * @param file The failed file-transfer.
   */
  void handleFailedTransfer(const tapegateway::FileErrorReportStruct &file)
    throw(castor::exception::Exception);

  /**
   * Acknowledges the end of the session to the tape-bridge.
   */
  void acknowledgeEndOfSession() throw(castor::exception::Exception);

  /**
   * Convenience method for sending EndNotificationErrorReport messages to the
   * tape-bridge.
   *
   * Note that this method intentionally does not throw any exceptions.  The
   * method tries to send an EndNotificationErrorReport messages to the
   * tape-bridge and fails silently in the case it cannot.
   *
   * @param tapebridgeTransactionId The tape-bridge transaction ID.
   * @param errorCode               The error code.
   * @param errorMessage            The error message.
   * @param sock                    The socket on which to reply to the
   *                                tape-bridge.
   */
  void sendEndNotificationErrorReport(
    const uint64_t             tapebridgeTransactionId,
    const int                  errorCode,
    const std::string          &errorMessage,
    castor::io::AbstractSocket &sock)
    throw();

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
   * @param sock The socket on which to reply to the tape-bridge with an
   *             EndNotificationErrorReport message if the cast fails.
   */
  template<class T> void castMessage(castor::IObject *const obj, T *&msg,
    castor::io::AbstractSocket &sock) throw() {
    msg = dynamic_cast<T*>(obj);

    if(msg == NULL) {
      std::stringstream oss;

      oss <<
        "Unexpected object type" <<
        ": Actual=" << utils::objectTypeToString(obj->type()) <<
        " Expected=" << utils::objectTypeToString(T().type());

      const uint64_t tapebridgeTransactionId = 0; // Transaction Id unknown
      sendEndNotificationErrorReport(tapebridgeTransactionId, SEINTERNAL,
        oss.str(), sock);

      TAPE_THROW_EX(castor::exception::Internal, oss.str());
    }
  }

  /**
   * Helper method that wraps the C function rfio_stat64 and converts any
   * error it reports into an exception.
   *
   * @param path    The path to be passed to rfio_stat64.
   * @param statBuf The stat buffer to be passed to rfio_stat64.
   */
  void rfioStat(const char *path, struct stat64 &statBuf)
    throw(castor::exception::Exception);


private:

  /**
   * Displays the specified error message, cleans up (at least deletes the
   * volume-request id if there is one) and then calls exit(1) indicating ani
   * error.
   */
  void displayErrorMsgCleanUpAndExit(
    const char *msg) throw();

  /**
   * Executes the main code of the command.
   *
   * The specification of this method intentionally does not have a throw()
   * clause so that any type of exception can be thrown.
   */
  void executeCommand();

  /**
   * Throws a permission denied exception if the user of the tpcp command
   * does not have permission to write to tape.
   *
   * @param poolName   The name of the pool in which the tape to be written
   *                   resides.
   * @param userId     The ID of the user.
   * @param groupId    The group ID of the user.
   * @param sourceHost The CUPV source host.
   */
  void checkUserHasTapeWritePermission(const char *const poolName,
    const uid_t userId, const gid_t groupId, const char *const sourceHost)
    throw (castor::exception::PermissionDenied);

  /**
   * Deletes the specified VDQM volume request.
   */
  void deleteVdqmVolumeRequest() throw (castor::exception::Exception);

  /**
   * An abstract functor to handle incomming tape-bridge messages.
   */
  class AbstractMsgHandler {
  public:

    /**
     * operator().
     *
     * @param obj  The tape-bridge message to be processed.
     * @param sock The socket on which to reply to the tape-bridge.
     * @return     True if there is more work to be done else false.
     */
    virtual bool operator()(
      castor::IObject            *obj,
      castor::io::AbstractSocket &sock
      ) const throw(castor::exception::Exception) = 0;

    /**
     * Destructor.
     */
    virtual ~AbstractMsgHandler() {
      // DO nothing
    }
  };

  /**
   * Concrete tape-bridge message handler template functor.
   */
  template<class T> class MsgHandler : public AbstractMsgHandler {
  public:

    /**
     * Constructor.
     */
    MsgHandler(
      T *const obj,
      bool (T::*const func)(
        castor::IObject            *obj,
        castor::io::AbstractSocket &sock)) :
      m_obj(obj), m_func(func) {
      // Do nothing
    }

    /**
     * operator().
     *
     * @param obj  The tape-bridge message to be processed.
     * @param sock The socket on which to reply to the tape-bridge.
     * @return     True if there is more work to be done else false.
     */
    virtual bool operator()(
      castor::IObject            *obj,
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

  private:

    /**
     * The object of class T on which the member function pointed to by m_func
     * will be called when the operator() function is called.
     */
    T *const m_obj;

    /**
     * The member function of class T to be called by operator().
     */
    bool (T::*const m_func)(
      castor::IObject            *obj,
      castor::io::AbstractSocket &sock);
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
   * The SIGINT signal handler.
   */
  static void sigintHandler(int signal);

  /**
   * The SIGINT action handler structure to be used with sigaction.
   */
  struct sigaction m_sigintAction;

  /**
   * Map from message type (uint32_t) to pointer message handler
   * (AbstractMsgHandler *).
   *
   * The destructor of this map is responsible for and will deallocate the
   * MsgHandlers it points to.
   */
  MsgHandlerMap m_msgHandlers;

  /**
   * Check the format of the filename, that MUST BE of the form: 
   * hostaname:/<filepath/>filename.
   * Local files need to have an hostname specified, if not will be added the
   * local hostname.
   * Hostnames like "localhost", "127.0.0.1" or "" are not allowed, they will be
   * translate in to local hostname.
   * Relative filename will be prefix by the hostaname and by the current
   * working directory
   */
  void checkFilenameFormat() throw(castor::exception::Exception);

  /**
   * The hostaname of the machine.
   */
  char m_hostname[CA_MAXHOSTNAMELEN+1];

  /**
   * The current working directory where tpcp command is run.
   */
  char m_cwd[CA_MAXPATHLEN+1];

}; // class TpcpCommand

} // namespace tpcp
} // namespace tape
} // namespace castor


#endif // CASTOR_TAPE_TPCP_TPCPCOMMAND_HPP
