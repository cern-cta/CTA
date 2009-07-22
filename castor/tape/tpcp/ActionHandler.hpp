/******************************************************************************
 *                 castor/tape/tpcp/ActionHandler.hpp
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

#ifndef CASTOR_TAPE_TPCP_ACTIONHANDLER_HPP
#define CASTOR_TAPE_TPCP_ACTIONHANDLER_HPP 1

#include "castor/exception/Exception.hpp"
#include "castor/io/ServerSocket.hpp"
#include "castor/tape/tpcp/Action.hpp"
#include "castor/tape/tpcp/FilenameList.hpp"
#include "castor/tape/tpcp/ParsedCommandLine.hpp"
#include "castor/tape/utils/utils.hpp"
#include "h/Castor_limits.h"
#include "h/vmgr_api.h"

#include <map>
#include <sstream>
#include <stdint.h>

namespace castor {
namespace tape   {
namespace tpcp   {

/**
 * Asbtract class specifying the run() method to be implemented by all Action
 * handlers.
 */
class ActionHandler {
public:

  /**
   * Constructor.
   *
   * @param cmdLine      The parsed command-line.
   * @param filenames    The list of RFIO filenames to be processed.  The list
   *                     of filenames in the command-line parameter cmdLine
   *                     should NOT be used.  Filenames are provided by the
   *                     user on the command-line or in a "file list" file.
   *                     These two methods of passing filenames are mutually
   *                     exclusive.  The user must provide the filenames on
   *                     the command-line or in a "file list" file, but NOT
   *                     both.  This list parameter is populated using either
   *                     the command-line or the "file list" file as
   *                     appropriate.
   * @param vmgrTapeInfo The information retrieved from the VMGR about the tape
   *                     to be used.
   * @param dgn          The DGN of the tape to be worked on.
   * @param volReqId     The volume request ID returned by the VDQM in response
   *                     to the request for a drive.
   * @param callbackSock The aggregator callback socket.
   */
  ActionHandler(ParsedCommandLine &cmdLine, FilenameList &filenames,
    const vmgr_tape_info &vmgrTapeInfo, const char *const dgn,
    const uint64_t volReqId, castor::io::ServerSocket &callbackSock) throw();

  /**
   * Destructor.
   */
  virtual ~ActionHandler();

  /**
   * Performs the action.
   */
  virtual void run() throw(castor::exception::Exception) = 0;


protected:

  /**
   * The parsed command-line.
   */
  ParsedCommandLine &m_cmdLine;

  /**
   * The list of RFIO filenames to be processed.
   */
  FilenameList &m_filenames;

  /**
   * Iterator pointing to the next RFIO filename;
   */
  FilenameList::const_iterator m_filenameItor;

  /**
   *  The information retrieved from the VMGR about the tape to be used.
   */
  const vmgr_tape_info &m_vmgrTapeInfo;

  /**
   * The DGN of the tape to be worked on.
   */
  const char *const m_dgn;

  /**
   * The volume request ID returned by the VDQM in response to the request for
   * a drive.
   */
  const uint64_t m_volReqId;

  /**
   * The aggregator callback socket.
   */
  castor::io::ServerSocket &m_callbackSock;

  /**
   * The next file transaction ID.
   */
  uint64_t m_fileTransactionId;

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
   * Convenience method fro sending EndNotificationErrorReport messages to the
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
    virtual ~AbstractMsgHandler();
  };

  /**
   * Conscrete Aggregator message handler template functor.
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
    ~MsgHandlerMap();
  }; // MsgHandlerMap

  /**
   * Map from message type (uint32_t) to pointer message handler
   * (AbstractMsgHandler *).
   *
   * The destructor of this map is responsible for and will deallocate the
   * MsgHandlers it points to.
   */
  MsgHandlerMap m_msgHandlers;

}; // class ActionHandler

} // namespace tpcp
} // namespace tape
} // namespace castor

#endif // CASTOR_TAPE_TPCP_ACTIONHANDLER_HPP
