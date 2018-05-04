/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include "common/Constants.hpp"
#include "mediachanger/acs/Constants.hpp"
#include "mediachanger/messages.hpp"
#include "mediachanger/ZmqSocket.hpp"
#include "mediachanger/ZmqSocketST.hpp"
#include "AcsImpl.hpp"
#include "common/exception/Exception.hpp"
#include <time.h>

namespace cta     {
namespace mediachanger        {
namespace acs     {
  
  
/**
 * Abstract class defining the ACS request presentation to be used in the list
 * of pending ACS requests.
 */
class AcsRequest {
public:
  
  /**
   * Constructor.
   * 
   * @param socket  ZMQ socket to use.
   * @param address ZMQ message with client address.
   * @param empty   ZMQ empty message.
   * @param seqNo   Sequence number for the ACS request.
   * @param vid     The vid of the ACS request.
   * @param acs     The acs value of the ACS request.
   * @param lsm     The lsm value of the ACS request.
   * @param panel   The panel value of the ACS request.
   * @param drive   The drive value of the ACS request.
   */
  AcsRequest(ZmqSocketST &socket, ZmqMsg &address,
    ZmqMsg &empty, const SEQ_NO seqNo,
    const std::string vid, const uint32_t acs,
    const uint32_t lsm, const uint32_t panel, const uint32_t drive);
  
  /**
   * Destructor.
   */
  virtual ~AcsRequest() = 0;

  /**
   * Perform any time related actions with the request to CASTOR ACS daemon.
   *
   * This method does not have to be called at any time precise interval.
   */
  virtual void tick() = 0;
  
  /**
   * Checks if the ACS request is in to be executed state.
   * 
   * @return true if the ACS request in state to Execute.
   */  
  bool isToExecute() const throw();  
  
  /**
   * Sets state of the ACS request to be executed.
   */
  void setStateToExecute() throw();
  
  /**
   * Checks if the ACS request is in running state.
   * 
   * @return true if the ACS request is in running state.
   */  
  bool isRunning() const throw();
  
  /**
   * Sets state of the ACS request to running.
   */
  void setStateIsRunning();
  
  /**
   * Checks if the ACS request is in completed state.
   * 
   * @return true if the ACS request is in completed state.
   */  
  bool isCompleted() const throw();
  
  /**
   * Sets state of the ACS request to completed and fills ZMQ replay frame with 
   * good status 0.
   */
  void setStateCompleted();
  
  /**
   * Checks if the ACS request is in failed state.
   * 
   * @return true if the ACS request is in failed state.
   */  
  bool isFailed() const throw();
  
  /**
   * Sets state of the ACS request to failed and fills ZMQ replay frame with
   * exception data.
   */
  void setStateFailed(const int code,const std::string& msg);
  
  /**
   * Checks if the ACS request is in to delete state.
   * 
   * @return true if the ACS request is in to delete state.
   */  
  bool isToDelete() const throw();
  
  /**
   * Sets state of the ACS request to be deleted.
   */
  void setStateToDelete() throw();

  /**
   * Gets the vid component of the ACS request.
   *
   * @return the vid component of the ACS request.
   */
  const std::string &getVid() const throw ();
  
  /**
   * Gets the acs component of the ACS request.
   *
   * @return the acs component of the ACS request.
   */
  uint32_t getAcs() const throw ();

  /**
   * Gets the lsm component of the ACS request.
   *
   * @return the lsm component of the ACS request.
   */
  uint32_t getLsm() const throw ();

  /**
   * Gets the panel component of the ACS request.
   *
   * @return the panel component of the ACS request.
   */
  uint32_t getPanel() const throw ();

  /**
   * Gets the drive component of the ACS request.
   *
   * @return the drive component of the ACS request.
   */
  uint32_t getDrive() const throw ();
  
  /**
   * Gets the SeqNumber component of the ACS request.
   *
   * @return the SeqNo component of the ACS request.
   */
  SEQ_NO getSeqNo() const throw ();
  
  /**
   * Sets the fields of the response message of the ACS response request.
   * 
   * @param responseType The type of the response message.
   * @param responseMsg  The response message.
   */
  void setResponse(const ACS_RESPONSE_TYPE responseType,
    const ALIGNED_BYTES *const responseMsg) throw ();
  
  /**
   * Send a replay to the client who issued the ACS request.
   */
  void sendReplayToClientOnce();
  
  /**
   * Returns string presentation for the connection identity with the client.
   * 
   * @return The connection Identity.
   */
  std::string getIdentity() const throw();
  
  /**
   * Returns a string representing ACS request.
   * 
   * @return The value of string presentation for the request 
   */
  std::string str() const;
  
  /**
   * Abstract method to be implemented in concrete implementation. Checks 
   * the status of the response from the response message buffer and the type of
   * the response. Throws exception if the type of the response is RT_FINAL but
   * the status is not success.
   * 
   * @return true if the type of response RT_FINAL and the response status 
   *         is STATUS_SUCCESS.
   */
  virtual bool isResponseFinalAndSuccess() const = 0; 
   
private:

  /**
   * Creates a message frame containing a ReturnValue message.
   *
   * @param value The return value of the ReturnValue message.
   * @return The message frame.
   */
  //cta::messages::Frame createReturnValueFrame(const int value);
  Frame createReturnValueFrame(const int value);
  
  /**
   * Creates a message frame containing an Exception message.
   *
   * @param code The error code of the exception.
   * @param msg The message string of the exception.
   */
  Frame createExceptionFrame(const int code, 
    const std::string& msg);
  
  /**
   * Request sequence number of the ACS request.
   */
  const SEQ_NO m_seqNo;
  
  /**
   * The vid component of the ACS request.
   */
  const std::string m_vid;

  /**
   * The acs component of the ACS request.
   */
  const uint32_t m_acs;
  
  /**
   * The lsm component of the ACS request.
   */
  const uint32_t m_lsm;
  
  /**
   * The panel component of the ACS request.
   */
  const uint32_t m_panel;
  
  /**
   * The drive component of the ACS request.
   */
  const uint32_t m_drive;
  
  /**
   * Internal state of the ACS request.
   */  
  RequestState m_state ; 
  
  /**
   * Replay ZMQ frame for the client.
   */
  Frame m_reply;
    
  /**
   * The ZMQ socket listening for messages.
   */
  ZmqSocketST &m_socket;
  
  /**
   * Client identity for logging.
   */  
  const std::string m_identity;
  
  /**
   * ZMQ address message for the client.
   */
  zmq_msg_t m_addressMsg;
  
  /**
   * ZMQ empty message for the client.
   */
  zmq_msg_t m_emptyMsg;
  
  /**
   * Store is the replay to the client has been sent or not.
   */
  bool m_isReplaySent;
  
protected:  
  /**
   * The type of the response message associated with the ACS request.
   */
  ACS_RESPONSE_TYPE m_responseType;
  
  /**
   * The response message associated with the ACS request.
   */
  ALIGNED_BYTES m_responseMsg[MAX_MESSAGE_SIZE / sizeof(ALIGNED_BYTES)];
  
}; // class AcsRequest

} // namespace acs
} // namespace mediachanger
} // namespace cta
