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
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#pragma once

#include "castor/log/Logger.hpp"
#include "castor/messages/Header.pb.h"
#include "castor/messages/Constants.hpp"
#include "castor/messages/Heartbeat.pb.h"
#include "castor/messages/NotifyDrive.pb.h"
#include "castor/tape/reactor/ZMQPollEventHandler.hpp"
#include "castor/tape/reactor/ZMQReactor.hpp"
#include "castor/legacymsg/VdqmProxy.hpp"
#include "castor/legacymsg/VmgrProxy.hpp"
#include "castor/tape/tapeserver/daemon/DriveCatalogue.hpp"
#include "castor/tape/utils/ZmqMsg.hpp"
#include "castor/tape/utils/ZmqSocket.hpp"
#include "castor/utils/utils.hpp"

namespace castor     {
namespace tape       {
namespace tapeserver {
namespace daemon     {

/**
 * Handles the events of the socket listening for connection from the admin
 * commands.
 */
class TapeMessageHandler: public reactor::ZMQPollEventHandler {
public:

  /**
   * Constructor.
   *
   * @param reactor The reactor to which new Vdqm connection handlers are to
   * be registered.
   * @param log The object representing the API of the CASTOR logging system.
   * @param driveCatalogue The tape-drive catalogue.
   * @param hostName The name of the host.
   * @param vdqm Proxy object representing the vdqmd daemon.
   * @param zmqContext The ZMQ context.
   */
  TapeMessageHandler(
    reactor::ZMQReactor &reactor,
    log::Logger &log,
    DriveCatalogue &driveCatalogue,
    const std::string &hostName,
    castor::legacymsg::VdqmProxy & vdqm,
    castor::legacymsg::VmgrProxy & vmgr,
    void *const zmqContext);

  /**
   * Destructor.
   */
  ~TapeMessageHandler() throw();

  /**
   * Returns the human-readable name this event handler.
   */
  std::string getName() const throw();

  /**
   * Fills the specified poll file-descriptor ready to be used in a call to
   * poll().
   */
  void fillPollFd(zmq_pollitem_t &fd) throw();

  /**
   * Handles the specified event.
   *
   * @param fd The poll file-descriptor describing the event.
   * @return true if the event handler should be removed from and deleted by
   * the reactor.
   */
  bool handleEvent(const zmq_pollitem_t &fd);
  
private:
  void sendSuccessReplyToClient();
  
  template <class T> void unserialize(T& msg, tape::utils::ZmqMsg& blob){
    std::string logMessage="Cant parse " ;
    logMessage+=castor::utils::demangledNameOf(msg)+" from binary data. Wrong body";
    if(!msg.ParseFromArray(zmq_msg_data(&blob.getZmqMsg()),zmq_msg_size(&blob.getZmqMsg()))){
        m_log(LOG_ERR,logMessage); 
      }
  }
  
   /**
   * The reactor to which new Vdqm connection handlers are to be registered.
   */
  reactor::ZMQReactor &m_reactor;

  /**
   * The object representing the API of the CASTOR logging system.
   */
  log::Logger &m_log;
  
  tape::utils::ZmqSocket m_socket;
  
    /**
   * The catalogue of tape drives controlled by the tape server daemon.
   */
  DriveCatalogue &m_driveCatalogue;
  
  /**
   * The name of the host on which tape daemon is running.
   */
  const std::string m_hostName;
  
  /** 
   * Reference to the VdqmProxy, allowing reporting of the drive status. It
   * will be used by the StatusReporter 
   */
  castor::legacymsg::VdqmProxy & m_vdqm;

  /** 
   * Reference to the VmgrProxy, allowing reporting and checking tape status.
   * It is also used by the StatusReporter 
   */
  castor::legacymsg::VmgrProxy & m_vmgr;
  
  void checkSocket(const zmq_pollitem_t &fd);
  
  void dispatchEvent(castor::messages::Header& header);
  
  void dealWith(const castor::messages::Header&,
                         const castor::messages::Heartbeat& body);
  
  void dealWith(const castor::messages::Header& header, 
     const castor::messages::NotifyDriveBeforeMountStarted& body);

void dealWith(const castor::messages::Header& header,
     const castor::messages::NotifyDriveTapeMounted& body); 

  /**
   * Unserialise the blob and check the header
   * @param headerBlob
   */
  castor::messages::Header buildHeader(tape::utils::ZmqMsg& headerBlob);
}; // class TapeMessageHandler

} // namespace daemon
} // namespace tapeserver
} // namespace tape
} // namespace castor
