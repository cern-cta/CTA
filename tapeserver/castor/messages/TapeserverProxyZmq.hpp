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
#include "castor/messages/Frame.hpp"
#include "castor/messages/Mutex.hpp"
#include "tapeserver/daemon/TapedProxy.hpp"
#include "castor/messages/ZmqSocketST.hpp"

namespace castor {
namespace messages {

/**
 * A concrete implementation of the interface to the internal network
 * communications of the tapeserverd daemon.
 */
class TapeserverProxyZmq: public cta::tape::daemon::TapedProxy {
public:
  /**
   * Constructor.
   *
   * @param log The object representing the API of the CASTOR logging system.
   * @param serverPort The TCP/IP port on which the tapeserverd daemon is
   * listening for internal ZMQ message.
   * @param zmqContext The ZMQ context.
   */
  TapeserverProxyZmq(log::Logger &log, const unsigned short serverPort,
    void *const zmqContext, const std::string & driveName) throw();

  void reportState(const cta::tape::session::SessionState state,
    const cta::tape::session::SessionType type, const std::string& vid) override;
  
  void reportHeartbeat(uint64_t totalTapeBytesMoved, uint64_t totalDiskBytesMoved) override;
private:
  uint32_t gotArchiveJobFromCTA(const std::string &vid,
    const std::string &unitName, const uint32_t nbFiles);
  
  void gotRetrieveJobFromCTA(const std::string &vid,
    const std::string &unitName);

  void tapeMountedForRecall(const std::string &vid,
    const std::string &unitName);
  
  void tapeMountedForMigration(const std::string &vid, 
    const std::string &unitName);

  void tapeUnmountStarted(const std::string &vid,
    const std::string &unitName) {}
 
  void tapeUnmounted(const std::string &vid,
    const std::string &unitName)  {}
 
  void notifyHeartbeat(const std::string &unitName,
    const uint64_t nbBlocksMoved);
public:  
  virtual void addLogParams(const std::string &unitName,
    const std::list<castor::log::Param> & params) override;
  
  virtual void deleteLogParams(const std::string &unitName,
    const std::list<std::string> & paramNames) override;
  
  void labelError(const std::string &unitName, const std::string &message) override;

private:

  /**
   * Mutex used to implement a critical section around the enclosed
   * ZMQ socket.
   */
  Mutex m_mutex;

  /**
   * The object representing the API of the CASTOR logging system.
   */
  log::Logger &m_log;
  
  /**
   * The name of the drive managed by this proxy.
   */
  std::string m_driveName;

  /**
   * The name of the host on which the vdqmd daemon is running.
   */
  const std::string m_tapeserverHostName;

  /**
   * The TCP/IP port on which the vdqmd daemon is listening.
   */
  const unsigned short m_serverPort;
  
  /**
   * Socket connecting this tape server proxy to the tape server daemon.
   */
  ZmqSocketST m_serverSocket;
  
  /**
   * Creates a frame containing a ArchiveJobFromCTA message.
   *
   * @param vid The volume identifier of the tape.
   * @param unitName The unit name of the tape drive.
   * @return The frame.
   */
  Frame createArchiveJobFromCTAFrame(const std::string &vid,
    const std::string &unitName, const uint32_t nbFiles);
          
  /**
   * Creates a frame containing a RetrieveJobFromCTA message.
   *
   * @param vid The volume identifier of the tape.
   * @param unitName The unit name of the tape drive.
   * @return The frame.
   */
  Frame createRetrieveJobFromCTAFrame(const std::string &vid,
    const std::string &unitName);

  /**
   * Creates a frame containing a TapeMountedForRecall message.
   *
   * @param vid The volume identifier of the tape.
   * @param unitName The unit name of the tape drive.
   * @return The frame.
   */
  Frame createTapeMountedForRecallFrame(const std::string &vid,
    const std::string &unitName);

  /**
   * Creates a frame containing a TapeMountedForMigration message.
   *
   * @param vid The volume identifier of the tape.
   * @param unitName The unit name of the tape drive.
   * @return The frame.
   */
  Frame createTapeMountedForMigrationFrame(const std::string &vid,
    const std::string &unitName);

  /**
   * Creates a frame containing a Heartbeat message.
   *
   * @param unitName The unit name of the tape drive.
   * @param nbBlocksMoved Delta value giving the number of blocks moved
   * since the last heartbeat message.
   * @return The frame.
   */
  Frame createHeartbeatFrame(const std::string &unitName,
    const uint64_t nbBlocksMoved);
  
  /**
   * Creates a frame containing an add log params.
   *
   * @param unitName The unit name of the tape drive.
   * @param params Values of the log parameters to be added or overwritten
   * in the status that the session will have in the main process's logs.
   * @return The frame.
   */
  Frame createAddLogParamsFrame(const std::string &unitName,
    const std::list<castor::log::Param> & params);

  /**
   * Creates a frame containing a delete log params.
   *
   * @param unitName The unit name of the tape drive.
   * @param paramNames names of the log parameters to remove
   * @return The frame.
   */
  Frame createDeleteLogParamsFrame(const std::string &unitName,
    const std::list<std::string> & paramNames);
  
  /**
   * Creates a frame containing a LabelError message.
   *
   * @param unitName The unit name of the tape drive.
   * @param message The error message.
   * @return The frame.
   */
  Frame createLabelErrorFrame(const std::string &unitName,
    const std::string &message);

}; // class TapeserverProxyZmq

} // namespace messages
} // namespace castor

