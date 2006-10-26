/******************************************************************************
 *                      FileSystemStatus.hpp
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
 * @(#)$RCSfile$ $Revision$ $Release$ $Date$ $Author$
 *
 * Describes the status of one file system
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef MONITORING_FILESYSTEMSTATUS_HPP
#define MONITORING_FILESYSTEMSTATUS_HPP 1

#include <iostream>
#include "osdep.h"
#include "castor/stager/FileSystemStatusCodes.hpp"
#include "castor/monitoring/AdminStatusCodes.hpp"

namespace castor {

  namespace monitoring {

    /*
     * Describes the status of one machine
     */
    class FileSystemStatus {

    public:

      /*
       * Constructor
       */
      FileSystemStatus(u_signed64 id = 0);

      /**
       * print method
       */
      void print(std::ostream& out,
                 const std::string& indentation = "") const
        throw();

    public:

      /// Accessor to id
      u_signed64 id() const { return m_id; }

      /// Accessor to id
      void setId(u_signed64 id) { m_id = id; }

      /// Accessor to weigth
      float weight() const { return m_weight; }

      /// Accessor to weigth
      void setWeight(float weight) { m_weight = weight; }

      /// Accessor to deltaWeight
      float deltaWeight() const { return m_deltaWeight; }

      /// Accessor to deltaWeight
      void setDeltaWeight(float deltaWeight) { m_deltaWeight = deltaWeight; }

      /// Accessor to deviation
      float deviation() const { return m_deviation; }

      /// Accessor to deviation
      void setDeviation(float deviation) { m_deviation = deviation; }

      /// Accessor to space
      u_signed64 space() const { return m_space; }

      /// Accessor to space
      void setSpace(u_signed64 space) { m_space = space; }

      /// Accessor to status
      castor::stager::FileSystemStatusCodes status() const {
        return m_status;
      }

      /// Accessor to status
      void setStatus(castor::stager::FileSystemStatusCodes status) {
        m_status = status;
      }

      /// Accessor to adminStatus
      castor::monitoring::AdminStatusCodes adminStatus() const {
        return m_adminStatus;
      }

      /// Accessor to adminStatus
      void setAdminStatus(castor::monitoring::AdminStatusCodes adminStatus) {
        m_adminStatus = adminStatus;
      }

      /// Accessor to readRate
      u_signed64 readRate() const { return m_readRate; }

      /// Accessor to readRate
      void setReadRate(u_signed64 readRate) { m_readRate = readRate; }

      /// Accessor to writeRate
      u_signed64 writeRate() const { return m_writeRate; }

      /// Accessor to writeRate
      void setWriteRate(u_signed64 writeRate) { m_writeRate = writeRate; }

      /// Accessor to readStreams
      unsigned int readStreams() const { return m_readStreams; }

      /// Accessor to readStreams
      void setReadStreams(unsigned int readStreams) { m_readStreams = readStreams; }

      /// Accessor to writeStreams
      unsigned int writeStreams() const { return m_writeStreams; }

      /// Accessor to writeStreams
      void setWriteStreams(unsigned int writeStreams) { m_writeStreams = writeStreams; }

      /// Accessor to readWriteStreams
      unsigned int readWriteStreams() const { return m_readWriteStreams; }

      /// Accessor to readWriteStreams
      void setReadWriteStreams(unsigned int readWriteStreams) { m_readWriteStreams = readWriteStreams; }

      /// Accessor to free space
      u_signed64 freeSpace() const { return m_freeSpace; }

      /// Accessor to free space
      void setFreeSpace(u_signed64 freeSpace) { m_freeSpace = freeSpace; }

      /// Accessor to lastStateupdate
      void setLastStateUpdate (u_signed64 lastStateUpdate) {
        m_lastStateUpdate = lastStateUpdate;
      }

      /// Accessor to lastStateupdate
      u_signed64 lastStateUpdate () { return m_lastStateUpdate; }

      /// Accessor to lastMetricsupdate
      void setLastMetricsUpdate (u_signed64 lastMetricsUpdate) {
        m_lastMetricsUpdate = lastMetricsUpdate;
      }

      /// Accessor to lastMetricsupdate
      u_signed64 lastMetricsUpdate () { return m_lastMetricsUpdate; }

    private:

      /// the filesystem id
      u_signed64 m_id;

      /// the fileSystem weigth
      float m_weight;

      /// the fileSystem deltaWeight
      float m_deltaWeight;

      /// the fileSystem deviation
      float m_deviation;

      /// total space
      u_signed64 m_space;

      /// status
      castor::stager::FileSystemStatusCodes m_status;

      /// admin status
      castor::monitoring::AdminStatusCodes m_adminStatus;

      /// The number of bytes read per second
      u_signed64 m_readRate;

      /// The number of bytes written per second
      u_signed64 m_writeRate;

      /// The number of read streams
      unsigned int m_readStreams;

      /// The number of read streams
      unsigned int m_writeStreams;

      /// The number of read-write streams
      unsigned int m_readWriteStreams;

      /// available space
      u_signed64 m_freeSpace;

      /// Last state update (second since EPOCH)
      u_signed64 m_lastStateUpdate;

      /// Last metrics update (second since EPOCH)
      u_signed64 m_lastMetricsUpdate;

    }; // end FileSystemStatus

  } // end monitoring

} // end castor

#endif // MONITORING_FILESYSTEMSTATUS_HPP
