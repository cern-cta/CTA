/******************************************************************************
 *                      DiskServerStatus.hpp
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
 * Describes the status of one diskServer
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef MONITORING_DISKSERVERSTATUS_HPP
#define MONITORING_DISKSERVERSTATUS_HPP 1

#include "castor/monitoring/FileSystemStatus.hpp"
#include "castor/stager/DiskServerStatusCode.hpp"
#include "castor/monitoring/AdminStatusCodes.hpp"
#include "castor/sharedMemory/Allocator.hpp"
#include "castor/monitoring/SharedMemoryString.hpp"
#include "castor/monitoring/ClusterStatusBlockKey.hpp"
#include <map>

namespace castor {

  namespace monitoring {

    /*
     * Describes the status of one diskServer
     * Enforces usage of an external memory allocator for the Filesystem list
     */
    class DiskServerStatus :
      public std::map<castor::monitoring::SharedMemoryString,
                      castor::monitoring::FileSystemStatus,
                      std::less<castor::monitoring::SharedMemoryString>,
                      castor::sharedMemory::Allocator
                      <std::pair<castor::monitoring::SharedMemoryString,
                                 castor::monitoring::FileSystemStatus>,
                       castor::monitoring::ClusterStatusBlockKey> > {

    public:

      /*
       * Constructor
       */
      DiskServerStatus();

      /**
       * print method
       */
      void print(std::ostream& out,
                 const std::string& indentation = "") const
        throw();

    public:

      /// Accessor to ram
      void setRam(u_signed64 ram) { m_ram = ram; }

      /// Accessor to ram
      u_signed64 ram() const { return m_ram; }

      /// Accessor to memory
      void setMemory(u_signed64 memory) { m_memory = memory; }

      /// Accessor to memory
      u_signed64 memory() const { return m_memory; }

      /// Accessor to swap
      void setSwap(u_signed64 swap) { m_swap = swap; }

      /// Accessor to swap
      u_signed64 swap() const { return m_swap; }

      /// Accessor to status
      void setStatus(castor::stager::DiskServerStatusCode status) {
        m_status = status;
      }

      /// Accessor to status
      castor::stager::DiskServerStatusCode status() const { return m_status; }

      /// Accessor to adminStatus
      void setAdminStatus(castor::monitoring::AdminStatusCodes adminStatus) {
        m_adminStatus = adminStatus;
      }

      /// Accessor to adminStatus
      castor::monitoring::AdminStatusCodes adminStatus() const {
        return m_adminStatus;
      }

      /// Accessor to freeRam
      void setFreeRam(u_signed64 freeRam) { m_freeRam = freeRam; }

      /// Accessor to freeRam
      u_signed64 freeRam()const { return m_freeRam; }

      /// Accessor to freeMemory
      void setFreeMemory(u_signed64 freeMemory) { m_freeMemory = freeMemory; }

      /// Accessor to freeMemory
      u_signed64 freeMemory()const { return m_freeMemory; }

      /// Accessor to freeSwap
      void setFreeSwap(u_signed64 freeSwap) { m_freeSwap = freeSwap; }

      /// Accessor to freeSwap
      u_signed64 freeSwap()const { return m_freeSwap; }

      /// Accessor to lastStateUpdate
      void setLastStateUpdate(u_signed64 lastStateUpdate) {
        m_lastStateUpdate = lastStateUpdate;
      }

      /// Accessor to lastStateUpdate
      u_signed64 lastStateUpdate() const { return m_lastStateUpdate; }

      /// Accessor to lastMetricsUpdate
      void setLastMetricsUpdate(u_signed64 lastMetricsUpdate) {
        m_lastMetricsUpdate = lastMetricsUpdate;
      }

      /// Accessor to lastMetricsUpdate
      u_signed64 lastMetricsUpdate() const { return m_lastMetricsUpdate; }

      /// Accessor to readRate
      u_signed64 readRate() const { return m_readRate; }

      /// Accessor to readRate
      void setReadRate(u_signed64 readRate) { m_readRate = readRate; }

      /// Accessor to deltaReadRate
      signed64 deltaReadRate() const { return m_deltaReadRate; }

      /// Accessor to deltaReadRate
      void setDeltaReadRate(signed64 deltaReadRate) {
	m_deltaReadRate = deltaReadRate;
      }

      /// Accessor to writeRate
      u_signed64 writeRate() const { return m_writeRate; }

      /// Accessor to writeRate
      void setWriteRate(u_signed64 writeRate) { m_writeRate = writeRate; }

      /// Accessor to deltaWriteRate
      signed64 deltaWriteRate() const { return m_deltaWriteRate; }

      /// Accessor to deltaWriteRate
      void setDeltaWriteRate(signed64 deltaWriteRate) {
	m_deltaWriteRate = deltaWriteRate;
      }

      /// Accessor to nbReadStreams
      unsigned int nbReadStreams() const { return m_nbReadStreams; }

      /// Accessor to nbReadStreams
      void setNbReadStreams(unsigned int nbReadStreams) { m_nbReadStreams = nbReadStreams; }

      /// Accessor to deltaNbReadStreams
      int deltaNbReadStreams() const { return m_deltaNbReadStreams; }

      /// Accessor to deltaNbReadStreams
      void setDeltaNbReadStreams(int deltaNbReadStreams) {
	m_deltaNbReadStreams = deltaNbReadStreams;
      }

      /// Accessor to nbWriteStreams
      unsigned int nbWriteStreams() const { return m_nbWriteStreams; }

      /// Accessor to nbWriteStreams
      void setNbWriteStreams(unsigned int nbWriteStreams) { m_nbWriteStreams = nbWriteStreams; }

      /// Accessor to deltaNbWriteStreams
      int deltaNbWriteStreams() const { return m_deltaNbWriteStreams; }

      /// Accessor to deltaNbWriteStreams
      void setDeltaNbWriteStreams(int deltaNbWriteStreams) {
	m_deltaNbWriteStreams = deltaNbWriteStreams;
      }

      /// Accessor to nbReadWriteStreams
      unsigned int nbReadWriteStreams() const { return m_nbReadWriteStreams; }

      /// Accessor to nbReadWriteStreams
      void setNbReadWriteStreams(unsigned int nbReadWriteStreams) {
	m_nbReadWriteStreams = nbReadWriteStreams;
      }

      /// Accessor to deltaNbReadWriteStreams
      int deltaNbReadWriteStreams() const { return m_deltaNbReadWriteStreams; }

      /// Accessor to deltaNbReadWriteStreams
      void setDeltaNbReadWriteStreams(int deltaNbReadWriteStreams)
      { m_deltaNbReadWriteStreams = deltaNbReadWriteStreams; }

      /// Accessor to nbMigratorStreams
      unsigned int nbMigratorStreams() const { return m_nbMigratorStreams; }

      /// Accessor to nbMigratorStreams
      void setNbMigratorStreams(unsigned int nbMigratorStreams) { m_nbMigratorStreams = nbMigratorStreams; }

      /// Accessor to deltaNbMigratorStreams
      int deltaNbMigratorStreams() const { return m_deltaNbMigratorStreams; }

      /// Accessor to deltaNbMigratorStreams
      void setDeltaNbMigratorStreams(int deltaNbMigratorStreams) {
	m_deltaNbMigratorStreams = deltaNbMigratorStreams;
      }

      /// Accessor to nbRecallerStreams
      unsigned int nbRecallerStreams() const { return m_nbRecallerStreams; }

      /// Accessor to nbRecallerStreams
      void setNbRecallerStreams(unsigned int nbRecallerStreams) { m_nbRecallerStreams = nbRecallerStreams; }

      /// Accessor to deltaNbRecallerStreams
      int deltaNbRecallerStreams() const { return m_deltaNbRecallerStreams; }

      /// Accessor to deltaNbRecallerStreams
      void setDeltaNbRecallerStreams(int deltaNbRecallerStreams) {
	m_deltaNbRecallerStreams = deltaNbRecallerStreams;
      }

    private:

      /// Total ram (in bytes)
      u_signed64 m_ram;

      /// Total memory (in bytes)
      u_signed64 m_memory;

      /// Total swap (in bytes)
      u_signed64 m_swap;

      /// status
      castor::stager::DiskServerStatusCode m_status;

      /// admin status
      castor::monitoring::AdminStatusCodes m_adminStatus;

      /// Free ram (in bytes)
      u_signed64 m_freeRam;

      /// Free memory (in bytes)
      u_signed64 m_freeMemory;

      /// Free swap (in bytes)
      u_signed64 m_freeSwap;

      /// Last state update (seconds since EPOCH)
      u_signed64 m_lastStateUpdate;

      /// Last metrics update (seconds since EPOCH)
      u_signed64 m_lastMetricsUpdate;

      /// The number of bytes read per second, aggregated over all filesystems
      u_signed64 m_readRate;

      /// Delta on the number of bytes read per second, aggregated over all filesystems
      signed64 m_deltaReadRate;

      /// The number of bytes written per second, aggregated over all filesystems
      u_signed64 m_writeRate;

      /// Delta on the number of bytes written per second, aggregated over all filesystems
      signed64 m_deltaWriteRate;

      /// The number of read streams, aggregated over all filesystems
      unsigned int m_nbReadStreams;

      /// Delta on the number of read streams, aggregated over all filesystems
      int m_deltaNbReadStreams;

      /// The number of write streams, aggregated over all filesystems
      unsigned int m_nbWriteStreams;

      /// Delta on the number of write streams, aggregated over all filesystems
      int m_deltaNbWriteStreams;

      /// The number of read-write streams, aggregated over all filesystems
      unsigned int m_nbReadWriteStreams;

      /// Delta on the number of read-write streams, aggregated over all filesystems
      int m_deltaNbReadWriteStreams;

      /// The number of migrator streams, aggregated over all filesystems
      unsigned int m_nbMigratorStreams;

      /// Delta on the number of migrator streams, aggregated over all filesystems
      int m_deltaNbMigratorStreams;

      /// The number of recaller streams, aggregated over all filesystems
      unsigned int m_nbRecallerStreams;

      /// Delta on the number of recaller streams, aggregated over all filesystems
      int m_deltaNbRecallerStreams;

    }; // end DiskServerStatus

  } // end monitoring

} // end castor

#endif // MONITORING_DISKSERVERSTATUS_HPP
