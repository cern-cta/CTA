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

// Include files
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

      /**
       * Constructor
       */
      DiskServerStatus();

      /**
       * print method
       */
      void print(std::ostream& out,
		 const std::string& indentation = "",
		 const bool showAll = true) const
        throw();

    public:

      /// Accessor to ram
      void setRam(u_signed64 new_var) {
	m_ram = new_var;
      }

      /// Accessor to ram
      u_signed64 ram() const {
	return m_ram;
      }

      /// Accessor to memory
      void setMemory(u_signed64 new_var) {
	m_memory = new_var;
      }

      /// Accessor to memory
      u_signed64 memory() const {
	return m_memory;
      }

      /// Accessor to swap
      void setSwap(u_signed64 new_var) {
	m_swap = new_var;
      }

      /// Accessor to swap
      u_signed64 swap() const {
	return m_swap;
      }

      /// Accessor to status
      void setStatus(castor::stager::DiskServerStatusCode new_var) {
        m_status = new_var;
      }

      /// Accessor to status
      castor::stager::DiskServerStatusCode status() const {
	return m_status;
      }

      /// Accessor to adminStatus
      void setAdminStatus(castor::monitoring::AdminStatusCodes new_var) {
        m_adminStatus = new_var;
      }

      /// Accessor to adminStatus
      castor::monitoring::AdminStatusCodes adminStatus() const {
        return m_adminStatus;
      }

      /// Accessor to freeRam
      void setFreeRam(u_signed64 new_var) {
	m_freeRam = new_var;
      }

      /// Accessor to freeRam
      u_signed64 freeRam() const {
	return m_freeRam;
      }

      /// Accessor to freeMemory
      void setFreeMemory(u_signed64 new_var) {
	m_freeMemory = new_var;
      }

      /// Accessor to freeMemory
      u_signed64 freeMemory() const {
	return m_freeMemory;
      }

      /// Accessor to freeSwap
      void setFreeSwap(u_signed64 new_var) {
	m_freeSwap = new_var;
      }

      /// Accessor to freeSwap
      u_signed64 freeSwap() const {
	return m_freeSwap;
      }

      /// Accessor to lastStateUpdate
      void setLastStateUpdate(u_signed64 new_var) {
        m_lastStateUpdate = new_var;
      }

      /// Accessor to lastStateUpdate
      u_signed64 lastStateUpdate() const {
	return m_lastStateUpdate;
      }

      /// Accessor to lastMetricsUpdate
      void setLastMetricsUpdate(u_signed64 new_var) {
        m_lastMetricsUpdate = new_var;
      }

      /// Accessor to lastMetricsUpdate
      u_signed64 lastMetricsUpdate() const {
	return m_lastMetricsUpdate;
      }

      /// Accessor to readRate
      u_signed64 readRate() const {
	return m_readRate;
      }

      /// Accessor to readRate
      void setReadRate(u_signed64 new_var) {
	m_readRate = new_var;
      }

      /// Accessor to deltaReadRate
      signed64 deltaReadRate() const {
	return m_deltaReadRate;
      }

      /// Accessor to deltaReadRate
      void setDeltaReadRate(signed64 new_var) {
	m_deltaReadRate = new_var;
      }

      /// Accessor to writeRate
      u_signed64 writeRate() const {
	return m_writeRate;
      }

      /// Accessor to writeRate
      void setWriteRate(u_signed64 new_var) {
	m_writeRate = new_var;
      }

      /// Accessor to deltaWriteRate
      signed64 deltaWriteRate() const {
	return m_deltaWriteRate;
      }

      /// Accessor to deltaWriteRate
      void setDeltaWriteRate(signed64 new_var) {
	m_deltaWriteRate = new_var;
      }

      /// Accessor to nbReadStreams
      unsigned int nbReadStreams() const {
	return m_nbReadStreams;
      }

      /// Accessor to nbReadStreams
      void setNbReadStreams(unsigned int new_var) {
	m_nbReadStreams = new_var;
      }

      /// Accessor to deltaNbReadStreams
      int deltaNbReadStreams() const {
	return m_deltaNbReadStreams;
      }

      /// Accessor to deltaNbReadStreams
      void setDeltaNbReadStreams(int new_var) {
	m_deltaNbReadStreams = new_var;
      }

      /// Accessor to nbWriteStreams
      unsigned int nbWriteStreams() const {
	return m_nbWriteStreams;
      }

      /// Accessor to nbWriteStreams
      void setNbWriteStreams(unsigned int new_var) {
	m_nbWriteStreams = new_var;
      }

      /// Accessor to deltaNbWriteStreams
      int deltaNbWriteStreams() const {
	return m_deltaNbWriteStreams;
      }

      /// Accessor to deltaNbWriteStreams
      void setDeltaNbWriteStreams(int new_var) {
	m_deltaNbWriteStreams = new_var;
      }

      /// Accessor to nbReadWriteStreams
      unsigned int nbReadWriteStreams() const {
	return m_nbReadWriteStreams;
      }

      /// Accessor to nbReadWriteStreams
      void setNbReadWriteStreams(unsigned int new_var) {
	m_nbReadWriteStreams = new_var;
      }

      /// Accessor to deltaNbReadWriteStreams
      int deltaNbReadWriteStreams() const {
	return m_deltaNbReadWriteStreams;
      }

      /// Accessor to deltaNbReadWriteStreams
      void setDeltaNbReadWriteStreams(int new_var) {
	m_deltaNbReadWriteStreams = new_var;
      }

      /// Accessor to nbMigratorStreams
      unsigned int nbMigratorStreams() const {
	return m_nbMigratorStreams;
      }

      /// Accessor to nbMigratorStreams
      void setNbMigratorStreams(unsigned int new_var) {
	m_nbMigratorStreams = new_var;
      }

      /// Accessor to deltaNbMigratorStreams
      int deltaNbMigratorStreams() const {
	return m_deltaNbMigratorStreams;
      }

      /// Accessor to deltaNbMigratorStreams
      void setDeltaNbMigratorStreams(int new_var) {
	m_deltaNbMigratorStreams = new_var;
      }

      /// Accessor to nbRecallerStreams
      unsigned int nbRecallerStreams() const {
	return m_nbRecallerStreams;
      }

      /// Accessor to nbRecallerStreams
      void setNbRecallerStreams(unsigned int new_var) {
	m_nbRecallerStreams = new_var;
      }

      /// Accessor to deltaNbRecallerStreams
      int deltaNbRecallerStreams() const {
	return m_deltaNbRecallerStreams;
      }

      /// Accessor to deltaNbRecallerStreams
      void setDeltaNbRecallerStreams(int new_var) {
	m_deltaNbRecallerStreams = new_var;
      }

      /// Accessor to toBeDeleted
      bool toBeDeleted() const {
        return m_toBeDeleted;
      }

      /// Accessor to toBeDeleted
      void setToBeDeleted(bool new_var) {
        m_toBeDeleted = new_var;
      }

    private:

      /// Total ram (in bytes)
      u_signed64 m_ram;

      /// Total memory (in bytes)
      u_signed64 m_memory;

      /// Total swap (in bytes)
      u_signed64 m_swap;

      /// Status
      castor::stager::DiskServerStatusCode m_status;

      /// Admin status
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

      /// Flag to indicate that the DiskServer should be deleted.
      bool m_toBeDeleted;

    }; /* end of class DiskServerStatus */

  } /* end of namespace monitoring */

} /* end of namespace castor */

#endif // MONITORING_DISKSERVERSTATUS_HPP
