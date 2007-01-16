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
#include "castor/monitoring/SharedMemoryAllocator.hpp"
#include "castor/monitoring/SharedMemoryString.hpp"
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
                      castor::monitoring::SharedMemoryAllocator
                      <std::pair<castor::monitoring::SharedMemoryString,
                                 castor::monitoring::FileSystemStatus> > > {

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

      /// Accessor to load
      void setLoad(unsigned int load) { m_load = load; }

      /// Accessor to load
      unsigned int load()const { return m_load; }

      /// Accessor to deltaLoad
      void setDeltaLoad(int deltaLoad) { m_deltaLoad = deltaLoad; }

      /// Accessor to deltaLoad
      int deltaLoad() const { return m_deltaLoad; }

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

      /// Load
      unsigned int m_load;

      /// DeltaLoad
      int m_deltaLoad;

      /// Last state update (seconds since EPOCH)
      u_signed64 m_lastStateUpdate;

      /// Last metrics update (seconds since EPOCH)
      u_signed64 m_lastMetricsUpdate;

    }; // end DiskServerStatus

  } // end monitoring

} // end castor

#endif // MONITORING_DISKSERVERSTATUS_HPP
