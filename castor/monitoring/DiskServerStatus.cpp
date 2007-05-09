/******************************************************************************
 *                      DiskServerStatus.cpp
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
 * Describes the status of one disk server
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#include "castor/monitoring/DiskServerStatus.hpp"
#include "castor/stager/DiskServerStatusCode.hpp"
#include "u64subr.h"
#include <iostream>
#include <iomanip>

// -----------------------------------------------------------------------
// Constructor
// -----------------------------------------------------------------------
castor::monitoring::DiskServerStatus::DiskServerStatus() :
  m_ram(0), m_memory(0),
  m_swap(0), m_status(castor::stager::DISKSERVER_DISABLED),
  m_adminStatus(ADMIN_FORCE), m_freeRam(0),
  m_freeMemory(0), m_freeSwap(0),
  m_lastStateUpdate(0), m_lastMetricsUpdate(0),
  m_readRate(0), m_deltaReadRate(0),
  m_writeRate(0), m_deltaWriteRate(0),
  m_nbReadStreams(0), m_deltaNbReadStreams(0),
  m_nbWriteStreams(0), m_deltaNbWriteStreams(0),
  m_nbReadWriteStreams(0), m_deltaNbReadWriteStreams(0),
  m_nbMigratorStreams(0), m_deltaNbMigratorStreams(0),
  m_nbRecallerStreams(0), m_deltaNbRecallerStreams(0) { }

//------------------------------------------------------------------------------
// print
//------------------------------------------------------------------------------
void castor::monitoring::DiskServerStatus::print
(std::ostream& out, const std::string& indentation) const
  throw() {
  char ramBuf[21];
  char memoryBuf[21];
  char swapBuf[21];
  char freeRamBuf[21];
  char freeMemoryBuf[21];
  char freeSwapBuf[21];
  u64tostru(m_ram, ramBuf, 0);
  u64tostru(m_memory, memoryBuf, 0);
  u64tostru(m_swap, swapBuf, 0);
  u64tostru(m_freeRam, freeRamBuf, 0);
  u64tostru(m_freeMemory, freeMemoryBuf, 0);
  u64tostru(m_freeSwap, freeSwapBuf, 0);

  out << indentation << std::setw(24)
      << "ram" << ": " << ramBuf << "\n"
      << indentation << std::setw(24)
      << "memory" << ": " << memoryBuf << "\n"
      << indentation << std::setw(24)
      << "swap" << ": " << swapBuf << "\n"
      << indentation << std::setw(24)
      << "status" << ": "
      << castor::stager::DiskServerStatusCodeStrings[m_status] << "\n"
      << indentation << std::setw(24)
      << "adminStatus" << ": "
      << castor::monitoring::AdminStatusCodesStrings[m_adminStatus] << "\n"
      << indentation << std::setw(24)
      << "freeRam" << ": " << freeRamBuf << "\n"
      << indentation << std::setw(24)
      << "freeMemory" << ": " << freeMemoryBuf << "\n"
      << indentation << std::setw(24)
      << "freeSwap" << ": " << freeSwapBuf << "\n"
      << indentation << std::setw(24)
      << "lastStateUpdate" << ": " << m_lastStateUpdate << "\n"
      << indentation << std::setw(24)
      << "lastMetricsUpdate" << ": " << m_lastMetricsUpdate << "\n"
      << indentation << std::setw(24)
      << "readRate" << ": " << m_readRate << "\n"
      << indentation << std::setw(24)
      << "deltaReadRate" << ": " << m_deltaReadRate << "\n"
      << indentation << std::setw(24)
      << "writeRate" << ": " << m_writeRate << "\n"
      << indentation << std::setw(24)
      << "deltaWriteRate" << ": " << m_deltaWriteRate << "\n"
      << indentation << std::setw(24)
      << "nbReadStreams" << ": " << m_nbReadStreams << "\n"
      << indentation << std::setw(24)
      << "deltaNbReadStreams" << ": " << m_deltaNbReadStreams << "\n"
      << indentation << std::setw(24)
      << "nbWriteStreams" << ": " << m_nbWriteStreams << "\n"
      << indentation << std::setw(24)
      << "deltaNbWriteStreams" << ": " << m_deltaNbWriteStreams << "\n"
      << indentation << std::setw(24)
      << "nbReadWriteStreams" << ": " << m_nbReadWriteStreams << "\n"
      << indentation << std::setw(24)
      << "deltaNbReadWriteStreams" << ": " << m_deltaNbReadWriteStreams << "\n"
      << indentation << std::setw(24)
      << "nbMigratorStreams" << ": " << m_nbMigratorStreams << "\n"
      << indentation << std::setw(24)
      << "deltaNbMigratorStreams" << ": " << m_deltaNbMigratorStreams << "\n"
      << indentation << std::setw(24)
      << "nbRecallerStreams" << ": " << m_nbRecallerStreams << "\n"
      << indentation << std::setw(24)
      << "deltaNbRecallerStreams" << ": " << m_deltaNbRecallerStreams
      << std::endl;
  if (0 == size()) {
    out << indentation << "No filesystems registered"
        << std::endl;
  } else {
    std::string fsIndent = indentation + "   ";
    for (const_iterator it = begin(); it != end(); it++) {
      out << fsIndent << std::setw(24)
          << "mountPoint" << ": " << it->first << "\n";
      it->second.print(out, fsIndent);
    }
  }
}
