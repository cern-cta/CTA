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
#include <iostream>
#include <iomanip>

// -----------------------------------------------------------------------
// Constructor
// -----------------------------------------------------------------------
castor::monitoring::DiskServerStatus::DiskServerStatus
(u_signed64 id) :
  m_id(id), m_ram(0), m_memory(0),
  m_swap(0), m_status(castor::stager::DISKSERVER_DISABLED),
  m_adminStatus(ADMIN_FORCE), m_freeRam(0),
  m_freeMemory(0), m_freeSwap(0), m_load(0),
  m_lastStateUpdate(0), m_lastMetricsUpdate(0) { }

//------------------------------------------------------------------------------
// print
//------------------------------------------------------------------------------
void castor::monitoring::DiskServerStatus::print
(std::ostream& out, const std::string& indentation) const
  throw() {
  out << indentation << std::setw(20)
      << "id" << ": " << m_id << "\n"
      << indentation << std::setw(20)
      << "ram" << ": " << m_ram << "\n"
      << indentation << std::setw(20)
      << "memory" << ": " << m_memory << "\n"
      << indentation << std::setw(20)
      << "swap" << ": " << m_swap << "\n"
      << indentation << std::setw(20)
      << "status" << ": " << m_status << "\n"
      << indentation << std::setw(20)
      << "adminStatus" << ": " << m_adminStatus << "\n"
      << indentation << std::setw(20)
      << "freeRam" << ": " << m_freeRam << "\n"
      << indentation << std::setw(20)
      << "freeMemory" << ": " << m_freeMemory << "\n"
      << indentation << std::setw(20)
      << "freeSwap" << ": " << m_freeSwap << "\n"
      << indentation << std::setw(20)
      << "load" << ": " << m_load << "\n"
      << indentation << std::setw(20)
      << "lastStateUpdate" << ": " << m_lastStateUpdate << "\n"
      << indentation << std::setw(20)
      << "lastMetricsUpdate" << ": " << m_lastMetricsUpdate
      << std::endl;
  if (0 == size()) {
    out << indentation << "No filesystems registered"
        << std::endl;
  } else {
    std::string fsIndent = indentation + "   ";
    for (const_iterator it = begin(); it != end(); it++) {
      out << fsIndent << std::setw(20)
          << "mountPoint" << ": " << it->first;
      it->second.print(out, fsIndent);
    }
  }
}
