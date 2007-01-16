/******************************************************************************
 *                      FileSystemStatus.cpp
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

#include "castor/monitoring/FileSystemStatus.hpp"
#include "castor/monitoring/AdminStatusCodes.hpp"
#include "castor/stager/FileSystemStatusCodes.hpp"
#include <iostream>
#include <iomanip>

// -----------------------------------------------------------------------
// Constructor
// -----------------------------------------------------------------------
castor::monitoring::FileSystemStatus::FileSystemStatus() :
  m_space(0),
  m_status(castor::stager::FILESYSTEM_DISABLED),
  m_adminStatus(ADMIN_FORCE),
  m_readRate(0), m_deltaReadRate(0),
  m_writeRate(0), m_deltaWriteRate(0),
  m_nbReadStreams(0), m_deltaNbReadStreams(0),
  m_nbWriteStreams(0), m_deltaNbWriteStreams(0),
  m_nbReadWriteStreams(0), m_deltaNbReadWriteStreams(0),
  m_freeSpace(0), m_deltaFreeSpace(0),
  m_lastStateUpdate(0), m_lastMetricsUpdate(0) { }

//------------------------------------------------------------------------------
// print
//------------------------------------------------------------------------------
void castor::monitoring::FileSystemStatus::print
(std::ostream& out, const std::string& indentation) const
  throw() {
  out << indentation << std::setw(20)
      << "space" << ": " << m_space << "\n"
      << indentation << std::setw(20)
      << "status" << ": "
      << castor::stager::FileSystemStatusCodesStrings[m_status] << "\n"
      << indentation << std::setw(20)
      << "adminStatus" << ": "
      << castor::monitoring::AdminStatusCodesStrings[m_adminStatus] << "\n"
      << indentation << std::setw(20)
      << "readRate" << ": " << m_readRate << "\n"
      << indentation << std::setw(20)
      << "deltaReadRate" << ": " << m_deltaReadRate << "\n"
      << indentation << std::setw(20)
      << "writeRate" << ": " << m_writeRate << "\n"
      << indentation << std::setw(20)
      << "deltaWriteRate" << ": " << m_deltaWriteRate << "\n"
      << indentation << std::setw(20)
      << "nbReadStreams" << ": " << m_nbReadStreams << "\n"
      << indentation << std::setw(20)
      << "deltaNbReadStreams" << ": " << m_deltaNbReadStreams << "\n"
      << indentation << std::setw(20)
      << "nbWriteStreams" << ": " << m_nbWriteStreams << "\n"
      << indentation << std::setw(20)
      << "deltaNbWriteStreams" << ": " << m_deltaNbWriteStreams << "\n"
      << indentation << std::setw(20)
      << "nbReadWriteStreams" << ": " << m_nbReadWriteStreams << "\n"
      << indentation << std::setw(20)
      << "deltaNbReadWriteStreams" << ": " << m_deltaNbReadWriteStreams << "\n"
      << indentation << std::setw(20)
      << "freeSpace" << ": " << m_freeSpace << "\n"
      << indentation << std::setw(20)
      << "deltaFreeSpace" << ": " << m_deltaFreeSpace << "\n"
      << indentation << std::setw(20)
      << "lastStateUpdate" << ": " << m_lastStateUpdate << "\n"
      << indentation << std::setw(20)
      << "lastMetricsUpdate" << ": " << m_lastMetricsUpdate
      << std::endl;
}
