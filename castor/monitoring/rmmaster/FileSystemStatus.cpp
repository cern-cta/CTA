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
 * 
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#include "castor/rmmaster/FileSystemStatus.hpp"
#include <iostream>

// -----------------------------------------------------------------------
// Constructor
// -----------------------------------------------------------------------
castor::rmmaster::FileSystemStatus::FileSystemStatus
(u_signed64 id) : m_id(id) { }

//------------------------------------------------------------------------------
// print
//------------------------------------------------------------------------------
void castor::rmmaster::FileSystemStatus::print
(std::ostream& out) const throw() {
  out << "[" << m_id << "] "
      << " (W=" << m_weight << ", DW="
      << m_deltaWeight << ", D="
      << m_deviation << ", AS="
      << m_availableSpace << ")" << std::endl;
}
