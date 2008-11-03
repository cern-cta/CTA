/******************************************************************************
 *                      InvalidConfigEntry.cpp
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
 * 
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#include "castor/exception/InvalidConfigEntry.hpp"

#include <errno.h>


// -----------------------------------------------------------------------------
// Constructor
// -----------------------------------------------------------------------------
castor::exception::InvalidConfigEntry::InvalidConfigEntry(
  char *entryCategory, char *entryName, char *entryValue) :
  castor::exception::Exception(EINVAL), m_entryCategory(entryCategory),
  m_entryName(entryName), m_entryValue(entryValue) {
}


// -----------------------------------------------------------------------------
// getEntryCategory()
// -----------------------------------------------------------------------------
const std::string &castor::exception::InvalidConfigEntry::getEntryCategory() {
  return m_entryCategory;
}


// -----------------------------------------------------------------------------
// getEntryName()
// -----------------------------------------------------------------------------
const std::string &castor::exception::InvalidConfigEntry::getEntryName() {
  return m_entryName;
}


// -----------------------------------------------------------------------------
// getEntryValue()
// -----------------------------------------------------------------------------
const std::string &castor::exception::InvalidConfigEntry::getEntryValue()
{
  return m_entryValue;
}
