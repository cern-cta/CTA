/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2021 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "common/exception/InvalidConfigEntry.hpp"

// -----------------------------------------------------------------------------
// Constructor
// -----------------------------------------------------------------------------
cta::exception::InvalidConfigEntry::InvalidConfigEntry(
  const char *const entryCategory, const char *const entryName,
  const char *const entryValue) :
  cta::exception::Exception(), m_entryCategory(entryCategory),
  m_entryName(entryName), m_entryValue(entryValue) {
}


// -----------------------------------------------------------------------------
// getEntryCategory()
// -----------------------------------------------------------------------------
const std::string &cta::exception::InvalidConfigEntry::getEntryCategory() {
  return m_entryCategory;
}


// -----------------------------------------------------------------------------
// getEntryName()
// -----------------------------------------------------------------------------
const std::string &cta::exception::InvalidConfigEntry::getEntryName() {
  return m_entryName;
}


// -----------------------------------------------------------------------------
// getEntryValue()
// -----------------------------------------------------------------------------
const std::string &cta::exception::InvalidConfigEntry::getEntryValue()
{
  return m_entryValue;
}
