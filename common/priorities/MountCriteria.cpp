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

#include "common/priorities/MountCriteria.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::MountCriteria::MountCriteria():
  m_nbBytes(0),
  m_nbFiles(0),
  m_ageInSecs(0) {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::MountCriteria::MountCriteria(const uint64_t nbBytes, const uint64_t nbFiles,
  const uint64_t ageInSecs):
  m_nbBytes(nbBytes),
  m_nbFiles(nbFiles),
  m_ageInSecs(ageInSecs) {
}

//------------------------------------------------------------------------------
// getNbBytes
//------------------------------------------------------------------------------
uint64_t cta::MountCriteria::getNbBytes() const throw() {
  return m_nbBytes;
}

//------------------------------------------------------------------------------
// getNbFiles
//------------------------------------------------------------------------------
uint64_t cta::MountCriteria::getNbFiles() const throw() {
  return m_nbFiles;
}

//------------------------------------------------------------------------------
// getAgeInSecs
//------------------------------------------------------------------------------
uint64_t cta::MountCriteria::getAgeInSecs() const throw() {
  return m_ageInSecs;
}
