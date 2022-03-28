/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
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
