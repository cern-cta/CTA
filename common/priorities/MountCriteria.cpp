/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "common/priorities/MountCriteria.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::MountCriteria::MountCriteria() : m_nbBytes(0), m_nbFiles(0), m_ageInSecs(0) {}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::MountCriteria::MountCriteria(const uint64_t nbBytes, const uint64_t nbFiles, const uint64_t ageInSecs)
    : m_nbBytes(nbBytes),
      m_nbFiles(nbFiles),
      m_ageInSecs(ageInSecs) {}

//------------------------------------------------------------------------------
// getNbBytes
//------------------------------------------------------------------------------
uint64_t cta::MountCriteria::getNbBytes() const noexcept {
  return m_nbBytes;
}

//------------------------------------------------------------------------------
// getNbFiles
//------------------------------------------------------------------------------
uint64_t cta::MountCriteria::getNbFiles() const noexcept {
  return m_nbFiles;
}

//------------------------------------------------------------------------------
// getAgeInSecs
//------------------------------------------------------------------------------
uint64_t cta::MountCriteria::getAgeInSecs() const noexcept {
  return m_ageInSecs;
}
