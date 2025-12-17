/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "common/exception/InvalidConfigEntry.hpp"

// -----------------------------------------------------------------------------
// Constructor
// -----------------------------------------------------------------------------
cta::exception::InvalidConfigEntry::InvalidConfigEntry(const char* const entryCategory,
                                                       const char* const entryName,
                                                       const char* const entryValue)
    : cta::exception::Exception(),
      m_entryCategory(entryCategory),
      m_entryName(entryName),
      m_entryValue(entryValue) {}

// -----------------------------------------------------------------------------
// getEntryCategory()
// -----------------------------------------------------------------------------
const std::string& cta::exception::InvalidConfigEntry::getEntryCategory() {
  return m_entryCategory;
}

// -----------------------------------------------------------------------------
// getEntryName()
// -----------------------------------------------------------------------------
const std::string& cta::exception::InvalidConfigEntry::getEntryName() {
  return m_entryName;
}

// -----------------------------------------------------------------------------
// getEntryValue()
// -----------------------------------------------------------------------------
const std::string& cta::exception::InvalidConfigEntry::getEntryValue() {
  return m_entryValue;
}
