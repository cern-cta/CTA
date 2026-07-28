/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "mediachanger/LibrarySlotParser.hpp"

#include "common/exception/Exception.hpp"
#include "common/utils/utils.hpp"

#include <sstream>
#include <vector>

//------------------------------------------------------------------------------
// parse
//------------------------------------------------------------------------------
cta::mediachanger::LibrarySlot cta::mediachanger::LibrarySlotParser::parse(const std::string& str) {
  if (isDummy(str)) {
    return parseDummyLibrarySlot(str);
  }
  if (isScsi(str)) {
    return parseScsiLibrarySlot(str);
  }

  cta::exception::Exception ex;
  ex.getMessage() << "Cannot determine library slot type: str=" << str;
  throw ex;
}

//------------------------------------------------------------------------------
// isDummy
//------------------------------------------------------------------------------
bool cta::mediachanger::LibrarySlotParser::isDummy(const std::string& str) {
  return 0 == str.find("dummy");
}

//------------------------------------------------------------------------------
// isScsi
//------------------------------------------------------------------------------
bool cta::mediachanger::LibrarySlotParser::isScsi(const std::string& str) {
  return 0 == str.find("smc");
}

//------------------------------------------------------------------------------
// parseDummyLibrarySlot
//------------------------------------------------------------------------------
cta::mediachanger::LibrarySlot cta::mediachanger::LibrarySlotParser::parseDummyLibrarySlot(const std::string& str) {
  return LibrarySlot(0, true);
}

//------------------------------------------------------------------------------
// parseScsiLibrarySlot
//------------------------------------------------------------------------------
cta::mediachanger::LibrarySlot cta::mediachanger::LibrarySlotParser::parseScsiLibrarySlot(const std::string& str) {
  if (str.find("smc") == std::string::npos) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to construct SCSI LibrarySlot: Library slot must start with smc: slot=" << str;
    throw ex;
  }

  const size_t drvOrdStrLen = str.length() - 3;  // length of "smc" is 3
  const std::string drvOrdStr = str.substr(3, drvOrdStrLen);
  if (drvOrdStr.empty()) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to construct SCSI LibrarySlot: Missing drive ordinal: slot=" << str;
    throw ex;
  }

  if (!utils::isValidUInt(drvOrdStr)) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to construct SCSI LibrarySlot: Drive ordinal " << drvOrdStr
                    << " is not a valid unsigned integer: slot=" << str;
    throw ex;
  }

  const uint16_t drvOrd = atoi(drvOrdStr.c_str());
  return LibrarySlot(drvOrd);
}
