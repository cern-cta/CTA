/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "common/exception/Exception.hpp"
#include "common/utils/utils.hpp"
#include "mediachanger/LibrarySlotParser.hpp"
#include "mediachanger/DummyLibrarySlot.hpp"
#include "mediachanger/ScsiLibrarySlot.hpp"

#include <sstream>
#include <vector>

//------------------------------------------------------------------------------
// parse
//------------------------------------------------------------------------------
cta::mediachanger::LibrarySlot *cta::mediachanger::LibrarySlotParser::
  parse(const std::string &str) {
  try {
    // Parse the string representation in two steps, first parsing the beginning
    // to get the library type and then the rst to get the details.  This two
    // step strategy gives the end user more detailed syntax errors wherei
    // necessary
    const TapeLibraryType libraryType = getLibrarySlotType(str);
    return parse(libraryType, str);
  } catch(cta::exception::Exception &ne) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to parse library slot from string"
      " representation: " << ne.getMessage().str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// getLibrarySlotType
//------------------------------------------------------------------------------
cta::mediachanger::TapeLibraryType cta::mediachanger::LibrarySlotParser::
  getLibrarySlotType(const std::string &str) {
  if(isDummy(str)) return TAPE_LIBRARY_TYPE_DUMMY;
  if(isScsi(str))  return TAPE_LIBRARY_TYPE_SCSI;

  cta::exception::Exception ex;
  ex.getMessage() << "Cannot determine library slot type: str=" << str;
  throw ex;
}

//------------------------------------------------------------------------------
// isDummy
//------------------------------------------------------------------------------
bool cta::mediachanger::LibrarySlotParser::isDummy(const std::string &str)
{
  return 0 == str.find("dummy");
}

//------------------------------------------------------------------------------
// isScsi
//------------------------------------------------------------------------------
bool cta::mediachanger::LibrarySlotParser::isScsi(const std::string &str)
  {
  return 0 == str.find("smc");
}

//------------------------------------------------------------------------------
// parse
//------------------------------------------------------------------------------
cta::mediachanger::LibrarySlot *cta::mediachanger::LibrarySlotParser::
  parse(const TapeLibraryType libraryType, const std::string &str) {

  switch(libraryType) {
  case TAPE_LIBRARY_TYPE_DUMMY:  return parseDummyLibrarySlot(str);
  case TAPE_LIBRARY_TYPE_SCSI:   return parseScsiLibrarySlot(str);
  default:
    {
      // Should never get here
      cta::exception::Exception ex;
      ex.getMessage() << "Unknown tape library type: libraryType=" <<
        libraryType;
      throw ex;
    }
  }
}

//------------------------------------------------------------------------------
// parseDummyLibrarySlot
//------------------------------------------------------------------------------
cta::mediachanger::DummyLibrarySlot *cta::mediachanger::
  LibrarySlotParser::parseDummyLibrarySlot(const std::string &str) {
  return new DummyLibrarySlot(str);
}


//------------------------------------------------------------------------------
// parseScsiLibrarySlot
//------------------------------------------------------------------------------
cta::mediachanger::ScsiLibrarySlot *cta::mediachanger::
  LibrarySlotParser::parseScsiLibrarySlot(const std::string &str) {
  if(str.find("smc") == std::string::npos) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to construct ScsiLibrarySlot: Library slot must start with smc: slot=" << str;
    throw ex;
  }

  const size_t drvOrdStrLen = str.length() - 3; // length of "smc" is 3
  const std::string drvOrdStr = str.substr(3, drvOrdStrLen);
  if(drvOrdStr.empty()) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to construct ScsiLibrarySlot: Missing drive ordinal: slot=" << str;
    throw ex;
  }

  if(!utils::isValidUInt(drvOrdStr)) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to construct ScsiLibrarySlot: Drive ordinal " <<
      drvOrdStr << " is not a valid unsigned integer: slot=" << str;
    throw ex;
  }

  const uint16_t drvOrd = atoi(drvOrdStr.c_str());
  return new ScsiLibrarySlot(drvOrd);
}
