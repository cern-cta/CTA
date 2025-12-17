/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "mediachanger/TapeLibraryType.hpp"

//------------------------------------------------------------------------------
// tapeLibraryTypeToString
//------------------------------------------------------------------------------
const char* cta::mediachanger::tapeLibraryTypeToString(const TapeLibraryType libraryType) {
  switch (libraryType) {
    case TAPE_LIBRARY_TYPE_NONE:
      return "NONE";
    case TAPE_LIBRARY_TYPE_DUMMY:
      return "DUMMY";
    case TAPE_LIBRARY_TYPE_SCSI:
      return "SCSI";
    default:
      return "UNKNOWN";
  }
}
