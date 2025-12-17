/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

namespace cta::mediachanger {

/**
 * Enumeration of the different types of tape library.
 */
enum TapeLibraryType { TAPE_LIBRARY_TYPE_NONE, TAPE_LIBRARY_TYPE_DUMMY, TAPE_LIBRARY_TYPE_SCSI };

/**
 * Thread safe method that returns the string representation of the
 * specified tape library.
 *
 * Please note that this function does not throw an exception.  If the
 * tape-library type is not known then an appropriate string is returned.
 */
const char* tapeLibraryTypeToString(const TapeLibraryType libraryType);

}  // namespace cta::mediachanger
