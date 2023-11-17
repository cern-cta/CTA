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

#pragma once

namespace cta::mediachanger {

/**
 * Enumeration of the different types of tape library.
 */
enum TapeLibraryType {
  TAPE_LIBRARY_TYPE_NONE,
  TAPE_LIBRARY_TYPE_DUMMY,
  TAPE_LIBRARY_TYPE_SCSI};
  
/**
 * Thread safe method that returns the string representation of the
 * specified tape library.
 *
 * Please note that this function does not throw an exception.  If the
 * tape-library type is not known then an appropriate string is returned.
 */
const char *tapeLibraryTypeToString(const TapeLibraryType libraryType);
  
} // namespace cta::mediachanger
