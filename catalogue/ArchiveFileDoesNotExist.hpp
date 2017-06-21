/*
 * The CERN Tape Archive(CTA) project
 * Copyright(C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 *(at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include "common/exception/UserError.hpp"

namespace cta {

namespace catalogue {

/**
 * Archive file does not exist in the CTA catalogue.
 */
class ArchiveFileDoesNotExist: public exception::UserError {
public:
  /**
   * The unique identifier of the archive file.
   */
  uint64_t archiveFileId;
};

} // namespace catalogue
} // namespace cta
