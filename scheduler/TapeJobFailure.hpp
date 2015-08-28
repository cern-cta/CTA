/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
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

namespace cta {

  /**
   * An enumeration of the different types of tape job failures.
   */
  enum TapeJobFailure {
    JOBFAILURE_NONE,
    JOBFAILURE_TAPEDRIVE,
    JOBFAILURE_TAPELIBRARY,
    JOBFAILURE_REMOTESTORAGE
  };

  /**
   * Thread safe method that returns the string representation of the specified
   * enumeration value.
   *
   * @param enumValue The integer value of the type.
   * @return The string representation.
   */
  const char *TapeJobFailureToStr(const TapeJobFailure enumValue) throw();

} // namespace cta
