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

#include "scheduler/MountType.hpp"

#include <string>

namespace cta {
  /**
   * A placeholder class from which will derive ArchiveSession and RetriveSession.
   * It's just here to allow RTTI.
   */
  class TapeMount {
  public:

    /**
     * Returns The type of this tape mount.
     *
     * @return The type of this tape mount.
     */
    virtual MountType::Enum getMountType() const throw() = 0;

    /**
     * Returns the volume identifier of the tape to be mounted.
     *
     * @return The volume identifier of the tape to be mounted.
     */
    virtual std::string getVid() const throw() = 0;
    
    /**
     * Returns the density of the tape to be mounted.
     *
     * @return The density of the tape to be mounted.
     */
    virtual std::string getDensity() const throw() = 0;
    
    /**
     * Returns the mount transaction id.
     *
     * @return The mount transaction id.
     */
    virtual std::string getMountTransactionId() const throw() = 0;

    /**
     * Indicates that the mount was successful.
     *
     * @param checksumOfTransfer The adler-32 checksum of the file as calculated
     * during the execution of the job.
     * @param fileSizeOfTransfer The size of the file as calculated during the
     * execution of the job.
     */
    virtual void complete() = 0;

    /**
     * Indicates that the mount failed.
     *
     * @param ex The reason for the failure.
     */
    virtual void failed(const std::exception &ex) = 0;
    
    /**
     * Destructor.
     */
    virtual ~TapeMount() throw() = 0;

  }; // class TapeMount
  
}
