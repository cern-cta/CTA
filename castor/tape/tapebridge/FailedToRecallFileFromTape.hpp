/******************************************************************************
 *                 castor/tape/tapebridge/FailedToRecallFileFromTape.hpp
 *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2003  CERN
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 *
 *
 *
 * @author Steven.Murray@cern.ch
 *****************************************************************************/

#ifndef CASTOR_TAPE_TAPEBRIDGE_FAILEDTORECALLFILEFROMTAPE_HPP 
#define CASTOR_TAPE_TAPEBRIDGE_FAILEDTORECALLFILEFROMTAPE_HPP 1

#include "castor/tape/tapebridge/FailedToCopyTapeFile.hpp"

namespace castor     {
namespace tape       {
namespace tapebridge {

/**
 * Failed to migrate a file to tape.
 */
class FailedToRecallFileFromTape : public FailedToCopyTapeFile {
public:

  /**
   * Constructor
   *
   * @param se         The serrno code of the corresponding C error.
   * @param failedFile Information about the failed file.
   */
  FailedToRecallFileFromTape(const int se, const FailedFile &failedFile)
    throw();

  /**
   * Destructor.
   */
  ~FailedToRecallFileFromTape();
}; // class FailedToRecallFileFromTape
      
} // namespace tapebridge
} // namespace tape
} // namespace castor

#endif // CASTOR_TAPE_TAPEBRIDGE_FAILEDTORECALLFILEFROMTAPE_HPP
