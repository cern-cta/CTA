/******************************************************************************
 *                castor/tape/tapebridge/IFileCloser.hpp
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

#ifndef CASTOR_TAPE_TAPEBRIDGE_IFILECLOSER_HPP
#define CASTOR_TAPE_TAPEBRIDGE_IFILECLOSER_HPP 1


namespace castor     {
namespace tape       {
namespace tapebridge {

/**
 * Abstract class defining the interface of an object responsible for closing
 * file-descriptors.  The main goal of this interface is to facilitate
 * unit-testing.
 */
class IFileCloser {
public:

  /**
   * Virtual destructor.
   */
  virtual ~IFileCloser() throw() {
    // Do nothing
  }

  /**
   * Closes the specified file-descriptor.
   *
   * @param fd The file-descriptor to be closed.
   * @return   The return value as defined by the the system close() function.
   */
  virtual int close(const int fd) = 0;

}; // class IFileCloser

} // namespace tapebridge
} // namespace tape
} // namespace castor

#endif // CASTOR_TAPE_TAPEBRIDGE_IFILECLOSER_HPP
