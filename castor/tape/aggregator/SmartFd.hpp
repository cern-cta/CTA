/******************************************************************************
 *                      castor/tape/aggregator/SmartFd.hpp
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
 * @author Nicola.Bessone@cern.ch Steven.Murray@cern.ch
 *****************************************************************************/

#ifndef CASTOR_TAPE_AGGREGATOR_SMARTFD
#define CASTOR_TAPE_AGGREGATOR_SMARTFD

#include "castor/exception/Exception.hpp"


namespace castor     {
namespace tape       {
namespace aggregator {

  /**
   * A smart file descriptor that owns a basic file descriptor.  The smart file
   * descriptor goes out of scope it will close the file descriptor it owns.
   */
  class SmartFd {

  public:

    /**
     * Constructor.
     *
     * @param fileDescriptor The file descriptor to be owned by the smart file
     * descriptor.
     */
    SmartFd(const int fileDescriptor);

    /**
     * Destructor.
     *
     * Closes the owned file descriptor if release() has not been called
     * previously.
     */
    ~SmartFd();

    /**
     * Returns the owned file descriptor.
     *
     * @return The owned file desccriptor.
     */
    int get() throw(castor::exception::Exception);

    /**
     * Releases the owned file descriptor.  After a call to this function, the
     * descructor will the smart file descriptor will not close the owned file
     * descriptor.
     *
     * @return The owned file descriptor.
     */
    int release() throw(castor::exception::Exception);


  private:

    /**
     * The owned file descriptor.  A value less than zero indicates release
     * has been called and the smart file descriptor no longer owns a basic
     * file descriptor.
     */ 
    int m_fileDescriptor;

  };

} // namespace aggregator
} // namespace tape
} // namespace castor

#endif // CASTOR_TAPE_AGGREGATOR_SMARTFD
