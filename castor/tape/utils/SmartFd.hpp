/******************************************************************************
 *                      castor/tape/utils/SmartFd.hpp
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

#ifndef CASTOR_TAPE_UTILS_SMARTFD
#define CASTOR_TAPE_UTILS_SMARTFD

#include "castor/exception/Exception.hpp"


namespace castor {
namespace tape   {
namespace utils  {

  /**
   * A smart file descriptor that owns a basic file descriptor.  When the smart
   * file descriptor goes out of scope, it will close the file descriptor it
   * owns.
   */
  class SmartFd {

  public:

    /**
     * Constructor.
     *
     */
    SmartFd();

    /**
     * Constructor.
     *
     * @param fd The file descriptor to be owned by the smart file
     * descriptor.
     */
    SmartFd(const int fd);

    /**
     * Take ownership of the specified file descriptor, closing the previously
     * owned file descriptor if there is one and it is not the same as the one
     * specified.
     *
     * @param File descriptor to be owned, defaults to -1 if not specified,
     * where  a negative number means this SmartFd does not own anything.
     */
    void reset(const int fd) throw();

    /**
     * SmartFd assignment operator.
     *
     * This function does the following:
     * <ul>
     * <li> Calls release on the previous owner (obj);
     * <li> Closes the file descriptor of this object if it already owns one.
     * <li> Makes this object the owner of the file descriptor released from
     *      the previous owner (obj).
     * </ul>
     */
    SmartFd &operator=(SmartFd& obj) throw();

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
     * descructor of the smart file descriptor will not close the now
     * previously-owned file descriptor.
     *
     * @return The released file descriptor.
     */
    int release() throw(castor::exception::Exception);


  private:

    /**
     * The owned file descriptor.  A value less than zero indicates release
     * has been called and the smart file descriptor no longer owns a basic
     * file descriptor.
     */ 
    int m_fd;

  };

} // namespace utils
} // namespace tape
} // namespace castor

#endif // CASTOR_TAPE_UTILS_SMARTFD
