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
     * A pointer to a callback function that will called by the Smart
     * immediately after the SmartFd has closed the file-descriptor it owns.
     *
     * Please note that any exception thrown by this function will be ignored
     * because this function maybe called by the destructor of SmartFd.
     *
     * @param closedfd The value of the file descriptor that was closed.
     */
    typedef void (*ClosedCallback)(int closedfd);

    /**
     * Constructor.
     *
     */
    SmartFd() throw();

    /**
     * Constructor.
     *
     * @param fd The file descriptor to be owned by the smart file
     *           descriptor.
     */
    SmartFd(const int fd) throw();

    /**
     * Sets the function to be called back by the SmartFd immediately after
     * the SmartFd has closed the file-descriptor it owns.
     *
     * Setting the callback function to NULL means that no function will be
     * called.
     *
     * Please note any exception thrown by the callback function will be
     * ignored because the callback function maybe called by the destructor of
     * SmartFd.
     *
     * @param fd             The file descriptor to be owned by the smart file
     *                       descriptor.
     * @param closedCallback This function will be called immediately after
     *                       the SmartFd has closed the file-descriptor it owns.
     *                       Please note that any exception thrown by this
     *                       function will be ignored because this function
     *                       maybe called by the destructor of SmartFd.
     */
    void setClosedCallback(const int fd, ClosedCallback closedCallback) throw();

    /**
     * Take ownership of the specified file descriptor, closing the previously
     * owned file descriptor if there is one and it is not the same as the one
     * specified.
     *
     * @param fd The file descriptor to be owned, defaults to -1 if not
     *           specified, where a negative number means this SmartFd does not
     *           own a file descriptor.
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
     * Closes the owned file descriptor if there is one.
     */
    ~SmartFd();

    /**
     * Returns the owned file descriptor or a negative number if this SmartFd
     * does not own a file descriptor.
     *
     * @return The owned file desccriptor.
     */
    int get() throw();

    /**
     * Releases the owned file descriptor.
     *
     * @return The released file descriptor.
     */
    int release() throw(castor::exception::Exception);


  private:

    /**
     * The owned file descriptor.  A negative value means this SmartFd does not
     * own a file descriptor..
     */
    int m_fd;

    /**
     * The function to be called immediately after the SmartFd has closed its
     * file-descriptor.  A value of null means no function will be called.
     */
    ClosedCallback m_closedCallback;

  };

} // namespace utils
} // namespace tape
} // namespace castor

#endif // CASTOR_TAPE_UTILS_SMARTFD
