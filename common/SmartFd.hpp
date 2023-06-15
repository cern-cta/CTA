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

#include "common/exception/NotAnOwner.hpp"

namespace cta {

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
   * @param closedFd The value of the file descriptor that was closed.
   */
  typedef void (*ClosedCallback)(int closedFd);

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
   * Setting the callback function to nullptr means that no function will be
   * called.
   *
   * Please note any exception thrown by the callback function will be
   * ignored because the callback function maybe called by the destructor of
   * SmartFd.
   *
   * @param closedCallback This function will be called immediately after
   *                       the SmartFd has closed the file-descriptor it owns.
   *                       Please note that any exception thrown by this
   *                       function will be ignored because this function
   *                       maybe called by the destructor of SmartFd.
   */
  void setClosedCallback(ClosedCallback closedCallback) throw();

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
  SmartFd& operator=(SmartFd& obj);

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
  int get() const throw();

  /**
   * Releases the owned file descriptor.
   *
   * @return The released file descriptor.
   */
  int release();

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

  /**
   * Private copy-constructor to prevent users from trying to create a new
   * copy of an object of this class.
   * Not implemented so that it cannot be called
   */
  SmartFd(const SmartFd& obj) throw();

};  // class SmartFd

}  // namespace cta
