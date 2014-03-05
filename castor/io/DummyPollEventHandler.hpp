/******************************************************************************
 *                castor/io/DummyPollEventHandler.hpp
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
 * @author Steven.Murray@cern.ch
 *****************************************************************************/

#ifndef CASTOR_IO_DUMMYPOLLEVENTHANDLER_HPP
#define CASTOR_IO_DUMMYPOLLEVENTHANDLER_HPP 1

#include "castor/io/PollEventHandler.hpp"

namespace castor {
namespace io {

/**
 * This is a dummy poll() event-handler that is intended to be used to write
 * unit tests.
 */
class DummyPollEventHandler: public PollEventHandler {
public:
  /**
   * Constructor.
   *
   * @param fd File descriptor to be returned by getFd().
   */
  DummyPollEventHandler(const int fd) throw();

  /**
   * Returns the integer file descriptor of this event handler.
   */
  int getFd() throw();

  /**
   * Sets each of the fields of the specified poll() file-descriptor to 0.
   */
  void fillPollFd(struct pollfd &fd) throw();

  /**
   * Does nothing.
   *
   * @param fd The poll file-descriptor describing the event.
   */
  void handleEvent(const struct pollfd &fd) throw(castor::exception::Exception);

  /**
   * Destructor.
   */
  ~DummyPollEventHandler() throw();

private:

  /**
   * File descriptor to be returned by getFd().
   */
  const int m_fd;

}; // class DummyPollEventHandler

} // namespace io
} // namespace castor

#endif // CASTOR_IO_DUMMYPOLLEVENTHANDLER_HPP
