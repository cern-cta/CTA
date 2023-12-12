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

#include <unistd.h>

#include "common/exception/Exception.hpp"

namespace cta {

/**
 * A smart file descriptor that owns a basic file descriptor. When the smart file descriptor goes out of scope,
 * it closes the file descriptor it owns.
 */
class SmartFd {
public:
  /**
   * Constructor
   *
   * @param fd The file descriptor to be owned
   */
  explicit SmartFd(int fd = -1) noexcept : m_fd(fd) { }

  ~SmartFd() { close(m_fd); }
  SmartFd& operator=(SmartFd& obj) = delete;
  SmartFd(const SmartFd& obj) = delete;

  /**
   * Take ownership of the specified file descriptor, closing the previously owned file descriptor if there is one
   *
   * @param fd The file descriptor to be owned
   */
  void reset(int fd = -1) noexcept {
    if(fd != m_fd) {
      if(m_fd >= 0) { close(m_fd); }
      m_fd = fd;
    }
  }

  /**
   * Returns the owned file descriptor
   */
  int get() const noexcept { return m_fd; }

  /**
   * Releases the owned file descriptor
   */
  int release() {
    if(m_fd < 0) {
      throw exception::Exception("No file descriptor is owned");
    }
    auto tmpFd = m_fd;
    m_fd = -1;
    return tmpFd;
  }

private:
  int m_fd;    //!< The owned file descriptor (-1 if this SmartFd does not own a file descriptor)
};

} // namespace cta
