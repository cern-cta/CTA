/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
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
