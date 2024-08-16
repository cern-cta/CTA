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

#include <string>
#include <pthread.h>

namespace cta::exception {

class Backtrace {
public:
  explicit Backtrace(bool fake = false);

  const std::string& str() const { return m_trace; }

private:
  std::string m_trace;
  /**
   * Singleton lock around the apparently racy backtrace().
   * We write it with no error check as it's used only here.
   * We need a class in order to have a constructor for the global object.
   */
  class mutex {
  public:
    mutex() { pthread_mutex_init(&m_mutex, nullptr); }
    void lock() { pthread_mutex_lock(&m_mutex); }
    void unlock() { pthread_mutex_unlock(&m_mutex); }
  private:
    pthread_mutex_t m_mutex;
  };
  static mutex g_lock;
};

} // namespace cta::exception
