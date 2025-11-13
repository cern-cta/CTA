/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2025 CERN
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

#include "common/log/LogContext.hpp"

namespace cta {

// Note that only a single instance of signal reader should exist per process
class SignalReader {
public:
  /**
   * Constructor
   */
  SignalReader();

  /**
   * Destructor
   */
  ~SignalReader() noexcept;

  /**
   * Read, if any, all blocked signals and return them as a set.
   *
   * @param lc the log context for logging
   * @return a std::set containing the blocked signals
   */
  std::set<uint32_t> processAndGetSignals(log::LogContext& lc) const;

private:
  /**
    * File descriptor to read the blocked signals from.
    */
  int m_sigFd = -1;
};

}  // namespace cta
