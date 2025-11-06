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

#include <string>
#include <optional>
#include "common/Timer.hpp"

namespace castor::tape::tapeserver::daemon {

// Used to track the number of active transfer tasks (e.g. DiskReadTask, TapeReadTask, etc..)
class TransferTaskTracker {
public:
  TransferTaskTracker(std::string_view ioDirection, std::string_view ioMedium);
  ~TransferTaskTracker();
  TransferTaskTracker (const TransferTaskTracker&) = delete;
  TransferTaskTracker& operator= (const TransferTaskTracker&) = delete;

private:
  std::string m_ioDirection;
  std::string m_ioMedium;
};

}  // namespace castor::tape::tapeserver::daemon
