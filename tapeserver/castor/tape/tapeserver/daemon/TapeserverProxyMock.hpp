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

#include "tapeserver/daemon/TapedProxy.hpp"
#include <gmock/gmock.h>

namespace cta::tape::daemon {

/**
 * A mock taped-proxy.
 */
class TapeserverProxyMock final : public TapedProxy {
public:
  MOCK_METHOD(void, reportState, (const cta::tape::session::SessionState state, const cta::tape::session::SessionType type, const std::string& vid), (override));
  MOCK_METHOD(void, reportHeartbeat, (uint64_t totalTapeBytesMoved, uint64_t totalDiskBytesMoved), (override));
  MOCK_METHOD(void, addLogParams, (const std::list<cta::log::Param>& params), (override));
  MOCK_METHOD(void, deleteLogParams, (const std::list<std::string>& paramNames), (override));
  MOCK_METHOD(void, resetLogParams, (), (override));
  MOCK_METHOD(void, labelError, (const std::string& unitName, const std::string& message), (override));
  MOCK_METHOD(void, setRefreshLoggerHandler, (std::function<void()> handler), (override));
};

} // namespace castor::messages

