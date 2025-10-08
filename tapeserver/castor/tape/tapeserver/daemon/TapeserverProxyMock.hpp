/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "tapeserver/daemon/TapedProxy.hpp"
#include <gmock/gmock.h>

namespace cta::tape::daemon {

/**
 * A mock taped-proxy.
 */
class TapeserverProxyMock : public TapedProxy {
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

