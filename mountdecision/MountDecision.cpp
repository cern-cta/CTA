/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "mountdecision/MountDecision.hpp"

#include "common/exception/Exception.hpp"
#include "common/semconv/Logging.hpp"

#include <exception>
#include <utility>

namespace cta::mountdecision {

MountDecision::MountDecision(ConnectionProvider connectionProvider, log::LogContext& lc) {
  try {
    m_db = std::make_unique<MountDecisionDB>(std::move(connectionProvider));
  } catch (cta::exception::Exception& ex) {
    log::ScopedParamContainer params(lc);
    params.add(semconv::log::exceptionMessage, ex.getMessageValue());
    lc.log(log::ERR,
           "In MountDecision::MountDecision(): Failed to initialise Mount Decision DB. "
           "Continuing without Mount Decision DB.");
  } catch (std::exception& ex) {
    log::ScopedParamContainer params(lc);
    params.add(semconv::log::exceptionMessage, ex.what());
    lc.log(log::ERR,
           "In MountDecision::MountDecision(): Failed to initialise Mount Decision DB. "
           "Continuing without Mount Decision DB.");
  }
}

bool MountDecision::isAvailable() const {
  return static_cast<bool>(m_db);
}

bool MountDecision::incrementCounter(const std::string& key, log::LogContext& lc, std::string_view operation) {
  if (!m_db) {
    return false;
  }

  try {
    m_db->incrementCounter(key);
    return true;
  } catch (cta::exception::Exception& ex) {
    log::ScopedParamContainer params(lc);
    params.add("mountDecisionCounterKey", key)
      .add("mountDecisionOperation", std::string(operation))
      .add(semconv::log::exceptionMessage, ex.getMessageValue());
    lc.log(log::ERR, "In MountDecision::incrementCounter(): Failed to increment counter.");
  } catch (std::exception& ex) {
    log::ScopedParamContainer params(lc);
    params.add("mountDecisionCounterKey", key)
      .add("mountDecisionOperation", std::string(operation))
      .add(semconv::log::exceptionMessage, ex.what());
    lc.log(log::ERR, "In MountDecision::incrementCounter(): Failed to increment counter.");
  }
  return false;
}

}  // namespace cta::mountdecision
