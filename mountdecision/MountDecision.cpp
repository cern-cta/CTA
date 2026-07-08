/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "mountdecision/MountDecision.hpp"

#include "common/exception/Exception.hpp"
#include "common/semconv/Logging.hpp"
#include "common/utils/utils.hpp"
#include "mountdecision/MountDecisionDBInit.hpp"

#include <exception>
#include <filesystem>
#include <system_error>
#include <utility>

namespace cta::mountdecision {

MountDecision::MountDecision(std::string ownerId, std::string configFile, uint64_t nbConns, log::LogContext& lc)
    : m_configFile(std::move(configFile)) {
  std::error_code existsError;
  const bool configFileExists = !m_configFile.empty() && std::filesystem::exists(m_configFile, existsError);
  if (!configFileExists) {
    log::ScopedParamContainer params(lc);
    params.add("mountDecisionDbConfigFile", m_configFile);
    if (existsError) {
      params.add(semconv::log::exceptionMessage, existsError.message());
    }
    lc.log(log::WARNING,
           "In MountDecision::MountDecision(): Mount Decision DB config file is missing. "
           "Continuing without Mount Decision DB.");
    return;
  }

  try {
    MountDecisionDBInit mountDecisionDbInit(ownerId, cta::utils::file2string(m_configFile), nbConns, lc.logger());
    m_db = mountDecisionDbInit.getDB(lc.logger());
  } catch (cta::exception::Exception& ex) {
    log::ScopedParamContainer params(lc);
    params.add("mountDecisionDbConfigFile", m_configFile).add(semconv::log::exceptionMessage, ex.getMessageValue());
    lc.log(log::ERR,
           "In MountDecision::MountDecision(): Failed to initialise Mount Decision DB. "
           "Continuing without Mount Decision DB.");
  } catch (std::exception& ex) {
    log::ScopedParamContainer params(lc);
    params.add("mountDecisionDbConfigFile", m_configFile).add(semconv::log::exceptionMessage, ex.what());
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
      .add("mountDecisionDbConfigFile", m_configFile)
      .add(semconv::log::exceptionMessage, ex.getMessageValue());
    lc.log(log::ERR, "In MountDecision::incrementCounter(): Failed to increment counter.");
  } catch (std::exception& ex) {
    log::ScopedParamContainer params(lc);
    params.add("mountDecisionCounterKey", key)
      .add("mountDecisionOperation", std::string(operation))
      .add("mountDecisionDbConfigFile", m_configFile)
      .add(semconv::log::exceptionMessage, ex.what());
    lc.log(log::ERR, "In MountDecision::incrementCounter(): Failed to increment counter.");
  }
  return false;
}

}  // namespace cta::mountdecision
