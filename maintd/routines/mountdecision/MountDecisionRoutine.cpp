/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "MountDecisionRoutine.hpp"

#include "common/exception/Exception.hpp"
#include "common/semconv/Logging.hpp"
#include "common/utils/utils.hpp"
#include "mountdecision/MountDecisionDBInit.hpp"

#include <cstdlib>
#include <exception>
#include <filesystem>
#include <system_error>

namespace cta::maintd {

namespace {

std::string getMountDecisionCounterKey() {
  const auto hostName = cta::utils::getShortHostname();
  const char* const myName = std::getenv("MY_NAME");
  if (myName != nullptr && myName[0] != '\0') {
    return "cta-maintd:" + std::string(myName) + ":" + hostName;
  }
  return "cta-maintd:" + hostName;
}

}  // namespace

MountDecisionRoutine::MountDecisionRoutine(cta::log::LogContext& lc,
                                           const cta::runtime::MountDecisionConfig& mountDecisionConfig)
    : m_lc(lc),
      m_counterKey(getMountDecisionCounterKey()) {
  log::ScopedParamContainer params(m_lc);
  params.add("mountDecisionCounterKey", m_counterKey);
  m_lc.log(cta::log::INFO, "In MountDecisionRoutine: Created MountDecisionRoutine");
  m_mountDecisionDb = createMountDecisionDb(mountDecisionConfig);
}

std::unique_ptr<cta::mountdecision::MountDecisionDB>
MountDecisionRoutine::createMountDecisionDb(const cta::runtime::MountDecisionConfig& mountDecisionConfig) const {
  const auto configFile = mountDecisionConfig.config_file;
  std::error_code existsError;
  const bool configFileExists = !configFile.empty() && std::filesystem::exists(configFile, existsError);
  if (!configFileExists) {
    log::ScopedParamContainer params(m_lc);
    params.add("mountDecisionDbConfigFile", configFile);
    if (existsError) {
      params.add(semconv::log::exceptionMessage, existsError.message());
    }
    m_lc.log(log::WARNING,
             "In MountDecisionRoutine::createMountDecisionDb(): Mount Decision DB config file is missing. "
             "Continuing without loop counter.");
    return nullptr;
  }

  try {
    cta::mountdecision::MountDecisionDBInit mountDecisionDbInit(m_counterKey,
                                                                cta::utils::file2string(configFile),
                                                                mountDecisionConfig.number_of_connections,
                                                                m_lc.logger());
    return mountDecisionDbInit.getDB(m_lc.logger());
  } catch (cta::exception::Exception& ex) {
    log::ScopedParamContainer params(m_lc);
    params.add("mountDecisionDbConfigFile", configFile).add(semconv::log::exceptionMessage, ex.getMessageValue());
    m_lc.log(log::ERR,
             "In MountDecisionRoutine::createMountDecisionDb(): Failed to initialise Mount Decision DB. "
             "Continuing without loop counter.");
  } catch (std::exception& ex) {
    log::ScopedParamContainer params(m_lc);
    params.add("mountDecisionDbConfigFile", configFile).add(semconv::log::exceptionMessage, ex.what());
    m_lc.log(log::ERR,
             "In MountDecisionRoutine::createMountDecisionDb(): Failed to initialise Mount Decision DB. "
             "Continuing without loop counter.");
  }
  return nullptr;
}

void MountDecisionRoutine::execute() {
  if (!m_mountDecisionDb) {
    return;
  }

  try {
    m_mountDecisionDb->incrementCounter(m_counterKey);

    log::ScopedParamContainer params(m_lc);
    params.add("mountDecisionCounterKey", m_counterKey);
    m_lc.log(log::DEBUG, "In MountDecisionRoutine::execute(): Incremented loop counter.");
  } catch (cta::exception::Exception& ex) {
    log::ScopedParamContainer params(m_lc);
    params.add("mountDecisionCounterKey", m_counterKey).add(cta::semconv::log::exceptionMessage, ex.getMessageValue());
    m_lc.log(log::ERR, "In MountDecisionRoutine::execute(): Failed to increment loop counter.");
    throw;
  } catch (std::exception& ex) {
    log::ScopedParamContainer params(m_lc);
    params.add("mountDecisionCounterKey", m_counterKey).add(cta::semconv::log::exceptionMessage, ex.what());
    m_lc.log(log::ERR, "In MountDecisionRoutine::execute(): Failed to increment loop counter.");
    throw;
  }
}

std::string MountDecisionRoutine::getName() const {
  return "MountDecisionRoutine";
}

}  // namespace cta::maintd
