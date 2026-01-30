/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "RoutineRunner.hpp"

#include "catalogue/CatalogueFactory.hpp"
#include "catalogue/CatalogueFactoryFactory.hpp"
#include "common/Timer.hpp"
#include "common/exception/Errnum.hpp"
#include "common/exception/UserError.hpp"
#include "common/semconv/Attributes.hpp"
#include "common/telemetry/metrics/instruments/MaintdInstruments.hpp"
#include "rdbms/Login.hpp"

#include <chrono>
#include <opentelemetry/context/runtime_context.h>
#include <signal.h>
#include <sys/prctl.h>
#include <thread>

namespace cta::maintd {

// Don't say this out loud
int64_t nowSecs() {
  return std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::now().time_since_epoch()).count();
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
RoutineRunner::RoutineRunner(int64_t sleepIntervalSecs, int64_t maxCycleDurationSecs)
    : m_sleepIntervalSecs(sleepIntervalSecs),
      m_maxCycleDurationSecs(maxCycleDurationSecs) {}

//------------------------------------------------------------------------------
// RoutineRunner::registerRoutine
//------------------------------------------------------------------------------
void RoutineRunner::registerRoutine(std::unique_ptr<IRoutine> routine) {
  m_routines.emplace_back(std::move(routine));
}

//------------------------------------------------------------------------------
// RoutineRunner::stop
//------------------------------------------------------------------------------
void RoutineRunner::stop() {
  m_running = false;
}

void RoutineRunner::safeRunRoutine(IRoutine& routine, cta::log::LogContext& lc) {
  log::ScopedParamContainer params(lc);
  params.add("routine", routine.getName());
  try {
    lc.log(log::INFO, "Routine started");
    cta::utils::Timer t;
    routine.execute();
    lc.log(log::INFO, "Routine finished");
    cta::telemetry::metrics::ctaMaintdRoutineDuration->Record(
      t.msecs(),
      {
        {cta::semconv::attr::kCtaRoutineName, routine.getName()}
    },
      opentelemetry::context::RuntimeContext::GetCurrent());
  } catch (cta::exception::Exception& ex) {
    params.add("exceptionMessage", ex.getMessageValue());
    lc.log(log::ERR, "In RoutineRunner::safeRunRoutine(): received an exception. Backtrace follows.");
    lc.logBacktrace(log::INFO, ex.backtrace());
  } catch (std::exception& ex) {
    params.add("exceptionMessage", ex.what());
    lc.log(log::ERR, "In RoutineRunner::safeRunRoutine(): received a std::exception.");
  } catch (...) {
    lc.log(log::ERR, "In RoutineRunner::safeRunRoutine(): received an unknown exception.");
  }
}

//------------------------------------------------------------------------------
// RoutineRunner::run
//------------------------------------------------------------------------------
void RoutineRunner::run(cta::log::LogContext& lc) {
  // At least one routine should be enabled.
  if (m_routines.empty()) {
    throw cta::exception::UserError("In RoutineRunner::run(): No routines enabled.");
  }
  m_running = true;
  lc.log(log::DEBUG, "In RoutineRunner::run(): New run started.");
  while (m_running) {
    m_executionStartTime = nowSecs();
    lc.log(log::DEBUG, "In RoutineRunner::run(): Executing all routines.");
    for (const auto& routine : m_routines) {
      safeRunRoutine(*routine, lc);
      if (!m_running) {
        lc.log(log::INFO, "In RoutineRunner::run(): Stop requested.");
        return;
      }
    }
    m_sleepStartTime = nowSecs();
    std::this_thread::sleep_for(std::chrono::seconds(m_sleepIntervalSecs));
  }
  lc.log(log::DEBUG, "In RoutineRunner::run(): Stop requested.");
}

/**
 * The routine runner is considered ready when:
 * - the routine runner is running
 * - the catalogue is reachable (not implemented yet)
 * - the scheduler is reachable (not implemented yet)
 */
bool RoutineRunner::isReady() {
  return m_running;
}

/**
 * The routine runner is considered alive when:
 * - a routine has executed in the last 2 minutes
 */
bool RoutineRunner::isLive() {
  // If the executionStartTime is the most recent, it means we are currently executing.
  if (m_executionStartTime > m_sleepStartTime) {
    // Check that we have not been executing for too long
    return (nowSecs() - m_executionStartTime) < m_maxCycleDurationSecs;
  }
  // The sleepStartTime is the most recent, so we are currently sleeping.
  // Check that we have not been sleeping for too long.
  // We need to give it a few extra seconds as the transition from sleeping to executing is not instant
  return (nowSecs() - m_sleepStartTime) < m_sleepIntervalSecs + 5;
}

}  // namespace cta::maintd
