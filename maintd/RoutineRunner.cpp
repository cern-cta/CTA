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

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
RoutineRunner::RoutineRunner(uint32_t sleepInterval) : m_sleepInterval(sleepInterval) {}

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
    m_lastExecutionTime =
      std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::now().time_since_epoch()).count();
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
    lc.log(log::DEBUG, "In RoutineRunner::run(): Executing all routines.");
    for (const auto& routine : m_routines) {
      safeRunRoutine(*routine, lc);
      if (!m_running) {
        lc.log(log::DEBUG, "In RoutineRunner::run(): Stop requested.");
        return;
      }
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(std::chrono::milliseconds(m_sleepInterval)));
  }
  lc.log(log::DEBUG, "In RoutineRunner::run(): Stop requested.");
}

bool RoutineRunner::isRunning() {
  return m_running;
}

bool RoutineRunner::didRecentlyFinishRoutine(int64_t seconds) {
  auto now =
    std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::now().time_since_epoch()).count();
  return (now - m_lastExecutionTime) < seconds;
}

}  // namespace cta::maintd
