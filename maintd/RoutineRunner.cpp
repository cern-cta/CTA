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
  m_stopRequested = true;
}

void RoutineRunner::safeRunRoutine(IRoutine& routine, cta::log::LogContext& lc) {
  try {
    cta::utils::Timer t;
    routine.execute();
    cta::telemetry::metrics::ctaMaintdRoutineDuration->Record(
      t.msecs(),
      {
        {cta::semconv::attr::kCtaRoutineName, routine.getName()}
    },
      opentelemetry::context::RuntimeContext::GetCurrent());
  } catch (cta::exception::Exception& ex) {
    log::ScopedParamContainer exParams(lc);
    exParams.add("routine", routine.getName());
    exParams.add("exceptionMessage", ex.getMessageValue());
    lc.log(log::ERR, "In RoutineRunner::safeRunRoutine(): received an exception. Backtrace follows.");
    lc.logBacktrace(log::INFO, ex.backtrace());
  } catch (std::exception& ex) {
    log::ScopedParamContainer exParams(lc);
    exParams.add("routine", routine.getName());
    exParams.add("exceptionMessage", ex.what());
    lc.log(log::ERR, "In RoutineRunner::safeRunRoutine(): received a std::exception.");
  } catch (...) {
    log::ScopedParamContainer exParams(lc);
    exParams.add("routine", routine.getName());
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
  m_stopRequested = false;
  lc.log(log::DEBUG, "In RoutineRunner::run(): New run started.");
  while (!m_stopRequested) {
    lc.log(log::DEBUG, "In RoutineRunner::run(): Executing all routines.");
    for (const auto& routine : m_routines) {
      safeRunRoutine(*routine, lc);
      if (m_stopRequested) {
        lc.log(log::DEBUG, "In RoutineRunner::run(): Stop requested.");
        return;
      }
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(std::chrono::milliseconds(m_sleepInterval)));
  }
  lc.log(log::DEBUG, "In RoutineRunner::run(): Stop requested.");
}

}  // namespace cta::maintd
