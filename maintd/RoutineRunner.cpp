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

#include <signal.h>
#include <sys/prctl.h>
#include <chrono>
#include <thread>
#include <opentelemetry/context/runtime_context.h>

#include "RoutineRunner.hpp"

#include "catalogue/CatalogueFactory.hpp"
#include "catalogue/CatalogueFactoryFactory.hpp"
#include "common/exception/Errnum.hpp"
#include "common/exception/UserError.hpp"
#include "common/semconv/Attributes.hpp"
#include "common/telemetry/metrics/instruments/MaintdInstruments.hpp"
#include "common/Timer.hpp"
#include "rdbms/Login.hpp"

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
