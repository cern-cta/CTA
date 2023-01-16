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

#include "catalogue/Catalogue.hpp"
#include <sys/mman.h>

namespace cta {
namespace catalogue {


std::atomic<cta::catalogue::countGetTapesByVid::CounterSpace *> Catalogue::g_cs;
std::atomic<log::Logger *> Catalogue::g_counterLog;

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
Catalogue::~Catalogue() {
}

void Catalogue::countGetTapesByVid(const countGetTapesByVid::Enum location) const {

  if (location >= countGetTapesByVid::EMAX) {
    return;
  }

  cta::catalogue::countGetTapesByVid::CounterSpace *cs = g_cs.load();
  if (!cs) {
    countGetTapesByVidInitShared();
    cs = g_cs.load();
  }

  ++(cs->getTapesByVidCounters[location]);

  struct ::timespec ts;
  ::clock_gettime(CLOCK_MONOTONIC, &ts);
  double now = ts.tv_sec + ts.tv_nsec*1e-9;
  double prev = cs->getTapesByVidCounterTime.load();

  if (prev == 0. || prev > now) {
    cs->getTapesByVidCounterTime = now;
    return;
  }

  if (now-prev < countGetTapesByVid::minReportInterval) return;
  log::Logger *log = g_counterLog.load();
  if (!log) return;

  uint64_t ctrs[countGetTapesByVid::EMAX];
  log::LogContext lc(*log);
  log::ScopedParamContainer spc(lc);

  {
    threading::MutexLocker locker(cs->counterMutex);
    prev = cs->getTapesByVidCounterTime.load();
    if (now - prev < countGetTapesByVid::minReportInterval) return;

    cs->getTapesByVidCounterTime = now;

    for(int i=0;i<countGetTapesByVid::EMAX;i++) {
      ctrs[i] = cs->getTapesByVidCounters[i].load();
      cs->getTapesByVidCounters[i].fetch_sub(ctrs[i]);
    }
  }

  for(int i=0;i<countGetTapesByVid::EMAX;i++) {
    int j;
    for(j=0;j<countGetTapesByVid::EMAX;j++) {
      if (countGetTapesByVid::logstr[j].loccode == i) break;
    }
    if (j<countGetTapesByVid::EMAX) {
      spc.add(countGetTapesByVid::logstr[j].logstr, ctrs[i]);
    } else {
      std::ostringstream msg;
      msg << "COUNTER_" << i;
      spc.add(msg.str(), ctrs[i]);
    }
  }

  spc.add("interval",now-prev);
  lc.log(log::INFO, "Catalogue - countGetTapesByVid periodic report");
}

void Catalogue::countGetTapesByVidInitShared() {
  static threading::Mutex mtxInitShared;
  threading::MutexLocker locker(mtxInitShared);
  
  cta::catalogue::countGetTapesByVid::CounterSpace *cs = g_cs.load();
  if (cs) return;

  cs = (cta::catalogue::countGetTapesByVid::CounterSpace *)
    ::mmap(nullptr, sizeof(cta::catalogue::countGetTapesByVid::CounterSpace),
           PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS, -1, 0);

   g_cs = new (cs) cta::catalogue::countGetTapesByVid::CounterSpace();
}

} // namespace catalogue
} // namespace cta
