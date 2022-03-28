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

#include "TimingList.hpp"
#include "LogContext.hpp"
#include "common/Timer.hpp"

namespace cta { namespace log {

void TimingList::addToLog(ScopedParamContainer& spc) {
  for (auto & t: *this) {
    std::string name;
    double value;
    std::tie(name, value) = t;
    spc.add(name, value);
  }
}

double& TimingList::at(const std::string& name) {
  for (auto &e: *this) {
    if (std::get<0>(e) == name) return std::get<1>(e);
  }
  throw std::out_of_range("In TimingList::at(): no such element.");
}


void TimingList::insertOrIncrement(const std::string& name, double value) {
  try {
    at(name)+=value;
  } catch (std::out_of_range&) {
    push_back(std::make_tuple(name, value));
  }
}

void TimingList::insOrIncAndReset(const std::string& name, utils::Timer& t) {
  insertOrIncrement(name, t.secs(utils::Timer::resetCounter));
}

void TimingList::insertAndReset(const std::string& name, utils::Timer& t) {
  push_back(std::make_tuple(name, t.secs(utils::Timer::resetCounter)));
}


TimingList& TimingList::operator+=(const TimingList& other) {
  for (auto & ot: other) {
    std::string oName;
    double oVal;
    std::tie(oName, oVal) = ot;
    for (auto & t: *this) {
      std::string name;
      double val;
      std::tie(name, val) = t;
      if (name == oName) {
        std::get<1>(t) = val+oVal;
        goto done;
      }
    }
    // We did not add this one to an existing entry: just insert a new one.
    push_back(ot);
    done:;
  }
  return *this;
}

}} // namespace cta::log
