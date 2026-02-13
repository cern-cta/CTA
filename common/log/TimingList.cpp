/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "TimingList.hpp"

#include "LogContext.hpp"
#include "common/utils/Timer.hpp"

namespace cta::log {

void TimingList::addToLog(ScopedParamContainer& spc) {
  for (auto& t : *this) {
    std::string name;
    double value;
    std::tie(name, value) = t;
    spc.add(name, value);
  }
}

double& TimingList::at(const std::string& name) {
  for (auto& e : *this) {
    if (std::get<0>(e) == name) {
      return std::get<1>(e);
    }
  }
  throw std::out_of_range("In TimingList::at(): no such element.");
}

void TimingList::insertOrIncrement(const std::string& name, double value) {
  try {
    at(name) += value;
  } catch (std::out_of_range&) {
    emplace_back(name, value);
  }
}

void TimingList::insOrIncAndReset(const std::string& name, utils::Timer& t) {
  insertOrIncrement(name, t.secs(utils::Timer::resetCounter));
}

void TimingList::insertAndReset(const std::string& name, utils::Timer& t) {
  emplace_back(name, t.secs(utils::Timer::resetCounter));
}

TimingList& TimingList::operator+=(const TimingList& other) {
  for (auto& ot : other) {
    std::string oName;
    double oVal;
    std::tie(oName, oVal) = ot;
    bool found = false;
    for (auto& t : *this) {
      std::string name;
      double val;
      std::tie(name, val) = t;
      if (name == oName) {
        std::get<1>(t) = val + oVal;
        found = true;
        break;
      }
    }
    if (!found) {
      // We did not add this one to an existing entry: just insert a new one.
      push_back(ot);
    }
  }
  return *this;
}

}  // namespace cta::log
