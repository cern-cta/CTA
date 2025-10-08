/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include <list>
#include <tuple>
#include <string>

namespace cta { namespace utils {
class Timer;
}}

namespace cta { namespace log {

class ScopedParamContainer;

class TimingList: public std::list<std::tuple<std::string, double>> {
public:
  void insertOrIncrement(const std::string &, double);
  void insOrIncAndReset(const std::string&, utils::Timer &);
  void insertAndReset(const std::string&, utils::Timer &);
  TimingList & operator+= (const TimingList &);
  void addToLog(ScopedParamContainer &);
private:
  double & at(const std::string&);
};

}} // namespace cta::log
