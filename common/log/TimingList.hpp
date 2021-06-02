/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2021 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
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