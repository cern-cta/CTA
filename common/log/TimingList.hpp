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

#pragma once

#include <list>
#include <tuple>
#include <string>

namespace cta {
namespace utils {
class Timer;
}
}  // namespace cta

namespace cta {
namespace log {

class ScopedParamContainer;

class TimingList : public std::list<std::tuple<std::string, double>> {
public:
  void insertOrIncrement(const std::string&, double);
  void insOrIncAndReset(const std::string&, utils::Timer&);
  void insertAndReset(const std::string&, utils::Timer&);
  TimingList& operator+=(const TimingList&);
  void addToLog(ScopedParamContainer&);

private:
  double& at(const std::string&);
};

}  // namespace log
}  // namespace cta