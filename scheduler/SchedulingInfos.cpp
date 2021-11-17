/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2015-2021 CERN
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

#include "SchedulingInfos.hpp"

namespace cta {

SchedulingInfos::SchedulingInfos(const std::string & logicalLibraryName):m_logicalLibraryName(logicalLibraryName) {
}

SchedulingInfos::SchedulingInfos(const SchedulingInfos& other): m_logicalLibraryName(other.m_logicalLibraryName), m_potentialMounts(other.m_potentialMounts) {
}

SchedulingInfos & SchedulingInfos::operator =(const SchedulingInfos& other){
  m_logicalLibraryName = other.m_logicalLibraryName;
  m_potentialMounts = other.m_potentialMounts;
  return *this;
}

void SchedulingInfos::addPotentialMount(const cta::SchedulerDatabase::PotentialMount& potentialMount) {
  m_potentialMounts.push_back(potentialMount);
}

std::string SchedulingInfos::getLogicalLibraryName() const {
  return m_logicalLibraryName;
}

std::list<cta::SchedulerDatabase::PotentialMount> SchedulingInfos::getPotentialMounts() const {
  return m_potentialMounts;
}

SchedulingInfos::~SchedulingInfos() {
}

}