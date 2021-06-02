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

#include "ActivitiesFairShareWeights.hpp"
#include "common/exception/Exception.hpp"

namespace cta { namespace common { namespace dataStructures {

void ActivitiesFairShareWeights::setWeightFromDouble(const std::string & activity, double weight) {
  if (weight < 0 || weight > 1)
    throw cta::exception::Exception("In ActivitiesFairShareWeights::setWeightFromDouble(): weight out of range.");
  activitiesWeights[activity] = weight;
}

void ActivitiesFairShareWeights::setWeightFromString(const std::string& activity, const std::string& sweight) {
  if (sweight.empty())
    throw cta::exception::Exception("In ActivitiesFairShareWeights::setWeightFromString() empty string.");
  size_t pos;
  double weight = std::stod(sweight, &pos);
  if (pos != sweight.size())
    throw cta::exception::Exception("In ActivitiesFairShareWeights::setWeightFromString(): bad format: garbage at the end of string.");
  setWeightFromDouble(activity, weight);
}


}}} // namespace cta::common::dataStructures.
