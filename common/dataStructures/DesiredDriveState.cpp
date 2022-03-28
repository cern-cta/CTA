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

#include "DesiredDriveState.hpp"
#include "common/log/PriorityMaps.hpp"

namespace cta {
namespace common {
namespace dataStructures {

DesiredDriveState::DesiredDriveState():up(false),forceDown(false){}  
  
DesiredDriveState::DesiredDriveState(const DesiredDriveState& ds) {
  if(this != &ds){
    up = ds.up;
    forceDown = ds.forceDown;
    reason = ds.reason;
    comment = ds.comment;
  }
}

DesiredDriveState& DesiredDriveState::operator=(const DesiredDriveState& ds) {
  if(this != &ds){
    up = ds.up;
    forceDown = ds.forceDown;
    reason = ds.reason;
    comment = ds.comment;
  }
  return *this;
}

std::string DesiredDriveState::c_tpsrvPrefixComment = "[cta-taped]";

void DesiredDriveState::setReasonFromLogMsg(const int logLevel, const std::string & msg){
  reason = DesiredDriveState::generateReasonFromLogMsg(logLevel,msg);
}

std::string DesiredDriveState::generateReasonFromLogMsg(const int logLevel, const std::string & msg){
  std::string localReason = c_tpsrvPrefixComment;
  localReason += " " + cta::log::PriorityMaps::getPriorityText(logLevel) + " "+msg;
  return localReason;
}

}}}


std::ostream &cta::common::dataStructures::operator<<(std::ostream& os, const DesiredDriveState& obj) {
  std::string upStr(obj.up?"true":"false"),
          forceStr(obj.forceDown?"true":"false");
  return os << "(up="  << upStr  << " forceDown="  << forceStr << ")";
}