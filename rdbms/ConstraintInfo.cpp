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

#include "rdbms/ConstraintInfo.hpp"
#include "common/exception/Exception.hpp"

namespace cta {
namespace rdbms {

std::string ConstraintInfo::constraintViolationMessage(SqlStatementType statementType, const std::string& constraint) {
  switch (statementType) {
  case CREATE:
    return constraintViolationMessage_onCreate(constraint);
  case UPDATE:
    return constraintViolationMessage_onUpdate(constraint);
  case DELETE:
    return constraintViolationMessage_onDelete(constraint);
  default:
    throw exception::Exception(std::string(__FUNCTION__) + ": Wrong sql statement type");
  }
}

std::string ConstraintInfo::constraintViolationMessage_onCreate(const std::string& constraint) {
  auto it = ConstraintViolationMessage_onCreate.find(constraint);
  if (it != ConstraintViolationMessage_onCreate.end()) {
    return it->second;
  } else {
    return constraint;
  }
}

std::string ConstraintInfo::constraintViolationMessage_onDelete(const std::string& constraint) {
  auto it = ConstraintViolationMessage_onDelete.find(constraint);
  if (it != ConstraintViolationMessage_onDelete.end()) {
    return it->second;
  } else {
    return constraint;
  }
}

std::string ConstraintInfo::constraintViolationMessage_onUpdate(const std::string& constraint) {

  auto it = ConstraintViolationMessage_onUpdate.find(constraint);
  if (it != ConstraintViolationMessage_onUpdate.end()) {
    return it->second;
  } else {
    return constraint;
  }
}

} // namespace rdbms
} // namespace cta
