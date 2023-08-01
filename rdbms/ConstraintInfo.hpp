/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2022 CERN
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

#include<string>
#include<map>

namespace cta {
namespace rdbms {

// Add an explanation for all constraints which might be violated directly due to user input
// These explanations can then be returned back inside user-friendly error messages

using ConstraintViolationMsgMap = std::map<std::string, std::string>;

const ConstraintViolationMsgMap ConstraintViolationMessage_onCreate = {
        // Unique constraint violations
        // Foreign key violations (if referenced by name)
        {"PHYSICAL_LIBRARY_PLN_CI_UN_IDX", "Physical library name already exists"},
};

const ConstraintViolationMsgMap ConstraintViolationMessage_onDelete = {
        // Foreign key violations
        {"LOGICAL_LIBRARY_PLI_FK", "Resource is being referenced by a logical library"},
};

const ConstraintViolationMsgMap ConstraintViolationMessage_onUpdate = {
        // Unique constraint violations
        // Foreign key violations (if referenced by name)
        {"PHYSICAL_LIBRARY_PLN_CI_UN_IDX", "Physical library name already exists"},
};

class ConstraintInfo {

public:

  enum SqlStatementType {
    CREATE,
    UPDATE,
    DELETE,
  };

  static std::string constraintViolationMessage(SqlStatementType statementType, const std::string& constraint);

private:

  static std::string constraintViolationMessage_onCreate(const std::string& constraint);
  static std::string constraintViolationMessage_onDelete(const std::string& constraint);
  static std::string constraintViolationMessage_onUpdate(const std::string& constraint);

  ConstraintInfo() = delete;
};

}  // namespace catalogue
}  // namespace cta