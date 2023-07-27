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
namespace catalogue {

class ConstraintInfo {

  // Add an explanation for all constraints which might be violated directly due to user input
  // These explanations can then be returned back inside user-friendly error messages
public:
  static std::string constraintViolationMessage_onCreate(const std::string& constraint) {
    static const std::map<std::string, std::string> ConstraintViolationMessage = {
            // Unique constraint violations
            // Foreign key violations (if referenced by name)
            {"PHYSICAL_LIBRARY_PLN_CI_UN_IDX", "Physical library name already exists"},
    };
    auto it = ConstraintViolationMessage.find(constraint);
    if (it != ConstraintViolationMessage.end()) {
      return it->second;
    } else {
      return constraint;
    }
  }

  static std::string constraintViolationMessage_onDelete(const std::string& constraint) {
    static const std::map<std::string, std::string> ConstraintViolationMessage = {
            // Foreign key violations
            {"LOGICAL_LIBRARY_PLI_FK", "Resource is being referenced by a logical library"},
    };
    auto it = ConstraintViolationMessage.find(constraint);
    if (it != ConstraintViolationMessage.end()) {
      return it->second;
    } else {
      return constraint;
    }
  }

  static std::string constraintViolationMessage_onModify(const std::string& constraint) {
    static const std::map<std::string, std::string> ConstraintViolationMessage = {
            // Unique constraint violations
            // Foreign key violations (if referenced by name)
            {"PHYSICAL_LIBRARY_PLN_CI_UN_IDX", "Physical library name already exists"},
    };
    auto it = ConstraintViolationMessage.find(constraint);
    if (it != ConstraintViolationMessage.end()) {
      return it->second;
    } else {
      return constraint;
    }
  }
  ConstraintInfo() = delete;
};

}  // namespace catalogue
}  // namespace cta