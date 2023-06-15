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

#include <google/protobuf/repeated_field.h>
#include "objectstore/SerializersExceptions.hpp"

namespace cta {
namespace objectstore {
namespace serializers {
void removeString(::google::protobuf::RepeatedPtrField<::std::string>* field, const std::string& value);

template<class C1, class C2>
void removeOccurences(::google::protobuf::RepeatedPtrField<C1>* field, const C2& value) {
  bool found;
  do {
    found = false;
    for (size_t i = 0; i < (size_t) field->size(); i++) {
      if (value == field->Get(i)) {
        found = true;
        field->SwapElements(i, field->size() - 1);
        field->RemoveLast();
        break;
      }
    }
  } while (found);
}

size_t findString(::google::protobuf::RepeatedPtrField<::std::string>* field, const std::string& value);

size_t findStringFrom(::google::protobuf::RepeatedPtrField<::std::string>* field,
                      size_t fromIndex,
                      const std::string& value);

template<class C1, class C2>
C1& findElement(::google::protobuf::RepeatedPtrField<C1>* field, const C2& value) {
  for (auto i = field->begin(); i != field->end(); i++) {
    if (value == *i) {
      return *i;
    }
  }
  throw NotFound("In cta::objectsotre::serializers::findElement(non-const): element not found");
}

// TODO Replace usage with c++ algorithms and lambdas.
template<class C1, class C2>
const C1& findElement(const ::google::protobuf::RepeatedPtrField<C1>& field, const C2& value) {
  for (auto i = field.begin(); i != field.end(); i++) {
    if (value == *i) {
      return *i;
    }
  }
  throw NotFound("In cta::objectsotre::serializers::findElement(const): element not found");
}

template<class C1, class C2>
bool isElementPresent(const ::google::protobuf::RepeatedPtrField<C1>& field, const C2& value) {
  for (auto i = field.begin(); i != field.end(); i++) {
    if (value == *i) {
      return true;
    }
  }
  return false;
}

}  // namespace serializers
}  // namespace objectstore
}  // namespace cta