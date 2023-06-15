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

#include "ProtocolBuffersAlgorithms.hpp"

void cta::objectstore::serializers::removeString(::google::protobuf::RepeatedPtrField<::std::string>* field,
                                                 const std::string& value) {
  //  bool found;
  //  do {
  //    found = false;
  //    for (size_t i=0; i<(size_t)field->size(); i++) {
  //      if (value == field->Get(i)) {
  //        found = true;
  //        field->SwapElements(i, field->size()-1);
  //        field->RemoveLast();
  //        break;
  //      }
  //    }
  //  } while (found);
  removeOccurences(field, value);
}

size_t cta::objectstore::serializers::findString(::google::protobuf::RepeatedPtrField<::std::string>* field,
                                                 const std::string& value) {
  for (size_t i = 0; i < (size_t) field->size(); i++) {
    if (value == field->Get(i)) {
      return i;
    }
  }
  throw NotFound("In cta::objectstore::serializers::findString: string not found");
}

size_t cta::objectstore::serializers::findStringFrom(::google::protobuf::RepeatedPtrField<::std::string>* field,
                                                     size_t fromIndex,
                                                     const std::string& value) {
  for (size_t i = fromIndex; i < (size_t) field->size(); i++) {
    if (value == field->Get(i)) {
      return i;
    }
  }
  throw NotFound("In cta::objectstore::serializers::findString: string not found");
}