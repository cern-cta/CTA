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

#include <common/dataStructures/LabelFormat.hpp>
#include "cta_admin.pb.h"

namespace cta::admin {

using LabelFormat = cta::common::dataStructures::Label::Format;

LabelFormat ProtobufToLabelFormat(TapeLsItem::LabelFormat labelFormat) {
  using namespace common::dataStructures;

  switch (labelFormat) {
    case TapeLsItem::CTA:            return LabelFormat::CTA;
    case TapeLsItem::OSM:            return LabelFormat::OSM;
    case TapeLsItem::Enstore:        return LabelFormat::Enstore;
    case TapeLsItem::EnstoreLarge:   return LabelFormat::EnstoreLarge;
    default:
      return LabelFormat::CTA;
  }
}

TapeLsItem::LabelFormat LabelFormatToProtobuf(LabelFormat labelFormat) {
  using namespace common::dataStructures;

  switch (labelFormat) {
    case LabelFormat::CTA:            return TapeLsItem::CTA;
    case LabelFormat::OSM:            return TapeLsItem::OSM;
    case LabelFormat::Enstore:        return TapeLsItem::Enstore;
    case LabelFormat::EnstoreLarge:   return TapeLsItem::EnstoreLarge;
  }
  return TapeLsItem::CTA;
}

}  // namespace cta::admin
