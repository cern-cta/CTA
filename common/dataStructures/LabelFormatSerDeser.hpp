/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <common/dataStructures/LabelFormat.hpp>

#include "cta_admin.pb.h"

namespace cta::admin {

using LabelFormat = cta::common::dataStructures::Label::Format;

inline LabelFormat ProtobufToLabelFormat(TapeLsItem::LabelFormat labelFormat) {
  using namespace common::dataStructures;

  switch (labelFormat) {
    case TapeLsItem::CTA:
      return LabelFormat::CTA;
    case TapeLsItem::OSM:
      return LabelFormat::OSM;
    case TapeLsItem::Enstore:
      return LabelFormat::Enstore;
    case TapeLsItem::EnstoreLarge:
      return LabelFormat::EnstoreLarge;
    default:
      return LabelFormat::CTA;
  }
}

inline TapeLsItem::LabelFormat LabelFormatToProtobuf(LabelFormat labelFormat) {
  using namespace common::dataStructures;

  switch (labelFormat) {
    case LabelFormat::CTA:
      return TapeLsItem::CTA;
    case LabelFormat::OSM:
      return TapeLsItem::OSM;
    case LabelFormat::Enstore:
      return TapeLsItem::Enstore;
    case LabelFormat::EnstoreLarge:
      return TapeLsItem::EnstoreLarge;
  }
  return TapeLsItem::CTA;
}

}  // namespace cta::admin
