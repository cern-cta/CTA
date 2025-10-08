/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "common/dataStructures/TestSourceType.hpp"

std::string cta::common::dataStructures::toString(cta::common::dataStructures::TestSourceType type) {
  switch(type) {
    case cta::common::dataStructures::TestSourceType::devzero:
      return "devzero";
    case cta::common::dataStructures::TestSourceType::devurandom:
      return "devurandom";
    default:
      return "UNKNOWN";
  }
}
