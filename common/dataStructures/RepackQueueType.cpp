/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "RepackQueueType.hpp"
#include "common/exception/Exception.hpp"

namespace cta::common::dataStructures {

std::string toString(RepackQueueType queueType) {
  switch(queueType) {
  case RepackQueueType::Pending:
    return "Pending";
  case RepackQueueType::ToExpand:
    return "ToExpand";
  }
  throw exception::Exception("In toString(RepackQueueType): unexpected queue type.");
}

} // namespace cta::common::dataStructures
