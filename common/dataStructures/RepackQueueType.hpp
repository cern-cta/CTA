/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <string>

namespace cta::common::dataStructures {

enum class RepackQueueType { Pending, ToExpand };
std::string toString(RepackQueueType queueType);
} // namespace cta::common::dataStructures
