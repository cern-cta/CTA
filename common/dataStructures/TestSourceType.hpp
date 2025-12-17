/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <string>

namespace cta::common::dataStructures {

enum TestSourceType { devzero, devurandom };

std::string toString(TestSourceType type);

}  // namespace cta::common::dataStructures
