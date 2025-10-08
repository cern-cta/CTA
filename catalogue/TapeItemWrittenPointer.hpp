/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "TapeItemWritten.hpp"
#include <memory>

namespace cta::catalogue {

/**
 * A utility struct allowing sorting unique_ptr according to content instead of pointer value
 */
struct TapeItemWrittenPointer : public std::unique_ptr<TapeItemWritten> {
  template<typename... Ts>
  TapeItemWrittenPointer(Ts... args) : std::unique_ptr<TapeItemWritten>::unique_ptr<TapeItemWritten>(args...) {}
};

// This operator (and not the member one) should be defined to be picked up by std::less,
// which will define the ordering of std::set<TapeItemWritenPointer>.
bool operator<(const TapeItemWrittenPointer& a, const TapeItemWrittenPointer& b);

}  // namespace cta::catalogue
