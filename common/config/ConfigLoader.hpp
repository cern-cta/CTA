/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/exception/UserError.hpp"

#include <rfl.hpp>
#include <rfl/toml.hpp>

namespace cta::config {

template<typename T>
T loadFromToml(const std::string& filePath, bool strict = false) {
  using StrictProcessors = rfl::Processors<rfl::NoExtraFields,  // error on unknown keys
                                           rfl::NoOptionals     // require optionals present
                                           >;

  using LenientProcessors = rfl::Processors<>;

  auto res = strict ? rfl::toml::load<T, StrictProcessors>(filePath) : rfl::toml::load<T, LenientProcessors>(filePath);

  if (!res) {
    throw cta::exception::UserError(res.error().what());
  }
  return res.value();
}

}  // namespace cta::config
