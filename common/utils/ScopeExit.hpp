/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <type_traits>
#include <utility>

namespace cta::utils {

/** Run cleanup once on scope exit, including exception unwinding. The callback must not throw. */
template<typename F>
class [[nodiscard]] ScopeExit {
public:
  // Keep construction non-throwing once the callback has been supplied.
  explicit ScopeExit(F cleanup) noexcept : m_cleanup(std::move(cleanup)) {
    static_assert(std::is_nothrow_move_constructible_v<F>);
  }

  ~ScopeExit() noexcept { m_cleanup(); }

  ScopeExit(const ScopeExit&) = delete;
  ScopeExit& operator=(const ScopeExit&) = delete;
  ScopeExit(ScopeExit&&) = delete;
  ScopeExit& operator=(ScopeExit&&) = delete;

private:
  F m_cleanup;
};

}  // namespace cta::utils
