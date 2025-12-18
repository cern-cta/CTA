/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

namespace castor::tape::utils {

template<class T>
void suppresUnusedVariable([[maybe_unused]] const T& sp) {
  /*
     * method is empty because as the name suggests, we do not intend to do anything with sp
     * we only need it for its side effects within the logger. Calling this function ensures
     * the compiler warning is suppressed.
     */
}

}  // namespace castor::tape::utils
