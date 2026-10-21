/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

// Apply the same dependency suppression to local tests, CI, and deployed ASan binaries.
extern "C" const char* __lsan_default_suppressions() {
  // XRootD 5.8.4 leaks its hostname allocation during libXrdUtils initialization.
  return "leak:getMyFQN\n";
}
