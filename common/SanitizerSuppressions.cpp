/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

// This file can be removed when we upgrade to XRootD >= 5.9.2

// Apply the same dependency suppression to local tests, CI, and deployed ASan binaries.
extern "C" const char* __lsan_default_suppressions() {
  // XRootD 5.8.4 leaks its hostname allocation even in an empty program linked to libXrdUtils.
  // Suppress only this allocation stack; remove when the dependency fixes getMyFQN.
  return "leak:getMyFQN\n";
}
