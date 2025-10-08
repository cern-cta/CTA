/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <string>

namespace cta::mediachanger {

/**
 * Class containing the code common to the parsed command-line of the ACS
 * command-line tools provided by CASTOR.
 */
class CmdLine {
protected:

  /**
   * Handles the specified parameter that is missing a parameter.
   *
   * @param opt The option.
   */
  void handleMissingParameter(const int opt);

  /**
   * Handles the specified unknown option.
   *
   * @param opt The option.
   */
  void handleUnknownOption(const int opt);

}; // class CmdLine

} // namespace cta::mediachanger
