/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "StatisticsSaveCmd.hpp"

//------------------------------------------------------------------------------
// main
//------------------------------------------------------------------------------
int main(const int argc, char *const *const argv) {
  cta::statistics::StatisticsSaveCmd cmd(std::cin, std::cout, std::cerr);
  return cmd.main(argc, argv);
}
