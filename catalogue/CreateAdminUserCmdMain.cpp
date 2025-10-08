/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "catalogue/CreateAdminUserCmd.hpp"

#include <iostream>

//------------------------------------------------------------------------------
// main
//------------------------------------------------------------------------------
int main(const int argc, char *const *const argv) {
  cta::catalogue::CreateAdminUserCmd cmd(std::cin, std::cout, std::cerr);
  return cmd.main(argc, argv);
}
