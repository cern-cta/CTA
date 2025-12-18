/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "scheduler/rdbms/schema/CreateSchemaCmd.hpp"

#include <iostream>

//------------------------------------------------------------------------------
// main
//------------------------------------------------------------------------------
int main(const int argc, char* const* const argv) {
  cta::schedulerdb::CreateSchemaCmd cmd(std::cin, std::cout, std::cerr);
  return cmd.cltMain(argc, argv);
}
