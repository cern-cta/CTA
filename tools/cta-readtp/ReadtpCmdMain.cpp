/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "ReadtpCmd.hpp"
#include "common/utils/utils.hpp"

#include <iostream>

//------------------------------------------------------------------------------
// main
//------------------------------------------------------------------------------
int main(const int argc, char* const* const argv) {
  cta::log::StdoutLogger log(cta::utils::getShortHostname(), "cta-readtp");
  cta::log::DummyLogger dummyLog("dummy", "dummy");

  cta::tape::readtp::ReadtpCmd cmd(std::cin, std::cout, std::cerr, log, dummyLog);
  return cmd.mainImpl(argc, argv);
}
