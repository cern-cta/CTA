/*
 * Copyright (C) 1990-2002 by CERN/IT/PDP/DM
 * All rights reserved
 */


/*  tpconfig - configure tape drive up/down */
#include <iostream>
#include <stdlib.h>
#include <errno.h>
#include <sstream>
#include <stdio.h>
#include <sys/types.h>
#include <string>
#include <string.h>
#include <unistd.h>

#include "Ctape.h"
#include "Ctape_api.h"
#include "serrno.h"
#include "tape/TpConfigCmdLine.hpp"
#include "tape/TpConfigException.hpp"

/* Foward declarations */
static void configureDrive(const tape::TpConfigCmdLine &cmdLine);

int main(const int argc, const char **argv) {
  using namespace tape;

  TpConfigCmdLine cmdLine;
  try {
    cmdLine = TpConfigCmdLine::parse(argc, argv);
  } catch(TpConfigException &ex) {
    std::cerr << "Failed to parse the command line: " << ex.what() << std::endl;
    std::cerr << "usage: tpconfig [unit_name] status" << std::endl;
    return ex.exitValue();
  }

  try {
    configureDrive(cmdLine);
  } catch(TpConfigException &ex) {
    std::cerr << ex.what() << std::endl;
    std::cerr << "Failed to configure drive" << std::endl;
    return ex.exitValue();
  }

  return 0;
}

static void configureDrive(const tape::TpConfigCmdLine &cmdLine) {
  using namespace tape;

  if (0 > Ctape_config (cmdLine.unitName.c_str(), cmdLine.status)) {
    const std::string serrnoStr = sstrerror(serrno);
    std::ostringstream oss;
    oss << "TP009 - could not configure " << cmdLine.unitName << ": " <<
      serrnoStr;
    const int exitValue = (serrno == EINVAL || serrno == ETIDN) ? USERR : SYERR;
    throw TpConfigException(oss.str(), exitValue);
  }
}
