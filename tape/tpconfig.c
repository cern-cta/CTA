/*
 * Copyright (C) 1990-2002 by CERN/IT/PDP/DM
 * All rights reserved
 */


/*  tpconfig - configure tape drive up/down */
#include <stdlib.h>
#include <errno.h>
#include <stdio.h>
#include <sys/types.h>
#include <string.h>
#include <unistd.h>

#include "h/Ctape.h"
#include "h/Ctape_api.h"
#include "h/serrno.h"

/* Foward declarations */
static int handleCmdLineShortVersion(const char **argv);
static int handleCmdLineLongVersion(const char **argv);
static void tpconfig_usage(const char *const programName);
static int parseCmdLineShortVersion(const char **argv);
static int parseStatus(const char *const programName, const char *str);
typedef struct CmdLineLongVersion {
  char unitName[256];
  int status;
} CmdLineLongVersion;
static CmdLineLongVersion parseCmdLineLongVersion(const char **argv);

int main(const int argc, const char **argv) {
  switch(argc) {
  case 2: return handleCmdLineShortVersion(argv);
  case 3: return handleCmdLineLongVersion(argv);
  default:
    fprintf (stderr, "Wrong number of command-line arguments"
      ": expected 2 for long form or 1 for short form,"
                        " actual was %d\n", (argc - 1));
    tpconfig_usage (argv[0]);
    return USERR;
  }
}

static void tpconfig_usage(const char *const programName) {
  fprintf (stderr, "usage: %s ", programName);
  fprintf (stderr, "[unit_name] status\n");
}

static int handleCmdLineShortVersion(const char **argv) {
  const int status = parseCmdLineShortVersion(argv);
  const int nbentries = CA_MAXNBDRIVES;
  char hostname[CA_MAXHOSTNAMELEN+1];
  struct drv_status drv_status[CA_MAXNBDRIVES];
                        
  if (0 > gethostname (hostname, sizeof(hostname))) {
    fprintf (stderr, "Call to gethostname() failed\n");
    return SYERR;
  }      

  const int nbDrives = Ctape_status (hostname, drv_status, nbentries);
  if (0 > nbDrives) {
    fprintf (stderr, "Call to Ctape_status() failed\n");
    return SYERR;
  }

  if(1 != nbDrives) {
    fprintf (stderr, "Short version of command line can only be used if there"
      " is one and only one drive connected to the tape server"
      ": nbDrives=%d\n", nbDrives);
    return USERR;
  }

  if (0 > Ctape_config (drv_status[0].drive, status)) {
    fprintf (stderr, TP009, argv[1], sstrerror(serrno));
    if (serrno == EINVAL || serrno == ETIDN) {
      return USERR;
    } else {
      return SYERR;
    }
  }

  return 0;
}

static int parseCmdLineShortVersion(const char **argv) {
  return parseStatus(argv[0], argv[1]);
}

static int parseStatus(const char *const programName, const char *str) {
  if (strcmp (str, "up") == 0) {
    return CONF_UP;
  } else if (strcmp (str, "down") == 0) {
    return CONF_DOWN;
  } else {
    fprintf (stderr, "Invalid status: expected=up or down actual=%s\n", str);
    tpconfig_usage (programName);
    exit (USERR);
  }
}

static int handleCmdLineLongVersion(const char **argv) {
  const CmdLineLongVersion cmdLine = parseCmdLineLongVersion(argv);

  if (Ctape_config (cmdLine.unitName, cmdLine.status) < 0) {
    fprintf (stderr, TP009, cmdLine.unitName, sstrerror(serrno));
    if (serrno == EINVAL || serrno == ETIDN) {
      return USERR;
    } else {
      return SYERR;
    }
  }

  return 0;
}

static CmdLineLongVersion parseCmdLineLongVersion(const char **argv) {
  CmdLineLongVersion cmdLine;
  cmdLine.status = parseStatus(argv[0], argv[2]);

  strcpy(cmdLine.unitName, argv[1]);

  return cmdLine;
}
