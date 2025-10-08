/*
 * SPDX-FileCopyrightText: 2000 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

/*
 * Cinitdaemon.c - Common routine for CASTOR daemon initialisation
 */

#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include <signal.h>
#include <errno.h>
#include <string.h>
#include "osdep.hpp"
#include "serrno.hpp"

int Cinitdaemon(const char* const name, void (*const wait4child)(int)) {
  int c;
  int maxfds;
  struct sigaction sa;

  maxfds = getdtablesize();
  /* Background */
  if ((c = fork()) < 0) {
    fprintf(stderr, "%s: cannot fork: %s\n", name, sstrerror(errno));
    exit(1);
  } else if (c > 0) {
    exit(0);
  }
  c = setsid();
  /* Redirect standard files to /dev/null */
  if (freopen("/dev/null", "r", stdin) == NULL) {
    fprintf(stderr, "%s: cannot freeopen stdin: %s\n", name, sstrerror(errno));
    exit(1);
  }
  if (freopen("/dev/null", "w", stdout) == NULL) {
    fprintf(stderr, "%s: cannot freeopen stdout: %s\n", name, sstrerror(errno));
    exit(1);
  }
  if (freopen("/dev/null", "w", stderr) == NULL) {
    fprintf(stderr, "%s: cannot freeopen stderr: %s\n", name, sstrerror(errno));
    exit(1);
  }

  for (c = 3; c < maxfds; c++) {
    close(c);
  }
  if (wait4child != NULL) {
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = wait4child;
    sa.sa_flags = SA_RESTART;
    sigaction(SIGCHLD, &sa, NULL);
  }
  return (maxfds);
}
