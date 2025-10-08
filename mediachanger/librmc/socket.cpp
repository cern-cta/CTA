/*
 * SPDX-FileCopyrightText: 2003 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include <sys/types.h>
#include <sys/socket.h>
#include <errno.h>
#include "serrno.hpp"

int netread(int s, char* buf, int nbytes) {
  int n, nb;
  if (nbytes < 0) {
    serrno = EINVAL;
    return -1;
  }
  nb = nbytes;
  for (; nb > 0;) {
    n = recv(s, buf, nb, 0);
    nb -= n;
    if (n <= 0) {
      if (n == 0) {
        serrno = SECONNDROP;
        return 0;
      }
      return n;
    }
    buf += n;
  }
  return nbytes;
}

int netwrite(const int s, const char* buf, const int nbytes) {
  int n, nb;
  if (nbytes < 0) {
    serrno = EINVAL;
    return -1;
  }
  nb = nbytes;
  for (; nb > 0;) {
    n = send(s, buf, nb, 0);
    nb -= n;
    if (n <= 0) {
      if (n == 0) {
        serrno = SECONNDROP;
        return 0;
      }
      return n;
    }
    buf += n;
  }
  return nbytes;
}

char* neterror() { /* return last error message    */
  if (serrno != 0) {
    return ((char*) sstrerror(serrno));
  } else {
    return ((char*) sstrerror(errno));
  }
}
