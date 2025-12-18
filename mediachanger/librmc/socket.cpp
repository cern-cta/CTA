/*
 * SPDX-FileCopyrightText: 2003 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "serrno.hpp"

#include <errno.h>
#include <sys/socket.h>
#include <sys/types.h>

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
    if (n == 0) {
      serrno = SECONNDROP;
      return 0;
    } else if (n < 0) {
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
    if (n == 0) {
      serrno = SECONNDROP;
      return 0;
    } else if (n < 0) {
      return n;
    }
    buf += n;
  }
  return nbytes;
}

// return last error message
const char* neterror() {
  return reinterpret_cast<const char*>(sstrerror(serrno == 0 ? errno : serrno));
}
