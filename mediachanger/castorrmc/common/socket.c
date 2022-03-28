/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2003-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#include <sys/types.h>
#include <sys/socket.h>
#include <errno.h>
#include <serrno.h>

int netread(int s, char *buf, int nbytes) {
  int n, nb;
  if (nbytes < 0) {
    serrno = EINVAL;
    return -1;
  }
 nb = nbytes;
 for (; nb >0;)       {
   n = recv(s, buf, nb, 0);
   nb -= n;
   if (n <= 0) {
     if (n == 0) {
       serrno=SECONNDROP;
       return 0;
     }
     return n;
   }
   buf += n;
 }
 return nbytes;
}

int netwrite (const int s, const char *buf, const int nbytes) {
  int n, nb;
  if (nbytes < 0) {
    serrno = EINVAL;
    return -1;
  }
  nb = nbytes;
  for (; nb >0;)       {
    n = send(s, buf, nb, 0);
    nb -= n;
    if (n <= 0) {
      if (n == 0) {
        serrno=SECONNDROP;
        return 0;
      }
      return n;
    }
    buf += n;
  }
  return nbytes;
}

char* neterror() {                              /* return last error message    */
  if ( serrno != 0 ) return((char *)sstrerror(serrno));
  else return((char *)sstrerror(errno));
}
