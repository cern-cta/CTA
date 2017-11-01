/******************************************************************************
 *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2003  CERN
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

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
