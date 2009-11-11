/******************************************************************************
 *                      adler32.c
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
 * @(#)$RCSfile: adler32.c,v $ $Revision: 1.2 $ $Release$ $Date: 2007/11/30 09:55:31 $ $Author: murrayc3 $
 *
 * 
 *
 * @author Olof Barring
 *****************************************************************************/

#include <stdio.h>
#include <fcntl.h>
#include <rfio.h>
#include <zlib.h>

void usage(char *cmd) 
{
  fprintf(stdout,"%s <file1> <file2> ...\n",cmd);
  return;
}

int main(int argc, char *argv[]) 
{
  unsigned long ckSum;
  unsigned char buffer[1024*1024];
  int i, fd, rc;

  if ( argc < 2 ) {
    usage(argv[0]);
    return(2);
  }
  
  for (i=1; i<argc; i++) {
    fd = rfio_open64(argv[i],O_RDONLY);
    if ( fd == -1 ) {
      fprintf(stderr,"rfio_open(%s): %s\n",argv[i],rfio_serror());
      continue;
    }
    ckSum = adler32(0L,Z_NULL,0);
    while ( (rc = rfio_read(fd,buffer,sizeof(buffer))) > 0 ) {
      ckSum = adler32(ckSum,buffer,(unsigned int)rc);
    }
    if ( rc == -1 ) {
      fprintf(stderr,"rfio_read(%s): %s\n",argv[i],rfio_serror());
    } else {
      fprintf(stderr,"adler32(%s) = %lu, 0x%lx\n",argv[i],ckSum,ckSum);
    } 
    rfio_close(fd);
  }
  return(0);
}


