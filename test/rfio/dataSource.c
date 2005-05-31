/******************************************************************************
 *                      dataSource.c
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
 * @(#)$RCSfile: dataSource.c,v $ $Revision: 1.1 $ $Release$ $Date: 2005/05/31 15:37:17 $ $Author: obarring $
 *
 * 
 *
 * @author Olof Barring
 *****************************************************************************/

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: dataSource.c,v $ $Revision: 1.1 $ $Date: 2005/05/31 15:37:17 $ CERN IT/FIO Olof Barring";
#endif /* lint */


#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <rfio.h>
#include <serrno.h>
#include <log.h>
#include <Cuuid.h>
#include <Cgetopt.h>

static long state1[32] = {
  3,
  0x9a319039, 0x32d9c024, 0x9b663182, 0x5da1f342,
  0x7449e56b, 0xbeb1dbb0, 0xab5c5918, 0x946554fd,
  0x8c2e680f, 0xeb3d799f, 0xb11ee0b7, 0x2d436b86,
  0xda672e2a, 0x1588ca88, 0xe369735d, 0x904f35f7,
  0xd7158fd6, 0x6fa6f051, 0x616e6b96, 0xac94efdc,
  0xde3b81e0, 0xdf0a6fb5, 0xf103bc02, 0x48f340fb,
  0x36413f93, 0xc622c298, 0xf5a42ab8, 0x8a88d77b,
  0xf5ad9d0e, 0x8999220b, 0x27fb47b9
};
static time_t startTime, endTime;

void usage(char *cmd) 
{
  fprintf(stdout,"Usage: %s [-d directory | -F file] [-D] [-b bufsize] [-f logfile] [-s filesize] [-n nbIterations]\n",cmd);
  return;
}

char *voidToString(void *in, int len) {
    int i, j, printlen;
    u_char *c;
    char *str, *buf = NULL;

    printlen = 2*len + len/4 + 1;
    buf = (char *)malloc(printlen);
    if ( buf == NULL ) return(NULL);
    c = (u_char *)in;
    str = buf;
    for ( i=0; i<len; i++ ) {
         sprintf(buf,"%.2x",*c++);
         buf += 2;
         if ( ((i+1) % 4 == 0) && (i+1 < len) ) *buf++ = '-';
    }
    return(str);
}

int generateFileName(
                     directory,
                     fileName
                     )
     char *directory, **fileName;
{
  Cuuid_t uuid;
  char buf[1024], *p;
  
  if ( directory == NULL || fileName == NULL ) {
    serrno = EINVAL;
    return(-1);
  }
  
  Cuuid_create(&uuid);
  
  p = voidToString((void*)&uuid,sizeof(uuid));
  sprintf(buf,"%s/%s",directory,p);
  free(p);
  *fileName = strdup(buf);
  if ( *fileName == NULL ) {
    serrno = errno;
    return(-1);
  }
  
  return(0);
}

int generateData(
                 fileName,
                 bufSize,
                 fileSize
                 )
     char *fileName;
     int bufSize, fileSize;
{
  int fd=-1, fdRandom=-1, useRandom = 0, rc, rcRandom, i;
  int v, size, cnt, save_serrno, nbBufs = 10, randSize = 0;
  char *buffer = NULL;

  v = RFIO_STREAM;
  rfiosetopt(RFIO_READOPT,&v,4); 
  buffer = (char *)malloc(bufSize*nbBufs);
  if ( buffer == NULL ) {
    serrno = errno;
    return(-1);
  }
  fdRandom = open("/dev/urandom",O_RDONLY,0644);
  if ( fdRandom < 0 ) {
    log(LOG_ERR,
        "open(/dev/urandom): %s, switch to random()\n",
        strerror(errno));
    return(-1);
  } else {
    log(LOG_INFO,"read %d*%d bytes from /dev/urandom\n",
        nbBufs,bufSize);
    i = randSize = 0;
    while ( randSize < bufSize*nbBufs ) {
      rcRandom = read(fdRandom,buffer+i*512,512);
      if ( rcRandom < 0 ) {
        useRandom = 1;
        log(LOG_ERR,
            "read(%d,%p,%d) from /dev/urandom: %s, switch to random()\n",
            fdRandom,buffer[i],bufSize,strerror(errno));
        break;
      }
      i++;
      randSize += 512;
    }
    log(LOG_INFO,"random buffers initialized\n");
    close(fdRandom);
  }
  errno = serrno = rfio_errno = 0;
  fd = rfio_open(fileName,O_CREAT | O_TRUNC | O_WRONLY, 0755);
  if ( fd < 0 ) {
    save_serrno = serrno;
    log(LOG_ERR,"rfio_open(%s,O_CREAT|O_TRUNC|O_WRONLY,0755): errno=%d, serrno=%d, rfio_errno=%d\n",
            fileName,errno,serrno,rfio_errno);
    free(buffer);
    sleep(1);
    serrno = save_serrno;
    return(0);
  }

  size = 0;
  i = 0;
  while ( size < fileSize ) {
    if ( fileSize-size < bufSize ) bufSize = fileSize - size;
    rc = 0;
    cnt = 0;
    while ( cnt < bufSize ) {
      errno = serrno = rfio_errno = 0;
      rc = rfio_write(fd,&buffer[i]+cnt,bufSize-cnt);
      if ( rc < 0 ) break;
      cnt += rc;
      size+=rc;
    }
    if ( rc == -1 ) break;
    i = (++i) % nbBufs;
  }
  if ( rc == -1 ) {
    save_serrno = serrno;
    log(LOG_ERR,"rfio_write(%s), size so far=%d, file size=%d, buffer size=%d: errno=%d, serrno=%d, rfio_errno=%d\n",
            fileName,
            size,fileSize,bufSize,errno,serrno,rfio_errno);
    (void)rfio_close(fd);
    free(buffer);
    sleep(1);
    serrno = save_serrno;
    return(0);
  }
  
  errno = serrno = rfio_errno = 0;
  (void)rfio_close(fd);
  free(buffer);
  return(0);
}

int doDataSource(
                 directory,
                 useFile,
                 nbIterations,
                 bufSize,
                 fileSize
                 ) 
     char *directory, *useFile;
     int nbIterations, bufSize, fileSize;
{
  int i, rc, transferTime = 0;
  char *fname;
  
  for ( i=0; i<nbIterations; i++ ) {
    fname = useFile;
    if ( fname == NULL ) {
       rc = generateFileName(directory,&fname);
       if ( rc == -1 || fname == NULL ) {
         log(LOG_ERR,"generateFileName(%s,%p): %s\n",
             directory,&fname,sstrerror(serrno));
         return(-1);
       }
    }
    log(LOG_INFO,"Writing file (%d/%d) %s\n",i,nbIterations,fname);
    transferTime = time(NULL);
    rc = generateData(fname,bufSize,fileSize);
    if ( rc == -1 ) {
      log(LOG_ERR,"generateData(%s,%p): %s\n",
          directory,&fname,sstrerror(serrno));
      return(-1);
    }
    transferTime = time(NULL) - transferTime;
    log(LOG_INFO,"    File (%d/%d) written. Transfer time=%d, speed=%f\n",
        i,nbIterations,
        transferTime,((float)fileSize)/
        ((float)(transferTime > 0 ? transferTime : 1)));
  }
  
  return(0);
}

int main(int argc, char *argv[]) 
{
  int rc, bufSize=1024*1024, fileSize = 1024*1024*1024, nbIterations=1, n;
  char *buffer, *directory = NULL, *logfile = "", *useFile = NULL;
  unsigned seed;
  time_t tt;
  int c;
  int daemon = 0;

  seed = 1;
  n = 128;
  initstate(seed,(char *)state1,n);
  setstate((char *)state1);

  Copterr = 1;
  Coptind = 1;
  
  while ( (c = Cgetopt(argc,argv,"f:F:hDd:s:b:n:")) != -1 ) {
    switch (c) {
    case 'f':
      logfile = strdup(Coptarg);
      break;
    case 'F':
      useFile = strdup(Coptarg);
      break;
    case 'h':
      usage(argv[0]);
      return(0); 
    case 'D':
      daemon++;
      break;
    case 'd':
      directory = strdup(Coptarg);
      break;
    case 's':
      fileSize = atoi(Coptarg);
      break;
    case 'b':
      bufSize = atoi(Coptarg);
      break;
    case 'n':
      nbIterations = atoi(Coptarg);
      break;
    default:
      log(LOG_ERR,"Unknown option -%c\n",c);
      usage(argv[0]);
      return(2);
    }
  }
  if ( (directory == NULL) && (useFile == NULL) ||
       (directory != NULL) && (useFile != NULL) ) {
      usage(argv[0]);
      return(2);
  }

  if (daemon) {
	  if ((c = fork()) < 0) {
		  perror("fork");
		  exit (1);
	  } else
		  if (c > 0) exit (0);
	  c = setsid();
	  for (c = 0; c < getdtablesize(); c++)
		  close (c);
  }
  
  initlog("dataSource",LOG_INFO,logfile);
  startTime = time(NULL);
  rc = doDataSource(directory,useFile,nbIterations,bufSize,fileSize);
  if ( rc == -1 ) {
    log(LOG_ERR,"doDataSource(%s,%s,%d,%d,%d): %s\n",
        (directory != NULL ? directory : "(null)"),
        (useFile != NULL ? useFile : "(null)"),
        nbIterations,
        bufSize,
        fileSize,
        sstrerror(serrno));
    return(1);
  }
  endTime = time(NULL);
  log(LOG_INFO,"Summary: %d * %d bytes transferred in %d seconds\n",
      nbIterations,fileSize,endTime-startTime);
  return(0);
}

                 
