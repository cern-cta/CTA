/*
 * $Id: tpreseek64.c,v 1.1 2002/11/19 09:20:52 jdurand Exp $
 */

/*
 * Copyright (C) 2002 by CERN/IT/DS/HSM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: tpreseek64.c,v $ $Revision: 1.1 $ $Date: 2002/11/19 09:20:52 $ CERN IT-DS/HSM Jean-Philippe Baud Jean-Damien Durand Benjamin Couturier";
#endif /* not lint */

/*	tpreseek64 - write NBRECORDS_TOWRITE records and
		   read back NBRECORDS_TOREAD using the rfio_preseek64 function */

#include <fcntl.h>
#include <stdio.h>
#if defined(_WIN32)
#include <winsock2.h>
#endif
#include "rfio_api.h"
#include "serrno.h"
#include "errno.h"

#ifdef _WIN32
#define CONSTLL(a) (a##i64)
#else
#define CONSTLL(a) (a##LL)
#endif


#define NBRECORDS_TOREAD 5
#define NBRECORDS_TOWRITE 10

#define MB ONE_MB
#define GB ONE_GB


main(argc, argv)
     int argc;
     char **argv;
{
  char buf[65536];
  int errflg = 0;
  int fd;
  int i;
  struct iovec64 iov[NBRECORDS_TOREAD];
  int iovnb = NBRECORDS_TOREAD;
  int j;
	
  static int lengths[NBRECORDS_TOWRITE] = {4096, 
					   32768, 
					   16384, 
					   8192, 
					   65536,
					   32768, 
					   16384, 
					   4096, 
					   65536, 
					   8192};

  static off64_t offsets[NBRECORDS_TOWRITE]={ 100 * MB,
					      500 * MB,
					      GB,
					      3LL * GB / 2LL,
					      2LL * GB - 65536LL,
					      2LL * GB,
					      5LL * GB / 2LL,
					      5LL * GB / 2LL + 16384LL,
					      3LL * GB,
					      3LL * GB + 65536LL };
	
  static int records_toread[NBRECORDS_TOREAD] = {2, 4, 5, 8, 9};

#if defined(_WIN32)
  WSADATA wsadata;
#endif

  if (argc != 2) {
    fprintf (stderr, "usage: tpreseek64 pathname\n");
    exit (1);
  }
#if defined(_WIN32)
  if (WSAStartup (MAKEWORD (2, 0), &wsadata)) {
    fprintf (stderr, "WSAStartup unsuccessful\n");
    exit (2);
  }
#endif
  while (!errflg) {

    off64_t rv = 0;

    /* Write variable length records.
     * Each record is filled with the record index
     */
    if ((fd = rfio_open64(argv[1], O_WRONLY|O_CREAT|O_TRUNC, 0644)) < 0) {
      rfio_perror ("rfio_open");
      errflg++;
      break;
    }

    rfio_write(fd, "TEST_PRESEEK\n", 13); 
   if (rv < 0) {
	rfio_perror ("First write");
	errflg++;
	break;
      }
    /* Iterating on the list of records to write */
    for (j = 0; j < NBRECORDS_TOWRITE; j++) {

      /* Initializing the buffer */
      for (i = 0; i < lengths[j]; i++) {
	buf[i] = j;
      }

      /* Positionning to the right place for writing */
      rv = rfio_lseek64(fd, (off64_t)offsets[j], SEEK_SET);
      if (rv < 0) {
	fprintf(stderr, "serrno/errno: %d/%d\n", serrno, errno);
	rfio_perror ("rfio_lseek64 for write");
	errflg++;
	break;
      }

      /* Writing the data */
      if (rfio_write (fd, buf, lengths[j]) < 0) {
	rfio_perror ("rfio_write");
	errflg++;
	break;
      }

      printf("Written record %d\n", j); 

    }

    /* All writes done */
    (void)rfio_close (fd);
    if (errflg) break;

    /* Prefetch a few records: the actual offset and lengths of the records
     * is set in the array of iov structures
     */
    if ((fd = rfio_open64 (argv[1], O_RDONLY)) < 0) {
      rfio_perror ("rfio_open64");
      errflg++;
      break;
    }

    /* Looping on the records to read */
    for (j = 0; j < NBRECORDS_TOREAD; j++) {
      
      /* Set record offset */
      iov[j].iov_base = offsets[records_toread[j]];

      /* set length */
      iov[j].iov_len = lengths[records_toread[j]];
    }
    if (rfio_preseek64 (fd, iov, iovnb) < 0) {
      rfio_perror ("rfio_preseek64");
      errflg++;
      break;
    }

    /* Read back the records and check their contents */
    for (j = 0; j < NBRECORDS_TOREAD; j++) {

      if (rfio_lseek64 (fd, (off64_t) iov[j].iov_base, SEEK_SET) < 0) {
	rfio_perror ("rfio_lseek64 for read");
	errflg++;
	break;
      }
      if (rfio_read (fd, buf, iov[j].iov_len) < 0) {
	rfio_perror ("rfio_read");
	errflg++;
	break;
      }

      for (i = 0; i < iov[j].iov_len; i++) {
	if (buf[i] != records_toread[j]) {
	  printf(" ERROR\n");
	  fprintf (stderr, "incorrect data read, record %d\n",
		   records_toread[j]);
	  errflg++;
	  break;
	}
      }
      if (errflg) {
	 printf("Checked record %d: ERROR\n", records_toread[j]);
	 break;
      } else {
	printf("Checked record %d: OK\n", records_toread[j]);
      }      
    }

    (void) rfio_close (fd);
    break;
  }

    if (rfio_unlink (argv[1]) < 0) { 
      rfio_perror ("rfio_unlink"); 
      errflg++; 
    } 
#if defined(_WIN32)
  WSACleanup();
#endif
  exit (errflg ? 1 : 0);
}






