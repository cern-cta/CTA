/*
 * Copyright (C) 2001 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: tpreseek.c,v $ $Revision: 1.1 $ $Date: 2001/05/29 06:10:45 $ CERN IT-PDP/DM Jean-Philippe Baud";
#endif /* not lint */

/*	tpreseek - write NBRECORDS_TOWRITE records and
		   read back NBRECORDS_TOREAD using the rfio_preseek function */

#include <fcntl.h>
#include <stdio.h>
#include "rfio_api.h"
#define NBRECORDS_TOREAD 5
#define NBRECORDS_TOWRITE 10

main(argc, argv)
int argc;
char **argv;
{
	char buf[65536];
	int fd;
	int i;
	struct iovec iov[NBRECORDS_TOREAD];
	int iovnb = NBRECORDS_TOREAD;
	int j;
	int lengths[NBRECORDS_TOWRITE] = {4096, 32768, 16384, 8192, 65536,
					  32768, 16384, 4096, 65536, 8192};
	int records_toread[NBRECORDS_TOREAD] = {2, 4, 5, 8, 9};

	if (argc != 2) {
		fprintf (stderr, "usage: tpreseek pathname\n");
		exit (1);
	}

	/* Write variable length records.
	 * Each record is filled with the record index
	 */

	if ((fd = rfio_open (argv[1], O_WRONLY|O_CREAT|O_TRUNC)) < 0) {
		rfio_perror ("rfio_open");
		exit (1);
	}
	for (j = 0; j < NBRECORDS_TOWRITE; j++) {
		for (i = 0; i < lengths[j]; i++)
			buf[i] = j;
		if (rfio_write (fd, buf, lengths[j]) < 0) {
			rfio_perror ("rfio_write");
			exit (1);
		}
	}
	close (fd);

	/* Prefetch a few records: the actual offset and lengths of the records
	 * is set in the array of iov structures
	 */

	if ((fd = rfio_open (argv[1], O_RDONLY)) < 0) {
		rfio_perror ("rfio_open");
		exit (1);
	}
	for (j = 0; j < NBRECORDS_TOREAD; j++) {
		/* compute record offset */
		for (i = 0; i < records_toread[j]; i++)
			iov[j].iov_base += lengths[i];
		/* set length */
		iov[j].iov_len = lengths[records_toread[j]];
	}
	if (rfio_preseek (fd, iov, iovnb) < 0) {
		rfio_perror ("rfio_preseek");
		exit (1);
	}

	/* Read back the records and check their cpntents */

	for (j = 0; j < NBRECORDS_TOREAD; j++) {
		if (rfio_lseek (fd, (off_t) iov[j].iov_base, SEEK_SET) < 0) {
			rfio_perror ("rfio_lseek");
			exit (1);
		}
		if (rfio_read (fd, buf, iov[j].iov_len) < 0) {
			rfio_perror ("rfio_read");
			exit (1);
		}
		for (i = 0; i < iov[j].iov_len; i++) {
			if (buf[i] != records_toread[j]) {
				fprintf (stderr, "incorrect data read, record %d\n",
				    records_toread[j]);
				exit (1);
			}
		}
	}
	if (rfio_unlink (argv[1]) < 0) {
		rfio_perror ("rfio_unlink");
		exit (1);
	}
	exit (0);
}
