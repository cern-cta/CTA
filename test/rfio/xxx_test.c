/*
 * $Id: xxx_test.c,v 1.3 2003/01/22 11:29:45 jdurand Exp $
 */

/*
 * Copyright (C) 2002 by CERN/IT/DS/HSM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: xxx_test.c,v $ $Revision: 1.3 $ $Date: 2003/01/22 11:29:45 $ CERN IT-DS/HSM Jean-Damien Durand";
#endif /* not lint */

#include <sys/types.h>
#ifndef _WIN32
#include <unistd.h>
#else
#define R_OK 4
#define W_OK 2
#define X_OK 1
#define F_OK 0
#endif
#include <sys/stat.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <fcntl.h>
#include <limits.h>
#include "rfio_api.h"
#include "u64subr.h"
#include "Cgetopt.h"

int main(argc,argv)
	int argc;
	char **argv;
{
	int fd;
	int ifd;
	char dummy[11] = "1234567890";
	int i;
	off_t offset;
	struct stat64 statbuf64;
	struct stat statbuf;
	size_t dummysize = 10;
	ssize_t gotsize;
	off64_t bigsize = INT_MAX-1000;
	u_signed64 savsize;
	char tmpbuf[21];
	char tmpbuf2[21];
	int rfio64 = 32;
	int v3 = 2;
	int c;
	int t = 0;

	static struct Coptions longopts[] =
    {
		{"mode",     REQUIRED_ARGUMENT,  NULL,      'm'},
		{"version",  REQUIRED_ARGUMENT,  NULL,      'v'},
		{"size",     REQUIRED_ARGUMENT,  NULL,      's'},
		{NULL,                 0,                  NULL,        0}
    };

	if (argc <= 1) {
	  usage:
		fprintf(stderr,
				"Usage: %s [options] tmpfile\n"
				"\n"
				" where options can be:\n"
				" -s <size>      This program will do a seek at <size> and write/read few bytes\n"
				"                Default is INT_MAX-1000=%d-1000=%d\n"
				" -v <2|3>       This program will run in RFIOV2 or RFIOV3\n"
				"                Default is RFIOV2\n"
				" -m <32|64>     This program will call 32-bits or 64-bits routines\n"
				"                Default is 32-bits mode\n"
				,argv[0],INT_MAX,INT_MAX-1000);
		exit(1);
	}

	Coptind = 1;
	Copterr = 1;
	while ((c = Cgetopt_long (argc, argv, "m:v:s:", longopts, NULL)) != -1) {
		switch (c) {
		case 'm':
			if (strcmp(Coptarg,"32") == 0) {
				/* Default */
			} else if (strcmp(Coptarg,"64") == 0) {
				rfio64 = 64;
			} else {
				goto usage;
			}
			break;
		case 'v':
			if (strcmp(Coptarg,"2") == 0) {
				/* Default */
			} else if (strcmp(Coptarg,"3") == 0) {
				v3 = 3;
			} else {
				goto usage;
			}
			break;
		case 's':
			bigsize = strtou64(Coptarg);
			break;
		default:
			goto usage;
		}
	}

	if (argc <= Coptind) {
		goto usage;
	}

	if (v3 == 3) {
		int v = RFIO_STREAM;
		rfiosetopt(RFIO_READOPT,&v,4);
	}

	if (rfio64 == 64) {
		fprintf(stdout,"[RFIOV%d-%dbits-T%02d] Opening %s in 64bits-write-mode... ", v3, rfio64, ++t, argv[Coptind]);
		if ((fd = rfio_open64(argv[Coptind],O_CREAT|O_TRUNC|O_RDWR, 0755)) < 0) {
			fprintf(stdout,"NOT OK\n");
			fflush(stdout);
			rfio_perror("rfio_open64");
			exit(1);
		}
		fprintf(stdout,"OK\n");
	} else {
		fprintf(stdout,"[RFIOV%d-%dbits-T%02d] Opening %s in 32bits-write-mode... ", v3, rfio64, ++t, argv[Coptind]);
		if ((fd = rfio_open(argv[Coptind],O_CREAT|O_TRUNC|O_RDWR, 0755)) < 0) {
			fprintf(stdout,"NOT OK\n");
			fflush(stdout);
			rfio_perror("rfio_open");
			exit(1);
		}
		fprintf(stdout,"OK\n");
	}

	/* Test rfio_access */
	fprintf(stdout,"[RFIOV%d-%dbits-T%02d] Test rfio_access(path,W_OK|R_OK|F_OK)... ", v3, rfio64, ++t);
	if (rfio_access(argv[Coptind],W_OK|R_OK|F_OK) != 0) {
		fprintf(stdout,"NOT OK\n");
		fflush(stdout);
		rfio_perror("rfio_access");
		exit(1);
	}
	fprintf(stdout,"OK\n");
	
	/* Test rfio_chmod */
	fprintf(stdout,"[RFIOV%d-%dbits-T%02d] Test rfio_chmod(path,0777)... ", v3, rfio64, ++t);
	if (rfio_chmod(argv[Coptind],0777) != 0) {
		fprintf(stdout,"NOT OK\n");
		fflush(stdout);
		rfio_perror("rfio_chmod");
		exit(1);
	}
	fprintf(stdout,"OK\n");

    /* In V3 mode we can seek only before any write or read */
	fprintf(stdout,"[RFIOV%d-%dbits-T%02d] Seek at %s bytes... ", v3, rfio64, ++t, u64tostr((u_signed64) bigsize, tmpbuf, 0));
	if (rfio64 == 64) {
		if (rfio_lseek64(fd,(off64_t) bigsize,SEEK_SET) < 0) {
			fprintf(stdout,"NOT OK\n");
			fflush(stdout);
			rfio_perror("rfio_lseek64");
			exit(1);
		}
	} else {
		if (rfio_lseek(fd,(off_t) bigsize,SEEK_SET) < 0) {
			fprintf(stdout,"NOT OK\n");
			fflush(stdout);
			rfio_perror("rfio_lseek");
			exit(1);
		}
	}
	fprintf(stdout,"OK\n");

	/* Write ten times dummy buffer */
	fprintf(stdout,"[RFIOV%d-%dbits-T%02d] Writing 10 * 10 bytes... ", v3, rfio64, ++t);
	for (i = 0; i < 10; i++) {
		if ((gotsize = rfio_write(fd,dummy,dummysize)) != dummysize) {
			fprintf(stdout,"\n");
			fflush(stdout);
			if (gotsize < 0) {
				rfio_perror("rfio_write");
			} else {
				fprintf(stderr,"rfio_write error: cannot extend file\n");
			}
			exit(1);
		}
	}
	fprintf(stdout,"OK\n");

	if (v3 == 2) {
		
		fprintf(stdout,"[RFIOV%d-%dbits-T%02d] Seek 10 bytes upper... ", v3, rfio64, ++t);
		/* Going 10 bytes more, creating a gap */
		if (rfio64 == 64) {
			if (rfio_lseek64(fd,10,SEEK_CUR) < 0) {
				fprintf(stdout,"NOT OK\n");
				fflush(stdout);
				rfio_perror("rfio_lseek64");
				exit(1);
			}
		} else {
			if (rfio_lseek(fd,10,SEEK_CUR) < 0) {
				fprintf(stdout,"NOT OK\n");
				fflush(stdout);
				rfio_perror("rfio_lseek");
				exit(1);
			}
		}
		fprintf(stdout,"OK\n");
		
		/* Write one time dummy buffer */
		fprintf(stdout,"[RFIOV%d-%dbits-T%02d] Writing 10 bytes... ", v3, rfio64, ++t);
		if ((gotsize = rfio_write(fd,dummy,dummysize)) != dummysize) {
			fprintf(stdout,"NOT OK\n");
			fflush(stdout);
			if (gotsize < 0) {
				rfio_perror("rfio_write");
			} else {
				fprintf(stderr,"rfio_write error: cannot extend file\n");
			}
			exit(1);
		}
		fprintf(stdout,"OK\n");
		
		/* Try to read 10 bytes */
		fprintf(stdout,"[RFIOV%d-%dbits-T%02d] Try to read 10 bytes [this must fail]... ", v3, rfio64, ++t);
		if ((gotsize = rfio_read(fd,dummy,dummysize)) == 1) {
			fprintf(stderr,"rfio_read succeeded !?\n");
			fflush(stdout);
			exit(1);
		}
		if (gotsize < 0) {
			rfio_perror("rfio_read");
		} else {
			fprintf(stderr,"rfio_read error !\n");
		}
		fprintf(stdout,"OK\n");

		fprintf(stdout,"[RFIOV%d-%dbits-T%02d] Seek at %s bytes... ", v3, rfio64, ++t, u64tostr((u_signed64) bigsize, tmpbuf, 0));
		if (rfio64 == 64) {
			if (rfio_lseek64(fd,(off64_t) bigsize,SEEK_SET) < 0) {
				fprintf(stdout,"NOT OK\n");
				fflush(stdout);
				rfio_perror("rfio_lseek64");
				exit(1);
			}
		} else {
			if (rfio_lseek(fd,(off_t) bigsize,SEEK_SET) < 0) {
				fprintf(stdout,"NOT OK\n");
				fflush(stdout);
				rfio_perror("rfio_lseek");
				exit(1);
			}
		}
		fprintf(stdout,"OK\n");

	}
	
	/* Write one time dummy buffer */
	fprintf(stdout,"[RFIOV%d-%dbits-T%02d] Writing 10 bytes... ", v3, rfio64, ++t);
	if ((gotsize = rfio_write(fd,dummy,dummysize)) != dummysize) {
		fprintf(stdout,"NOT OK\n");
		fflush(stdout);
		if (gotsize < 0) {
			rfio_perror("rfio_write");
		} else {
			fprintf(stderr,"rfio_write error: cannot extend file\n");
		}
		exit(1);
	}
	fprintf(stdout,"OK\n");

	/* Close file */
	fprintf(stdout,"[RFIOV%d-%dbits-T%02d] Closing %s... ", v3, rfio64, ++t, argv[Coptind]);
	if (rfio_close(fd) != 0) {
		fprintf(stdout,"NOT OK\n");
		fflush(stdout);
		rfio_perror("rfio_close");
		exit(1);
	}
	fprintf(stdout,"OK\n");

#ifdef PRIVDEBUG
	exit(0);
#endif
	
	/* Stat the file */
	fprintf(stdout,"[RFIOV%d-%dbits-T%02d] Stating %s... ", v3, rfio64, ++t, argv[Coptind]);
	if (rfio64 == 64) {
		if (rfio_stat64(argv[Coptind],&statbuf64) != 0) {
			fprintf(stdout,"NOT OK\n");
			fflush(stdout);
			rfio_perror("rfio_stat");
			exit(1);
		}
	} else {
		if (rfio_stat(argv[Coptind],&statbuf) != 0) {
			fprintf(stdout,"NOT OK\n");
			fflush(stdout);
			rfio_perror("rfio_stat");
			exit(1);
		}
	}
	if (v3 == 2) {
		char tmpbuf[21];
		char tmpbuf1[21];
		/* In v2 mode we created a gap */
		if ((rfio64 == 64 ? statbuf64.st_size : statbuf.st_size) != (bigsize + 120)) {
			fprintf(stdout,"Size %s != %s - NOT OK\n", u64tostr((rfio64 == 64 ? statbuf64.st_size : statbuf.st_size), tmpbuf, 0), u64tostr((u_signed64) (bigsize+120), tmpbuf1, 0));
			fflush(stdout);
			exit(1);
		}
	} else {
		char tmpbuf[21];
		char tmpbuf1[21];
		if ((rfio64 == 64 ? statbuf64.st_size : statbuf.st_size) != (bigsize + 110)) {
			fprintf(stdout,"Size %s != %s - NOT OK\n", u64tostr((rfio64 == 64 ? statbuf64.st_size : statbuf.st_size), tmpbuf, 0), u64tostr((u_signed64) (bigsize+10), tmpbuf1, 0));
			fflush(stdout);
			exit(1);
		}
	}
	{
		char tmpbuf1[21];
		if (v3 == 2) {
			fprintf(stdout,"Size %s - OK\n", u64tostr((u_signed64) (bigsize+120), tmpbuf1, 0));
		} else {
			fprintf(stdout,"Size %s - OK\n", u64tostr((u_signed64) (bigsize+110), tmpbuf1, 0));
		}
	}
	

	/* MStat the file */
	fprintf(stdout,"[RFIOV%d-%dbits-T%02d] MStating %s... ", v3, rfio64, ++t, argv[Coptind]);
	if (rfio64 == 64) {
		if (rfio_mstat64(argv[Coptind],&statbuf64) != 0) {
			fprintf(stdout,"NOT OK\n");
			fflush(stdout);
			rfio_perror("rfio_mstat");
			exit(1);
		}
	} else {
		if (rfio_mstat(argv[Coptind],&statbuf) != 0) {
			fprintf(stdout,"NOT OK\n");
			fflush(stdout);
			rfio_perror("rfio_mstat");
			exit(1);
		}
	}
	rfio_end();
	if (v3 == 2) {
		char tmpbuf[21];
		char tmpbuf1[21];
		/* In v2 mode we created a gap */
		if ((rfio64 == 64 ? statbuf64.st_size : statbuf.st_size) != (bigsize + 120)) {
			fprintf(stdout,"Size %s != %s - NOT OK\n", u64tostr((rfio64 == 64 ? statbuf64.st_size : statbuf.st_size), tmpbuf, 0), u64tostr((u_signed64) (bigsize+120), tmpbuf1, 0));
			fflush(stdout);
			exit(1);
		}
		savsize = bigsize + 120;
	} else {
		char tmpbuf[21];
		char tmpbuf1[21];
		if ((rfio64 == 64 ? statbuf64.st_size : statbuf.st_size) != (bigsize + 110)) {
			fprintf(stdout,"Size %s != %s - NOT OK\n", u64tostr((rfio64 == 64 ? statbuf64.st_size : statbuf.st_size), tmpbuf, 0), u64tostr((u_signed64) (bigsize+10), tmpbuf1, 0));
			fflush(stdout);
			exit(1);
		}
		savsize = bigsize + 110;
	}
	{
		char tmpbuf1[21];
		if (v3 == 2) {
			fprintf(stdout,"Size %s - OK\n", u64tostr((u_signed64) (bigsize+120), tmpbuf1, 0));
		} else {
			fprintf(stdout,"Size %s - OK\n", u64tostr((u_signed64) (bigsize+110), tmpbuf1, 0));
		}
	}
	

#ifdef PRIVDEBUG
	exit(0);
#endif
	
	/* Open it in readmode */
	if (rfio64 == 64) {
		fprintf(stdout,"[RFIOV%d-%dbits-T%02d] Opening %s in 64bits-read-mode... ", v3, rfio64, ++t, argv[Coptind]);
		if ((fd = rfio_open64(argv[Coptind],O_RDONLY,0755)) < 0) {
			fprintf(stdout,"NOT OK\n");
			fflush(stdout);
			rfio_perror("rfio_open64");
			exit(1);
		}
		fprintf(stdout,"OK\n");
	} else {
		fprintf(stdout,"[RFIOV%d-%dbits-T%02d] Opening %s in 32bits-read-mode... ", v3, rfio64, ++t, argv[Coptind]);
		if ((fd = rfio_open(argv[Coptind],O_RDONLY,0755)) < 0) {
			fprintf(stdout,"NOT OK\n");
			fflush(stdout);
			rfio_perror("rfio_open");
			exit(1);
		}
		fprintf(stdout,"OK\n");
	}

	fprintf(stdout,"[RFIOV%d-%dbits-T%02d] Seek at %s bytes... ", v3, rfio64, ++t, u64tostr((u_signed64) (bigsize-10), tmpbuf, 0));
	if (rfio64 == 64) {
		if (rfio_lseek64(fd,(off64_t) (bigsize-10),SEEK_SET) < 0) {
			fprintf(stdout,"NOT OK\n");
			fflush(stdout);
			rfio_perror("rfio_lseek64");
			exit(1);
		}
	} else {
		if (rfio_lseek(fd,(off_t) (bigsize-10),SEEK_SET) < 0) {
			fprintf(stdout,"NOT OK\n");
			fflush(stdout);
			rfio_perror("rfio_lseek");
			exit(1);
		}
	}
	fprintf(stdout,"OK\n");
	
	/* Read 10 bytes */
	fprintf(stdout,"[RFIOV%d-%dbits-T%02d] Read 10 bytes... ", v3, rfio64, ++t);
	if ((gotsize = rfio_read(fd,dummy,dummysize)) != dummysize) {
		fprintf(stdout,"NOT OK\n");
		fflush(stdout);
		if (gotsize < 0) {
			rfio_perror("rfio_read");
		} else {
			fprintf(stderr,"rfio_read error !\n");
		}
		exit(1);
	}
	fprintf(stdout,"OK\n");
	
	/* Check the bytes */
	fprintf(stdout,"[RFIOV%d-%dbits-T%02d] Checking content... ", v3, rfio64, ++t);
	dummy[10] = '\0';
	if (strcmp(dummy,"") != 0) {
		fprintf(stderr,"NOT OK\n==> Wrong content, we should have had \"%s\", we got \"%s\"\n", "",dummy);
		fflush(stdout);
		exit(1);
	}
	fprintf(stdout,"OK\n");
	
	/* Read 10 bytes agains */
	if (v3 == 2) {
		fprintf(stdout,"[RFIOV%d-%dbits-T%02d] Read 10 bytes again... ", v3, rfio64, ++t);
	} else {
		fprintf(stdout,"[RFIOV%d-%dbits-T%02d] Read 10 bytes... ", v3, rfio64, ++t);
	}
	if ((gotsize = rfio_read(fd,dummy,dummysize)) != dummysize) {
		fprintf(stdout,"NOT OK\n");
		fflush(stdout);
		if (gotsize < 0) {
			rfio_perror("rfio_read");
		} else {
			fprintf(stderr,"rfio_read error !\n");
		}
		exit(1);
	}
	fprintf(stdout,"OK\n");

	/* Check the bytes */
	fprintf(stdout,"[RFIOV%d-%dbits-T%02d] Checking content... ", v3, rfio64, ++t);
	dummy[10] = '\0';
	if (strcmp(dummy,"1234567890") != 0) {
		fprintf(stderr,"NOT OK\n==>Wrong content, we should have had \"%s\", we got \"%s\"\n", "123567890",dummy);
		fflush(stdout);
		exit(1);
	}
	fprintf(stdout,"OK\n");
	
	if (v3 == 2) {
		/* Try to write 10 bytes */
		fprintf(stdout,"[RFIOV%d-%dbits-T%02d] Try to write 10 bytes [this must fail]... ", v3, rfio64, ++t);
		if ((gotsize = rfio_write(fd,dummy,dummysize)) == 1) {
			fprintf(stderr,"rfio_write succeeded !?\n");
			fflush(stdout);
			exit(1);
		}
		if (gotsize < 0) {
			rfio_perror("rfio_write");
		} else {
			fprintf(stderr,"rfio_write error: cannot extend file\n");
		}
		fprintf(stdout,"OK\n");

		fprintf(stdout,"[RFIOV%d-%dbits-T%02d] Seek at %s bytes... ", v3, rfio64, ++t, u64tostr((u_signed64) bigsize, tmpbuf, 0));
		if (rfio64 == 64) {
			if (rfio_lseek64(fd,(off64_t) bigsize,SEEK_SET) < 0) {
				fprintf(stdout,"NOT OK\n");
				fflush(stdout);
				rfio_perror("rfio_lseek64");
				exit(1);
			}
		} else {
			if (rfio_lseek(fd,(off_t) bigsize,SEEK_SET) < 0) {
				fprintf(stdout,"NOT OK\n");
				fflush(stdout);
				rfio_perror("rfio_lseek");
				exit(1);
			}
		}
		fprintf(stdout,"OK\n");
		
		/* Read one time dummy buffer */
		fprintf(stdout,"[RFIOV%d-%d-T%d] Read 10 bytes... ", v3, rfio64, ++t);
		if ((gotsize = rfio_read(fd,dummy,dummysize)) != dummysize) {
			fprintf(stdout,"NOT OK\n");
			fflush(stdout);
			if (gotsize < 0) {
				rfio_perror("rfio_write");
			} else {
				fprintf(stderr,"rfio_write error: cannot extend file\n");
			}
			exit(1);
		}
		fprintf(stdout,"OK\n");
		
		/* Check the bytes */
		fprintf(stdout,"[RFIOV%d-%dbits-T%02d] Checking content... ", v3, rfio64, ++t);
		dummy[10] = '\0';
		if (strcmp(dummy,"1234567890") != 0) {
			fprintf(stderr,"NOT OK\n==>Wrong content, we should have had \"%s\", we got \"%s\"\n", "123567890",dummy);
			fflush(stdout);
			exit(1);
		}
		fprintf(stdout,"OK\n");
	}
	
	/* Close file */
	fprintf(stdout,"[RFIOV%d-%dbits-T%02d] Closing %s... ", v3, rfio64, ++t, argv[Coptind]);
	if (rfio_close(fd) != 0) {
		fprintf(stdout,"NOT OK\n");
		fflush(stdout);
		rfio_perror("rfio_close");
		exit(1);
	}
	fprintf(stdout,"OK\n");

	/* Open it in readmode */
	if (rfio64 == 64) {
		fprintf(stdout,"[RFIOV%d-%dbits-T%02d] Opening %s in 64bits-read-mode... ", v3, rfio64, ++t, argv[Coptind]);
		if ((fd = rfio_open64(argv[Coptind],O_RDONLY,0755)) < 0) {
			fprintf(stdout,"NOT OK\n");
			fflush(stdout);
			rfio_perror("rfio_open64");
			exit(1);
		}
		fprintf(stdout,"OK\n");
	} else {
		fprintf(stdout,"[RFIOV%d-%dbits-T%02d] Opening %s in 32bits-read-mode... ", v3, rfio64, ++t, argv[Coptind]);
		if ((fd = rfio_open(argv[Coptind],O_RDONLY,0755)) < 0) {
			fprintf(stdout,"NOT OK\n");
			fflush(stdout);
			rfio_perror("rfio_open");
			exit(1);
		}
		fprintf(stdout,"OK\n");
	}

	if (v3 == 2) {
		fprintf(stdout,"[RFIOV%d-%dbits-T%02d] Seek at %s bytes... ", v3, rfio64, ++t, u64tostr((u_signed64) bigsize + 110, tmpbuf, 0));
		if (rfio64 == 64) {
			if (rfio_lseek64(fd,(off64_t) (bigsize+110),SEEK_SET) < 0) {
				fprintf(stdout,"NOT OK\n");
				fflush(stdout);
				rfio_perror("rfio_lseek64");
				exit(1);
			}
		} else {
			if (rfio_lseek(fd,(off_t) (bigsize+110),SEEK_SET) < 0) {
				fprintf(stdout,"NOT OK\n");
				fflush(stdout);
				rfio_perror("rfio_lseek");
				exit(1);
			}
		}
		fprintf(stdout,"OK\n");
	} else {
		fprintf(stdout,"[RFIOV%d-%dbits-T%02d] Seek at %s bytes... ", v3, rfio64, ++t, u64tostr((u_signed64) (bigsize+100), tmpbuf, 0));
		if (rfio64 == 64) {
			if (rfio_lseek64(fd,(off64_t) (bigsize+100),SEEK_SET) < 0) {
				fprintf(stdout,"\n");
				fflush(stdout);
				rfio_perror("rfio_lseek64");
				exit(1);
			}
		} else {
			if (rfio_lseek(fd,(off_t) (bigsize+100),SEEK_SET) < 0) {
				fprintf(stdout,"\n");
				fflush(stdout);
				rfio_perror("rfio_lseek");
				exit(1);
			}
		}
		fprintf(stdout,"OK\n");
	}

	fprintf(stdout,"[RFIOV%d-%dbits-T%02d] Read byte per byte up to eof using rfio_read... ", v3, rfio64, ++t);
	i = 0;
	while (1) {
		char thischar;

		if (rfio_read(fd,&thischar,sizeof(thischar)) == 0) {
			fprintf(stdout," EOF\n");
			break;
		}
		if (thischar != dummy[i]) {
			fprintf(stdout,"Got '%c', expected '%c'\n", thischar, dummy[i]);
			fflush(stdout);
			exit(1);
		}
		if (++i > 10) {
			fprintf(stdout,"Exceeded 10 bytes to read - should not be\n");
			fflush(stdout);
			exit(1);
		}
		fprintf(stdout,"\n\tByte No %d OK ('%c')... ",i,thischar);
	}

	/* Close file */
	fprintf(stdout,"[RFIOV%d-%dbits-T%02d] Closing %s... ", v3, rfio64, ++t, argv[Coptind]);
	if (rfio_close(fd) != 0) {
		fprintf(stdout,"NOT OK\n");
		fflush(stdout);
		rfio_perror("rfio_close");
		exit(1);
	}
	fprintf(stdout,"OK\n");

	/* Open it in readmode */
	if (rfio64 == 64) {
		fprintf(stdout,"[RFIOV%d-%dbits-T%02d] Opening %s in 64bits-read-mode... ", v3, rfio64, ++t, argv[Coptind]);
		if ((fd = rfio_open64(argv[Coptind],O_RDONLY,0755)) < 0) {
			fprintf(stdout,"NOT OK\n");
			fflush(stdout);
			rfio_perror("rfio_open64");
			exit(1);
		}
		fprintf(stdout,"OK\n");
	} else {
		fprintf(stdout,"[RFIOV%d-%dbits-T%02d] Opening %s in 32bits-read-mode... ", v3, rfio64, ++t, argv[Coptind]);
		if ((fd = rfio_open(argv[Coptind],O_RDONLY,0755)) < 0) {
			fprintf(stdout,"NOT OK\n");
			fflush(stdout);
			rfio_perror("rfio_open");
			exit(1);
		}
		fprintf(stdout,"OK\n");
	}

	fprintf(stdout,"[RFIOV%d-%dbits-T%02d] Read by chunks of 1M up to eof using rfio_read... ", v3, rfio64, ++t);
	{
		u_signed64 totalread = 0;
		while (1) {
			char dummy[1024 * 1024];
			int thisrc;
			
			thisrc = rfio_read(fd,dummy,sizeof(dummy));
			if (thisrc == 0) {
				fprintf(stdout," EOF\n");
				break;
			} else if (thisrc < 0) {
				rfio_perror("rfio_read");
				exit(1);
			}
			totalread += thisrc;
			fprintf(stdout,"\n... %s bytes (%d%%)",  u64tostr((u_signed64) totalread, tmpbuf, 0), (int) (totalread * 100 / savsize));
			fflush(stdout);
		}
		if (totalread != savsize) {
			fprintf(stderr,"Got %s bytes instead of %s !?\n", u64tostr((u_signed64) totalread, tmpbuf, 0), u64tostr((u_signed64) savsize, tmpbuf2, 0));
		} else {
			fprintf(stdout,"Got %s bytes OK\n", u64tostr((u_signed64) totalread, tmpbuf, 0), u64tostr((u_signed64) savsize, tmpbuf2, 0));
		}
	}
	fprintf(stdout,"OK\n");

	/* Close file */
	fprintf(stdout,"[RFIOV%d-%dbits-T%02d] Closing %s... ", v3, rfio64, ++t, argv[Coptind]);
	if (rfio_close(fd) != 0) {
		fprintf(stdout,"NOT OK\n");
		fflush(stdout);
		rfio_perror("rfio_close");
		exit(1);
	}
	fprintf(stdout,"OK\n");

	/* Unlink file */
	fprintf(stdout,"[RFIOV%d-%dbits-T%d] Unlink %s... ", v3, rfio64, ++t, argv[Coptind]);
	if (rfio_unlink(argv[Coptind]) != 0) {
		fprintf(stdout,"NOT OK\n");
		fflush(stdout);
		rfio_perror("rfio_unlink");
		exit(1);
	}
	fprintf(stdout,"OK\n");

	fprintf(stdout,"\n\n==> ALL TESTS OK <==\n");
	exit(0);

}
