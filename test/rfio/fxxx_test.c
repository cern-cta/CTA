/*
 * $Id: fxxx_test.c,v 1.3 2003/01/22 11:29:45 jdurand Exp $
 */

/*
 * Copyright (C) 2002 by CERN/IT/DS/HSM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: fxxx_test.c,v $ $Revision: 1.3 $ $Date: 2003/01/22 11:29:45 $ CERN IT-DS/HSM Jean-Damien Durand";
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
	FILE *fd;
	char dummy[11] = "1234567890";
	int i;
	off_t offset;
	off64_t offset64;
	struct stat64 statbuf64;
	struct stat statbuf;
	size_t dummysize = 10;
	size_t gotsize;
	off64_t bigsize = INT_MAX-1000;
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
			bigsize = (off64_t) strtou64(Coptarg);
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
		if ((fd = rfio_fopen64(argv[Coptind],"w")) == NULL) {
			fprintf(stdout,"NOT OK\n");
			fflush(stdout);
			rfio_perror("rfio_fopen64");
			exit(1);
		}
		fprintf(stdout,"OK\n");
	} else {
		fprintf(stdout,"[RFIOV%d-%dbits-T%02d] Opening %s in 32bits-write-mode... ", v3, rfio64, ++t, argv[Coptind]);
		if ((fd = rfio_fopen(argv[Coptind],"w")) == NULL) {
			fprintf(stdout,"NOT OK\n");
			fflush(stdout);
			rfio_perror("rfio_fopen");
			exit(1);
		}
		fprintf(stdout,"OK\n");
	}
	
	/* Test rfio_access */
	fprintf(stdout,"[RFIOV%d-%dbits-T%02d] Test rfio_access(W_OK|R_OK|F_OK)... ", v3, rfio64, ++t);
	if (rfio_access(argv[Coptind],W_OK|R_OK|F_OK) != 0) {
		fprintf(stdout,"NOT OK\n");
		fflush(stdout);
		rfio_perror("rfio_access");
		exit(1);
	}
	fprintf(stdout,"OK\n");

	/* Test rfio_fchmod */
	fprintf(stdout,"[RFIOV%d-%dbits-T%02d] Test rfio_fchmod(fd,0777)... ", v3, rfio64, ++t);
	if (rfio_fchmod(rfio_fileno(fd),0777) != 0) {
		fprintf(stdout,"NOT OK\n");
		fflush(stdout);
		if (rfio_ferror(fd)) {
			rfio_perror("rfio_fchmod");
		} else {
			fprintf(stderr,"rfio_fchmod error !\n");
		}
		exit(1);
	}
	fprintf(stdout,"OK\n");

	/* Test seek */
	if (bigsize <= INT_MAX) {
		fprintf(stdout,"[RFIOV%d-%dbits-T%02d] Seek at %s bytes with fseek... ", v3, rfio64, ++t, u64tostr((u_signed64) bigsize, tmpbuf, 0));
		if (rfio_fseek(fd,(long int) bigsize,SEEK_SET) < 0) {
			fprintf(stdout,"NOT OK\n");
			fflush(stdout);
			if (rfio_ferror(fd)) {
				rfio_perror("rfio_fseek");
			} else {
				fprintf(stderr,"rfio_fseek error !\n");
			}
			exit(1);
		}
		fprintf(stdout,"OK\n");
	}
	fprintf(stdout,"[RFIOV%d-%dbits-T%02d] Seek at %s bytes with fseeko64... ", v3, rfio64, ++t, u64tostr((u_signed64) bigsize, tmpbuf, 0));
	if (rfio_fseeko64(fd,(off64_t) bigsize,SEEK_SET) < 0) {
		fprintf(stdout,"NOT OK\n");
		fflush(stdout);
		if (rfio_ferror(fd)) {
			rfio_perror("rfio_fseeko64");
		} else {
			fprintf(stderr,"rfio_fseeko64 error !\n");
		}
		exit(1);
	}
	fprintf(stdout,"OK\n");

	/* Write ten times dummy buffer */
	fprintf(stdout,"[RFIOV%d-%dbits-T%02d] Writing 10 * 10 bytes... ", v3, rfio64, ++t);
	for (i = 0; i < 10; i++) {
		if ((gotsize = rfio_fwrite(dummy,dummysize,1,fd)) != 1) {
			fprintf(stdout,"\n");
			fflush(stdout);
			if (rfio_ferror(fd)) {
				rfio_perror("rfio_fwrite");
			} else {
				fprintf(stderr,"rfio_fwrite error !\n");
			}
			exit(1);
		}
	}
	fprintf(stdout,"OK\n");

	/* Do fflush - no meaning in mode v3 - in mode v2 data is flushed by def */
	/* [but still rfio_fflush() is a dummy operation when the file is remote] */
	if (v3 == 2) {
		fprintf(stdout,"[RFIOV%d-%dbits-T%02d] rfio_fflush(fd)... ", v3, rfio64, ++t);
		if (rfio_fflush(fd) < 0) {
			fprintf(stdout,"NOT OK\n");
			fflush(stdout);
			if (rfio_ferror(fd)) {
				rfio_perror("rfio_fflush");
			} else {
				fprintf(stderr,"rfio_fflush error !\n");
			}
			exit(1);
		}
		fprintf(stdout,"OK\n");
		
		/* Test fstat */
		if (bigsize <= (INT_MAX-100)) {
			fprintf(stdout,"[RFIOV%d-%dbits-T%02d] rfio_fstat(fd,...)... ", v3, rfio64, ++t);
			if (rfio_fstat(rfio_fileno(fd),&statbuf) < 0) {
				fprintf(stdout,"NOT OK\n");
				fflush(stdout);
				if (rfio_ferror(fd)) {
					rfio_perror("rfio_fstat");
				} else {
					fprintf(stderr,"rfio_fstat error !\n");
				}
				exit(1);
			}
			fprintf(stdout,"Got size=%d, should be %d...", (int) statbuf.st_size, (int) (bigsize+100));
			if (statbuf.st_size != (bigsize+100)) {
				fprintf(stdout,"NOT OK\n");
				fflush(stdout);
				exit(1);
			}
			fprintf(stdout,"OK\n");
		}
		fprintf(stdout,"[RFIOV%d-%dbits-T%02d] rfio_fstat64... ", v3, rfio64, ++t);
		if (rfio_fstat64(rfio_fileno(fd),&statbuf64) < 0) {
			fprintf(stdout,"NOT OK\n");
			fflush(stdout);
			if (rfio_ferror(fd)) {
				rfio_perror("rfio_fstat64");
			} else {
				fprintf(stderr,"rfio_fstat64 error !\n");
			}
			exit(1);
		}
		fprintf(stdout,"Got size=%s, should be %s...", u64tostr((u_signed64) statbuf64.st_size, tmpbuf2, 0), u64tostr((u_signed64) (bigsize+100), tmpbuf, 0));
		if (statbuf64.st_size != (bigsize+100)) {
			fprintf(stdout,"NOT OK\n");
			fflush(stdout);
			exit(1);
		}
		fprintf(stdout,"OK\n");
	}

	if (v3 == 2) {
		/* Get current offset */
		if (bigsize <= (INT_MAX-100)) {
			/* Get current offset with ftell */
			fprintf(stdout,"[RFIOV%d-%dbits-T%02d] Getting current offset with rfio_ftell... ", v3, rfio64, ++t);
			offset = rfio_ftell(fd);
			fprintf(stdout,"Got %d, should be %d...", (int) offset, (int) (bigsize+100));
			if (offset != bigsize+100) {
				fprintf(stdout,"NOT OK\n");
				fflush(stdout);
				exit(1);
			}
			fprintf(stdout,"OK\n");
		}
		/* Get current offset with ftello64 - Converted to lseek64(SEEK_CUR) - this is NOT supported in V3 */
		if (v3 == 2) {
			fprintf(stdout,"[RFIOV%d-%dbits-T%02d] Getting current offset with rfio_ftello64... ", v3, rfio64, ++t);
			offset64 = rfio_ftello64(fd);
			if (offset64 < 0) {
				if (rfio_ferror(fd)) {
					rfio_perror("rfio_ftello64");
				} else {
					fprintf(stderr,"rfio_ftello64 error !\n");
				}
			} else {
				char tmpbuf1[21], tmpbuf2[21];
				fprintf(stdout,"Got %s, should be %s...", u64tostr((u_signed64) offset64, tmpbuf1, 0), u64tostr((u_signed64) (bigsize+100), tmpbuf2, 0));
			}
			if (offset64 != bigsize+100) {
				fprintf(stdout,"NOT OK\n");
				fflush(stdout);
				exit(1);
			}
			fprintf(stdout,"OK\n");
		}

		if ((bigsize+110) <= INT_MAX) {
			fprintf(stdout,"[RFIOV%d-%dbits-T%02d] Seek 10 bytes upper with fseek... ", v3, rfio64, ++t);
			/* Going 10 bytes more, creating a gap */
			if (rfio_fseek(fd,10,SEEK_CUR) != 0) {
				fprintf(stdout,"NOT OK\n");
				fflush(stdout);
				if (rfio_ferror(fd)) {
					rfio_perror("rfio_fseek");
				} else {
					fprintf(stderr,"rfio_fseek error !\n");
				}
				exit(1);
			}
			fprintf(stdout,"OK\n");
			fprintf(stdout,"[RFIOV%d-%dbits-T%02d] Seek 10 bytes lower with fseek... ", v3, rfio64, ++t);
			/* Going 10 bytes more, creating a gap */
			if (rfio_fseek(fd,-10,SEEK_CUR) != 0) {
				fprintf(stdout,"NOT OK\n");
				fflush(stdout);
				if (rfio_ferror(fd)) {
					rfio_perror("rfio_fseek");
				} else {
					fprintf(stderr,"rfio_fseek error !\n");
				}
				exit(1);
			}
			fprintf(stdout,"OK\n");
		}
		fprintf(stdout,"[RFIOV%d-%dbits-T%02d] Seek at %s bytes with fseeko64... ", v3, rfio64, ++t, u64tostr((u_signed64) (bigsize+110), tmpbuf, 0));
		if (rfio_fseeko64(fd,(off64_t) (bigsize+110),SEEK_SET) < 0) {
			fprintf(stdout,"NOT OK\n");
			fflush(stdout);
			if (rfio_ferror(fd)) {
				rfio_perror("rfio_fseeko64");
			} else {
				fprintf(stderr,"rfio_fseeko64 error !\n");
			}
			exit(1);
		}
		fprintf(stdout,"OK\n");

		if (bigsize <= (INT_MAX-110)) {
			/* Get current offset with ftell */
			fprintf(stdout,"[RFIOV%d-%dbits-T%02d] Getting current offset... ", v3, rfio64, ++t);
			offset = rfio_ftell(fd);
			fprintf(stdout,"Got %d, should be %d...", (int) offset, (int) (bigsize+110));
			if (offset != bigsize+110) {
				fprintf(stdout,"NOT OK\n");
				fflush(stdout);
				exit(1);
			}
			fprintf(stdout,"OK\n");
		}
		/* Get current offset with ftello64 - Converted to lseek64(SEEK_CUR) - this is NOT supported in V3 */
		if (v3 == 2) {
			fprintf(stdout,"[RFIOV%d-%dbits-T%02d] Getting current offset with rfio_ftello64... ", v3, rfio64, ++t);
			offset64 = rfio_ftello64(fd);
			if (offset64 < 0) {
				if (rfio_ferror(fd)) {
					rfio_perror("rfio_ftello64");
				} else {
					fprintf(stderr,"rfio_ftello64 error !\n");
				}  
			} else {
				char tmpbuf1[21], tmpbuf2[21];
				fprintf(stdout,"Got %s, should be %s...", u64tostr((u_signed64) offset64, tmpbuf1, 0), u64tostr((u_signed64) (bigsize+110), tmpbuf2, 0));
			}
			if (offset64 != bigsize+110) {
				fprintf(stdout,"NOT OK\n");
				fflush(stdout);
				exit(1);
			}
			fprintf(stdout,"OK\n");
		}

		/* Flush buffer */
		fprintf(stdout,"[RFIOV%d-%dbits-T%02d] Flush buffer... ", v3, rfio64, ++t);
		if (rfio_fflush(fd) != 0) {
			fprintf(stdout,"NOT OK\n");
			fflush(stdout);
			if (rfio_ferror(fd)) {
				rfio_perror("rfio_fflush");
			} else {
				fprintf(stderr,"rfio_fflush error !\n");
			}
			exit(1);
		}
		fprintf(stdout,"OK\n");
		
		if (bigsize <= (INT_MAX-110)) {
			/* Get current offset with ftell */
			fprintf(stdout,"[RFIOV%d-%dbits-T%02d] Getting current offset... ", v3, rfio64, ++t);
			offset = rfio_ftell(fd);
			fprintf(stdout,"Got %d, should be %d...", (int) offset, (int) (bigsize+110));
			if (offset != bigsize+110) {
				fprintf(stdout,"NOT OK\n");
				fflush(stdout);
				exit(1);
			}
			fprintf(stdout,"OK\n");
		}
		/* Get current offset with ftello64 - Converted to lseek64(SEEK_CUR) - this is NOT supported in V3 */
		if (v3 == 2) {
			fprintf(stdout,"[RFIOV%d-%dbits-T%02d] Getting current offset with rfio_ftello64... ", v3, rfio64, ++t);
			offset64 = rfio_ftello64(fd);
			if (offset64 < 0) {
				if (rfio_ferror(fd)) {
					rfio_perror("rfio_ftello64");
				} else {
					fprintf(stderr,"rfi_ftello64 error !\n");
				}
			} else {
				char tmpbuf1[21], tmpbuf2[21];
				fprintf(stdout,"Got %s, should be %s...", u64tostr((u_signed64) offset64, tmpbuf1, 0), u64tostr((u_signed64) (bigsize+110), tmpbuf2, 0));
			}
			if (offset64 != bigsize+110) {
				fprintf(stdout,"NOT OK\n");
				fflush(stdout);
				exit(1);
			}
			fprintf(stdout,"OK\n");
		}


		/* Write one time dummy buffer */
		fprintf(stdout,"[RFIOV%d-%dbits-T%02d] Writing 10 bytes... ", v3, rfio64, ++t);
		if ((gotsize = rfio_fwrite(dummy,dummysize,1,fd)) != 1) {
			fprintf(stdout,"NOT OK\n");
			fflush(stdout);
			if (rfio_ferror(fd)) {
				rfio_perror("rfio_fwrite");
			} else {
				fprintf(stderr,"rfio_fwrite error !\n");
			}
			exit(1);
		}
		fprintf(stdout,"OK\n");
		
		if (bigsize <= (INT_MAX-110)) {
			/* Get current offset with ftell */
			fprintf(stdout,"[RFIOV%d-%dbits-T%02d] Getting current offset... ", v3, rfio64, ++t);
			offset = rfio_ftell(fd);
			fprintf(stdout,"Got %d, should be %d...", (int) offset, (int) (bigsize+120));
			if (offset != bigsize+120) {
				fprintf(stdout,"NOT OK\n");
				fflush(stdout);
				exit(1);
			}
			fprintf(stdout,"OK\n");
		}
		/* Get current offset with ftello64 - Converted to lseek64(SEEK_CUR) - this is NOT supported in V3 */
		if (v3 == 2) {
			fprintf(stdout,"[RFIOV%d-%dbits-T%02d] Getting current offset with rfio_ftello64... ", v3, rfio64, ++t);
			offset64 = rfio_ftello64(fd);
			if (offset64 < 0) {
				if (rfio_ferror(fd)) {
					rfio_perror("rfio_ftello64");
				} else {
					fprintf(stderr,"rfio_ftello64 error !\n");
				}
			} else {
				char tmpbuf1[21], tmpbuf2[21];
				fprintf(stdout,"Got %s, should be %s...", u64tostr((u_signed64) offset64, tmpbuf1, 0), u64tostr((u_signed64) (bigsize+120), tmpbuf2, 0));
			}
			if (offset64 != bigsize+120) {
				fprintf(stdout,"NOT OK\n");
				fflush(stdout);
				exit(1);
			}
			fprintf(stdout,"OK\n");
		}

		/* Try to read 10 bytes */
		fprintf(stdout,"[RFIOV%d-%dbits-T%02d] Try to read 10 bytes [this must fail]... ", v3, rfio64, ++t);
		if ((gotsize = rfio_fread(dummy,dummysize,1,fd)) == 1) {
			fprintf(stderr,"rfio_fread succeeded !?\n");
			fflush(stdout);
			exit(1);
		}
		if (rfio_ferror(fd)) {
			rfio_perror("rfio_fread");
		} else {
			fprintf(stderr,"rfio_fread error !\n");
		}
		fprintf(stdout,"OK\n");
	
		if (bigsize <= INT_MAX) {
			fprintf(stdout,"[RFIOV%d-%dbits-T%02d] Seek at %s bytes with fseek... ", v3, rfio64, ++t, u64tostr((u_signed64) bigsize, tmpbuf, 0));
			
			if (rfio_fseek(fd,(long int) bigsize,SEEK_SET) < 0) {
				fprintf(stdout,"NOT OK\n");
				fflush(stdout);
				if (rfio_ferror(fd)) {
					rfio_perror("rfio_fseek");
				} else {
					fprintf(stderr,"rfio_fseek error !\n");
				}
				exit(1);
			}
			fprintf(stdout,"OK\n");
		}
		fprintf(stdout,"[RFIOV%d-%dbits-T%02d] Seek at %s bytes with fseeko64... ", v3, rfio64, ++t, u64tostr((u_signed64) bigsize, tmpbuf, 0));
		
		if (rfio_fseeko64(fd,(off64_t) bigsize,SEEK_SET) < 0) {
			fprintf(stdout,"NOT OK\n");
			fflush(stdout);
			if (rfio_ferror(fd)) {
				rfio_perror("rfio_fseeko64");
			} else {
				fprintf(stderr,"rfio_fseeko64 error !\n");
			}
			exit(1);
		}
		fprintf(stdout,"OK\n");

		if (bigsize <= INT_MAX) {
			/* Get current offset with ftell */
			fprintf(stdout,"[RFIOV%d-%dbits-T%02d] Getting current offset... ", v3, rfio64, ++t);
			offset = rfio_ftell(fd);
			fprintf(stdout,"Got %d, should be %d...", (int) offset, (int) bigsize);
			if (offset != bigsize) {
				fprintf(stdout,"NOT OK\n");
				fflush(stdout);
				exit(1);
			}
			fprintf(stdout,"OK\n");
		}
		/* Get current offset with ftello64 - Converted to lseek64(SEEK_CUR) - this is NOT supported in V3 */
		if (v3 == 2) {
			fprintf(stdout,"[RFIOV%d-%dbits-T%02d] Getting current offset with rfio_ftello64... ", v3, rfio64, ++t);
			offset64 = rfio_ftello64(fd);
			if (offset64 < 0) {
				if (rfio_ferror(fd)) {
					rfio_perror("rfio_ftello64");
				} else {
					fprintf(stderr,"rfio_ftello64 error !\n");
				}
			} else {
				char tmpbuf1[21], tmpbuf2[21];
				fprintf(stdout,"Got %s, should be %s...", u64tostr((u_signed64) offset64, tmpbuf1, 0), u64tostr((u_signed64) bigsize, tmpbuf2, 0));
			}
			if (offset64 != bigsize) {
				fprintf(stdout,"NOT OK\n");
				fflush(stdout);
				exit(1);
			}
			fprintf(stdout,"OK\n");
		}

	}

	/* Write one time dummy buffer */
	fprintf(stdout,"[RFIOV%d-%dbits-T%02d] Writing 10 bytes... ", v3, rfio64, ++t);
	if ((gotsize = rfio_fwrite(dummy,dummysize,1,fd)) != 1) {
		fprintf(stdout,"NOT OK\n");
		fflush(stdout);
		if (rfio_ferror(fd)) {
			rfio_perror("rfio_fwrite");
		} else {
			fprintf(stderr,"rfio_fwrite error !\n");
		}
		exit(1);
	}
	fprintf(stdout,"OK\n");

	/* Close file */
	fprintf(stdout,"[RFIOV%d-%dbits-T%02d] Closing %s... ", v3, rfio64, ++t, argv[Coptind]);
	if (rfio_fclose(fd) != 0) {
		fprintf(stdout,"NOT OK\n");
		fflush(stdout);
		rfio_perror("rfio_fclose");
		exit(1);
	}
	fprintf(stdout,"OK\n");

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
		if ((rfio64 == 64 ? statbuf64.st_size : statbuf.st_size) != (bigsize+110)) {
			fprintf(stdout,"Size %s != %s - NOT OK\n", u64tostr((rfio64 == 64 ? statbuf64.st_size : statbuf.st_size), tmpbuf, 0), u64tostr((u_signed64) (bigsize+110), tmpbuf1, 0));
			fflush(stdout);
			exit(1);
		}
	}
	if (v3 == 2) {
		char tmpbuf1[21];
		fprintf(stdout,"Size %s - OK\n", u64tostr((u_signed64) (bigsize+120), tmpbuf1, 0));
	} else {
		char tmpbuf1[21];
		fprintf(stdout,"Size %s - OK\n", u64tostr((u_signed64) (bigsize+110), tmpbuf1, 0));
	}
	
	/* Open it in readmode */
	if (rfio64 == 64) {
		fprintf(stdout,"[RFIOV%d-%dbits-T%02d] Opening %s in 64bits-read-mode... ", v3, rfio64, ++t, argv[Coptind]);
		if ((fd = rfio_fopen64(argv[Coptind],"r")) == NULL) {
			fprintf(stdout,"NOT OK\n");
			fflush(stdout);
			rfio_perror("rfio_fopen64");
			exit(1);
		}
		fprintf(stdout,"OK\n");
	} else {
		fprintf(stdout,"[RFIOV%d-%dbits-T%02d] Opening %s in 32bits-read-mode... ", v3, rfio64, ++t, argv[Coptind]);
		if ((fd = rfio_fopen(argv[Coptind],"r")) == NULL) {
			fprintf(stdout,"NOT OK\n");
			fflush(stdout);
			rfio_perror("rfio_fopen");
			exit(1);
		}
		fprintf(stdout,"OK\n");
	}
	if ((bigsize-(v3 == 3 ? 0 : 10)) <= INT_MAX) {
		fprintf(stdout,"[RFIOV%d-%dbits-T%02d] Seek at %s bytes with fseek... ", v3, rfio64, ++t, u64tostr((u_signed64) (bigsize-(v3 == 3 ? 0 : 10)), tmpbuf, 0));

		if (rfio_fseek(fd,(long int) (bigsize-(v3 == 3 ? 0 : 10)),SEEK_SET) < 0) {
			fprintf(stdout,"NOT OK\n");
			fflush(stdout);
			if (rfio_ferror(fd)) {
				rfio_perror("rfio_fseek");
			} else {
				fprintf(stderr,"rfio_fseek error !\n");
			}
			exit(1);
		}
		fprintf(stdout,"OK\n");
	}
	fprintf(stdout,"[RFIOV%d-%dbits-T%02d] Seek at %s bytes with fseeko64... ", v3, rfio64, ++t, u64tostr((u_signed64) (bigsize-(v3 == 3 ? 0 : 10)), tmpbuf, 0));
	
	if (rfio_fseeko64(fd,(off64_t) (bigsize-(v3 == 3 ? 0 : 10)),SEEK_SET) < 0) {
		fprintf(stdout,"NOT OK\n");
		fflush(stdout);
		if (rfio_ferror(fd)) {
			rfio_perror("rfio_fseeko64");
		} else {
			fprintf(stderr,"rfio_fseeko64 error !\n");
		}
		exit(1);
	}
	fprintf(stdout,"OK\n");
	
	if (v3 == 2) {
		if ((bigsize-10) <= INT_MAX) {
			/* Get current offset with ftell */
			fprintf(stdout,"[RFIOV%d-%dbits-T%02d] Getting current offset... ", v3, rfio64, ++t);
			offset = rfio_ftell(fd);
			fprintf(stdout,"Got %d, should be %d...", (int) offset, (int) (bigsize-10));
			if (offset != bigsize-10) {
				fprintf(stdout,"NOT OK\n");
				fflush(stdout);
				exit(1);
			}
			fprintf(stdout,"OK\n");
		}
		/* Get current offset with ftello64 - Converted to lseek64(SEEK_CUR) - this is NOT supported in V3 */
		if (v3 == 2) {
			fprintf(stdout,"[RFIOV%d-%dbits-T%02d] Getting current offset with rfio_ftello64... ", v3, rfio64, ++t);
			offset64 = rfio_ftello64(fd);
			if (offset64 < 0) {
				if (rfio_ferror(fd)) {
					rfio_perror("rfio_ftello64");
				} else {
					fprintf(stderr,"rfio_ftello64 error !\n");
				}
			} else {
				char tmpbuf1[21], tmpbuf2[21];
				fprintf(stdout,"Got %s, should be %s...", u64tostr((u_signed64) offset64, tmpbuf1, 0), u64tostr((u_signed64) (bigsize-10), tmpbuf2, 0));
			}
			if (offset64 != bigsize-10) {
				fprintf(stdout,"NOT OK\n");
				fflush(stdout);
				exit(1);
			}
			fprintf(stdout,"OK\n");
		}

		/* Read 10 bytes */
		fprintf(stdout,"[RFIOV%d-%dbits-T%02d] Read 10 bytes... ", v3, rfio64, ++t);
		if ((gotsize = rfio_fread(dummy,dummysize,1,fd)) != 1) {
			fprintf(stdout,"NOT OK\n");
			fflush(stdout);
			if (rfio_ferror(fd)) {
				rfio_perror("rfio_fread");
			} else {
				fprintf(stderr,"rfio_fread error !\n");
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
	}
	
	/* Read 10 bytes agains */
	if (v3 == 2) {
		fprintf(stdout,"[RFIOV%d-%dbits-T%02d] Read 10 bytes again... ", v3, rfio64, ++t);
	} else {
		fprintf(stdout,"[RFIOV%d-%dbits-T%02d] Read 10 bytes... ", v3, rfio64, ++t);
	}
	if ((gotsize = rfio_fread(dummy,dummysize,1,fd)) != 1) {
		fprintf(stdout,"NOT OK\n");
		fflush(stdout);
		if (rfio_ferror(fd)) {
			rfio_perror("rfio_fread");
		} else {
			fprintf(stderr,"rfio_fread error !\n");
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
		if ((gotsize = rfio_fwrite(dummy,dummysize,1,fd)) == 1) {
			fprintf(stderr,"rfio_fwrite succeeded !?\n");
			fflush(stdout);
			exit(1);
		}
		if (rfio_ferror(fd)) {
			rfio_perror("rfio_fwrite");
		} else {
			fprintf(stderr,"rfio_fwrite error !\n");
		}
		fprintf(stdout,"OK\n");

		if (bigsize <= INT_MAX) {
			fprintf(stdout,"[RFIOV%d-%dbits-T%02d] Seek at %s bytes with fseek... ", v3, rfio64, ++t, u64tostr((u_signed64) bigsize, tmpbuf, 0));

			if (rfio_fseek(fd,(long int) bigsize,SEEK_SET) < 0) {
				fprintf(stdout,"NOT OK\n");
				fflush(stdout);
				if (rfio_ferror(fd)) {
					rfio_perror("rfio_fseek");
				} else {
					fprintf(stderr,"rfio_fseek error !\n");
				}
				exit(1);
			}
			fprintf(stdout,"OK\n");
		}
		fprintf(stdout,"[RFIOV%d-%dbits-T%02d] Seek at %s bytes with fseeko64... ", v3, rfio64, ++t, u64tostr((u_signed64) bigsize, tmpbuf, 0));
		
		if (rfio_fseeko64(fd,(off64_t) bigsize,SEEK_SET) < 0) {
			fprintf(stdout,"NOT OK\n");
			fflush(stdout);
			if (rfio_ferror(fd)) {
				rfio_perror("rfio_fseeko64");
			} else {
				fprintf(stderr,"rfio_fseeko64 error !\n");
			}
			exit(1);
		}
		fprintf(stdout,"OK\n");

		if (bigsize <= INT_MAX) {
			/* Get current offset with ftell */
			fprintf(stdout,"[RFIOV%d-%dbits-T%02d] Getting current offset... ", v3, rfio64, ++t);
			offset = rfio_ftell(fd);
			fprintf(stdout,"Got %d, should be %d...", (int) offset, (int) (bigsize+110));
			if (offset != bigsize) {
				fprintf(stdout,"NOT OK\n");
				fflush(stdout);
				exit(1);
			}
			fprintf(stdout,"OK\n");
		}
		/* Get current offset with ftello64 - Converted to lseek64(SEEK_CUR) - this is NOT supported in V3 */
		if (v3 == 2) {
			fprintf(stdout,"[RFIOV%d-%dbits-T%02d] Getting current offset with rfio_ftello64... ", v3, rfio64, ++t);
			offset64 = rfio_ftello64(fd);
			if (offset64 < 0) {
				if (rfio_ferror(fd)) {
					rfio_perror("rfio_ftello64");
				} else {
					fprintf(stderr,"rfio_ftello64 error !\n");
				}
			} else {
				char tmpbuf1[21], tmpbuf2[21];
				fprintf(stdout,"Got %s, should be %s...", u64tostr((u_signed64) offset64, tmpbuf1, 0), u64tostr((u_signed64) bigsize, tmpbuf2, 0));
			}
			if (offset64 != bigsize) {
				fprintf(stdout,"NOT OK\n");
				fflush(stdout);
				exit(1);
			}
			fprintf(stdout,"OK\n");
		}

		/* Read one time dummy buffer */
		fprintf(stdout,"[RFIOV%d-%dbits-T%02d] Read 10 bytes... ", v3, rfio64, ++t);
		if ((gotsize = rfio_fread(dummy,dummysize,1,fd)) != 1) {
			fprintf(stdout,"NOT OK\n");
			fflush(stdout);
			if (rfio_ferror(fd)) {
				rfio_perror("rfio_fwrite");
			} else {
				fprintf(stderr,"rfio_fwrite error !\n");
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
	if (rfio_fclose(fd) != 0) {
		fprintf(stdout,"NOT OK\n");
		fflush(stdout);
		rfio_perror("rfio_fclose");
		exit(1);
	}
	fprintf(stdout,"OK\n");

	/* Try to read 10 bytes */
	/* The following can SIGSEGV and it is normal! */
	/*
	  fprintf(stdout,"[RFIOV%d-%dbits-T%02d] Try to read 10 bytes [this must fail]... ", v3, rfio64, ++t);
	  if ((gotsize = rfio_fread(dummy,dummysize,1,fd)) == 1) {
	  fprintf(stderr,"rfio_fread succeeded !?\n");
	  fflush(stdout);
	  exit(1);
	  }
	  fprintf(stdout,"OK\n");
	*/
	
	/* Open it in readmode */
	if (rfio64 == 64) {
		fprintf(stdout,"[RFIOV%d-%dbits-T%02d] Opening %s in 64bits-read-mode... ", v3, rfio64, ++t, argv[Coptind]);
		if ((fd = rfio_fopen64(argv[Coptind],"r")) == NULL) {
			fprintf(stdout,"NOT OK\n");
			fflush(stdout);
			rfio_perror("rfio_fopen64");
			exit(1);
		}
		fprintf(stdout,"OK\n");
	} else {
		fprintf(stdout,"[RFIOV%d-%dbits-T%02d] Opening %s in 32bits-read-mode... ", v3, rfio64, ++t, argv[Coptind]);
		if ((fd = rfio_fopen(argv[Coptind],"r")) == NULL) {
			fprintf(stdout,"NOT OK\n");
			fflush(stdout);
			rfio_perror("rfio_fopen");
			exit(1);
		}
		fprintf(stdout,"OK\n");
	}

	if ((bigsize + (v3 == 3 ? 100 : 110)) <= INT_MAX) {
		fprintf(stdout,"[RFIOV%d-%dbits-T%02d] Seek at %s bytes with fseek... ", v3, rfio64, ++t, u64tostr((u_signed64) (bigsize + (v3 == 3 ? 100 : 110)), tmpbuf, 0));

		if (rfio_fseek(fd,(long int) (bigsize + (v3 == 3 ? 100 : 110)),SEEK_SET) < 0) {
			fprintf(stdout,"NOT OK\n");
			fflush(stdout);
			if (rfio_ferror(fd)) {
				rfio_perror("rfio_fseek");
			} else {
				fprintf(stderr,"rfio_fseek error !\n");
			}
			exit(1);
		}
		fprintf(stdout,"OK\n");
	}
	fprintf(stdout,"[RFIOV%d-%dbits-T%02d] Seek at %s bytes with fseeko64... ", v3, rfio64, ++t, u64tostr((u_signed64) (bigsize + (v3 == 3 ? 100 : 110)), tmpbuf, 0));
	
	if (rfio_fseeko64(fd,(off64_t) (bigsize + (v3 == 3 ? 100 : 110)),SEEK_SET) < 0) {
		fprintf(stdout,"NOT OK\n");
		fflush(stdout);
		if (rfio_ferror(fd)) {
			rfio_perror("rfio_fseeko64");
		} else {
			fprintf(stderr,"rfio_fseeko64 error !\n");
		}
		exit(1);
	}
	fprintf(stdout,"OK\n");
	if (v3 == 2) {
		if ((bigsize+110) <= INT_MAX) {
			/* Get current offset with ftell */
			fprintf(stdout,"[RFIOV%d-%dbits-T%02d] Getting current offset... ", v3, rfio64, ++t);
			offset = rfio_ftell(fd);
			fprintf(stdout,"Got %d, should be %s...", (int) offset, u64tostr((u_signed64) (bigsize+110), tmpbuf, 0));
			if (offset != (bigsize+110)) {
				fprintf(stdout,"NOT OK\n");
				fflush(stdout);
				exit(1);
			}
			fprintf(stdout,"OK\n");
		}
	}
	/* Get current offset with ftello64 - Converted to lseek64(SEEK_CUR) - this is NOT supported in V3 */
	if (v3 == 2) {
		fprintf(stdout,"[RFIOV%d-%dbits-T%02d] Getting current offset with rfio_ftello64... ", v3, rfio64, ++t);
		offset64 = rfio_ftello64(fd);
		if (offset64 < 0) {
			if (rfio_ferror(fd)) {
				rfio_perror("rfio_ftello64");
			} else {
				fprintf(stderr,"rfio_ftello64 error !\n");
			}
		} else {
			char tmpbuf1[21], tmpbuf2[21];
			fprintf(stdout,"Got %s, should be %s...", u64tostr((u_signed64) offset64, tmpbuf1, 0), u64tostr((u_signed64) (bigsize+110), tmpbuf2, 0));
		}
		if (offset64 != bigsize+110) {
			fprintf(stdout,"NOT OK\n");
			fflush(stdout);
			exit(1);
		}
		fprintf(stdout,"OK\n");
	}

	fprintf(stdout,"[RFIOV%d-%dbits-T%02d] Read byte per byte up to eof using rfio_fread... ", v3, rfio64, ++t);
	i = 0;
	while (1) {
		char thischar;

		rfio_fread(&thischar,sizeof(thischar),1,fd);
		if (rfio_feof(fd)) {
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
	if (rfio_fclose(fd) != 0) {
		fprintf(stdout,"NOT OK\n");
		fflush(stdout);
		rfio_perror("rfio_fclose");
		exit(1);
	}
	fprintf(stdout,"OK\n");

	/* Open it in readmode */
	if (rfio64 == 64) {
		fprintf(stdout,"[RFIOV%d-%dbits-T%02d] Opening %s in 64bits-read-mode... ", v3, rfio64, ++t, argv[Coptind]);
		if ((fd = rfio_fopen64(argv[Coptind],"r")) == NULL) {
			fprintf(stdout,"NOT OK\n");
			fflush(stdout);
			rfio_perror("rfio_fopen64");
			exit(1);
		}
		fprintf(stdout,"OK\n");
	} else {
		fprintf(stdout,"[RFIOV%d-%dbits-T%02d] Opening %s in 32bits-read-mode... ", v3, rfio64, ++t, argv[Coptind]);
		if ((fd = rfio_fopen(argv[Coptind],"r")) == NULL) {
			fprintf(stdout,"NOT OK\n");
			fflush(stdout);
			rfio_perror("rfio_fopen");
			exit(1);
		}
		fprintf(stdout,"OK\n");
	}

	if ((bigsize + (v3 == 3 ? 100 : 110)) <= INT_MAX) {
		fprintf(stdout,"[RFIOV%d-%dbits-T%02d] Seek at %s bytes with fseek... ", v3, rfio64, ++t, u64tostr((u_signed64) (bigsize + (v3 == 3 ? 100 : 110)), tmpbuf, 0));

		if (rfio_fseek(fd,(bigsize + (v3 == 3 ? 100 : 110)),SEEK_SET) < 0) {
			fprintf(stdout,"NOT OK\n");
			fflush(stdout);
			if (rfio_ferror(fd)) {
				rfio_perror("rfio_fseek");
			} else {
				fprintf(stderr,"rfio_fseek error !\n");
			}
			exit(1);
		}
		fprintf(stdout,"OK\n");
	}
	fprintf(stdout,"[RFIOV%d-%dbits-T%02d] Seek at %s bytes with fseeko64... ", v3, rfio64, ++t, u64tostr((u_signed64) (bigsize + (v3 == 3 ? 100 : 110)), tmpbuf, 0));
	
	if (rfio_fseeko64(fd,(bigsize + (v3 == 3 ? 100 : 110)),SEEK_SET) < 0) {
		fprintf(stdout,"NOT OK\n");
		fflush(stdout);
		if (rfio_ferror(fd)) {
			rfio_perror("rfio_fseeko64");
		} else {
			fprintf(stderr,"rfio_fseeko64 error !\n");
		}
		exit(1);
	}
	fprintf(stdout,"OK\n");
	
	if (v3 == 2) {
		if (bigsize <= (INT_MAX-110)) {
			/* Get current offset with ftell */
			fprintf(stdout,"[RFIOV%d-%dbits-T%02d] Getting current offset... ", v3, rfio64, ++t);
			offset = rfio_ftell(fd);
			fprintf(stdout,"Got %d, should be %s...", (int) offset, u64tostr((bigsize + 110),tmpbuf,0));
			if (offset != (bigsize + 110)) {
				fprintf(stdout,"NOT OK\n");
				fflush(stdout);
				exit(1);
			}
			fprintf(stdout,"OK\n");
		}
		/* Get current offset with ftello64 - Converted to lseek64(SEEK_CUR) - this is NOT supported in V3 */
		if (v3 == 2) {
			fprintf(stdout,"[RFIOV%d-%dbits-T%02d] Getting current offset with rfio_ftello64... ", v3, rfio64, ++t);
			offset64 = rfio_ftello64(fd);
			if (offset64 < 0) {
				if (rfio_ferror(fd)) {
					rfio_perror("rfio_ftello64");
				} else {
					fprintf(stderr,"rfio_ftello64 error !\n");
				}
			} else {
				char tmpbuf1[21], tmpbuf2[21];
				fprintf(stdout,"Got %s, should be %s...", u64tostr((u_signed64) offset64, tmpbuf1, 0), u64tostr((u_signed64) (bigsize+110), tmpbuf2, 0));
			}
			if (offset64 != bigsize+110) {
				fprintf(stdout,"NOT OK\n");
				fflush(stdout);
				exit(1);
			}
			fprintf(stdout,"OK\n");
		}
	}

	fprintf(stdout,"[RFIOV%d-%dbits-T%02d] Read byte per byte up to eof using rfio_getc... ", v3, rfio64, ++t);
	i = 0;
	while (1) {
		char thischar;

		thischar = rfio_getc(fd);
		if (rfio_feof(fd)) {
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
	if (rfio_fclose(fd) != 0) {
		fprintf(stdout,"NOT OK\n");
		fflush(stdout);
		rfio_perror("rfio_fclose");
		exit(1);
	}
	fprintf(stdout,"OK\n");

	/* Open it in readmode */
	if (rfio64 == 64) {
		fprintf(stdout,"[RFIOV%d-%dbits-T%02d] Opening %s in 64bits-read-mode... ", v3, rfio64, ++t, argv[Coptind]);
		if ((fd = rfio_fopen64(argv[Coptind],"r")) == NULL) {
			fprintf(stdout,"NOT OK\n");
			fflush(stdout);
			rfio_perror("rfio_fopen64");
			exit(1);
		}
		fprintf(stdout,"OK\n");
	} else {
		fprintf(stdout,"[RFIOV%d-%dbits-T%02d] Opening %s in 32bits-read-mode... ", v3, rfio64, ++t, argv[Coptind]);
		if ((fd = rfio_fopen(argv[Coptind],"r")) == NULL) {
			fprintf(stdout,"NOT OK\n");
			fflush(stdout);
			rfio_perror("rfio_fopen");
			exit(1);
		}
		fprintf(stdout,"OK\n");
	}

	fprintf(stdout,"[RFIOV%d-%dbits-T%02d] Read chunks of 1M up to eof using rfio_fread... ", v3, rfio64, ++t);
	fflush(stdout);
	{
		u_signed64 totalfread = 0;
		while (1) {
			char dummy[1024 * 1024];
			int freadrc;

			freadrc = rfio_fread(dummy,sizeof(dummy),1,fd);
			if (rfio_feof(fd)) {
				fprintf(stdout," EOF\n");
				break;
			}
			if (freadrc < 0) {
				fprintf(stdout,"NOT OK\n");
				fflush(stdout);
				rfio_perror("rfio_fread");
				exit(1);
				break;
			}
			totalfread += sizeof(dummy);
			fprintf(stdout,"\n... %s bytes",  u64tostr((u_signed64) totalfread, tmpbuf, 0));
			fflush(stdout);
		}
	}
	fprintf(stdout,"OK\n");

	/* Close file */
	fprintf(stdout,"[RFIOV%d-%dbits-T%02d] Closing %s... ", v3, rfio64, ++t, argv[Coptind]);
	if (rfio_fclose(fd) != 0) {
		fprintf(stdout,"NOT OK\n");
		fflush(stdout);
		rfio_perror("rfio_fclose");
		exit(1);
	}
	fprintf(stdout,"OK\n");

	/* Unlink file */
	fprintf(stdout,"[RFIOV%d-%dbits-%d] Unlink %s... ", v3, rfio64, ++t, argv[Coptind]);
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
