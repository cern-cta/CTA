/*
 * Copyright (C) 1990-1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: tpdump.c,v $ $Revision: 1.12 $ $Date: 2000/01/09 17:42:36 $ CERN IT-PDP/DM Jean-Philippe Baud";
#endif /* not lint */

/*	tpdump - analyse the content of a tape */
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <fcntl.h>
#include <signal.h>
#if defined(ADSTAR)
#include <sys/Atape.h>
#endif
#include "Ctape.h"
#include "Ctape_api.h"
#include "serrno.h"
extern	int	optind;
extern	char	*optarg;
#if !defined(linux)
extern	char	*sys_errlist[];
#endif
int Ctape_kill_needed;
int code;
char *dvrname;
int infd = -1;
char infil[CA_MAXPATHLEN+1];
int irec;
int maxbyte = -1;
int reserve_done;

main(argc, argv)
int	argc;
char	**argv;
{
	static char aden[CA_MAXDENLEN+1] = "";
	int avg_block_length;
	int blksiz = -1;
	char *buffer;
	int c;
	void cleanup();
	static char codes[4][7] = {"", "ASCII", "", "EBCDIC"};
	int count;
	int den;
	int density;
	int dev1tm = 0;	/* by default 2 tapemarks are written at EOI */
	char devtype[CA_MAXDVTLEN+1];
	static char dgn[CA_MAXDGNLEN+1] = "";
	struct dgn_rsv dgn_rsv;
	char *dp;
	char drive[CA_MAXUNMLEN+1];
	char driver_name[7];
	int errcat = 0;
	int errflg = 0;
	int fromblock = -1;
	float gap;
	int goodrec;
	int ignoreeoi = 0;
	char label[81];
	static char lbltype[4] = "";
	int lcode;
	int max_block_length;
	int maxfile = -1;
	int min_block_length;
	char *msgaddr;
	int nbytes;
	int nerr;
	int nfile;
	char *p;
	int perc;
	int qbov, qlab;
	u_signed64 sum_block_length;
	float tape_used;
	int toblock = -1;
	static char vid[CA_MAXVIDLEN+1] = "";
	static char vsn[CA_MAXVIDLEN+1] = "";

	while ((c = getopt (argc, argv, "B:b:C:d:E:F:g:N:S:T:V:v:")) != EOF) {
		switch (c) {
		case 'B':
			if (maxbyte < 0) {
				maxbyte = strtol (optarg, &dp, 10);
				if (*dp != '\0') {
					fprintf (stderr, TP006, "-B");
					errflg++;
				}
			} else {
				fprintf (stderr, TP018, "-B");
				errflg++;
			}
			break;
		case 'b':
			if (blksiz < 0) {
				blksiz = strtol (optarg, &dp, 10);
				if (*dp != '\0') {
					fprintf (stderr, TP006, "-b");
					errflg++;
				}
			} else {
				fprintf (stderr, TP018, "-b");
				errflg++;
			}
			break;
		case 'C':
			if (code == 0) {
				if (strcmp (optarg, "ascii") == 0)
					code = AL;
				else if( strcmp (optarg, "ebcdic") == 0) {
					code = SL;
				} else {
					fprintf (stderr, TP006, "-C");
					errflg++;
				}
			} else {
				fprintf (stderr, TP018, "-C");
				errflg++;
			}
			break;
		case 'd':
			if (aden[0]) {
				if (strlen (optarg) <= CA_MAXDENLEN) {
					strcpy (aden, optarg);
				} else {
					fprintf (stderr, TP006, "-d");
					errflg++;
				}
			} else {
				fprintf (stderr, TP018, "-d");
				errflg++;
			}
			break;
		case 'E':
			if (strcmp (optarg, "ignoreeoi") == 0)
				ignoreeoi = 'i';
			else {
				fprintf (stderr, TP006, "-E");
				errflg++;
			}
			break;
		case 'F':
			if (maxfile < 0) {
				maxfile = strtol (optarg, &dp, 10);
				if (*dp != '\0') {
					fprintf (stderr, TP006, "-F");
					errflg++;
				}
			} else {
				fprintf (stderr, TP018, "-F");
				errflg++;
			}
			break;
		case 'g':
			if (dgn[0]) {
				if (strlen (optarg) <= CA_MAXDGNLEN) {
					strcpy (dgn, optarg);
				} else {
					fprintf (stderr, TP006, "-g");
					errflg++;
				}
			} else {
				fprintf (stderr, TP018, "-g");
				errflg++;
			}
			break;
		case 'N':
			if (fromblock < 0) {
				if (p = strchr (optarg, ',')) {
					*p++ = '\0';
					fromblock = strtol (optarg, &dp, 10);
					if (*dp != '\0') {
						fprintf (stderr, TP006, "-N");
						errflg++;
					}
				} else {
					p = optarg;
					fromblock = 1;
				}
				toblock = strtol (p, &dp, 10);
				if (*dp != '\0') {
					fprintf (stderr, TP006, "-N");
					errflg++;
				}
			}
			break;
		case 'S':
			break;
		case 'V':
			if (vid[0] == '\0') {
				if (strlen(optarg) <= CA_MAXVIDLEN) {
					strcpy (vid, optarg);
				} else {
					fprintf (stderr, TP006, "-V");
					errflg++;
				}
			} else {
				fprintf (stderr, TP018, "-V");
				errflg++;
			}
			break;
		case 'v':
			if (vsn[0] == '\0') {
				if (strlen(optarg) <= CA_MAXVIDLEN) {
					strcpy (vsn, optarg);
				} else {
					fprintf (stderr, TP006, "-v");
					errflg++;
				}
			} else {
				fprintf (stderr, TP018, "-v");
				errflg++;
			}
			break;
		case '?':
			errflg++;
			break;
		}
	}
	if (*vid == '\0' && *vsn == '\0') {
		fprintf (stderr, TP031);
		errflg++;
	}
	if (errflg) {
		usage (argv[0]);
		exit_prog (USERR);
	}

	if (strcmp (dgn, "CT1") == 0) strcpy (dgn, "CART");

#if ! defined(_WIN32)
	signal (SIGHUP, cleanup);
#endif
	signal (SIGINT, cleanup);
#if ! defined(_WIN32)
	signal (SIGQUIT, cleanup);
#endif
	signal (SIGTERM, cleanup);

	/* Get defaults from TMS (if installed) */

#if TMS
	if (tmscheck (vid, vsn, dgn, aden, lbltype, WRITE_DISABLE, NULL))
		exit_prog (USERR);
#else
	if (*vsn == '\0')
		strcpy (vsn, vid);
	if (*dgn == '\0')
		strcpy (dgn, DEFDGN);
	if (strcmp (dgn, "TAPE") == 0 && *aden == '\0')
		strcpy (aden, "6250");
#endif
	strcpy (infil, tempnam (NULL, "tp"));

#if defined(_AIX) && defined(_IBMR2)
	if (getdvrnam (infil, driver_name) < 0)
		strcpy (driver_name, "tape");
	dvrname = driver_name;
#endif

	/* reserve resources */

	strcpy (dgn_rsv.name, dgn);
	dgn_rsv.num = 1;
	count = 1;
	if (Ctape_reserve (count, &dgn_rsv))
		exit_prog ((serrno == EINVAL || serrno == ETIDG) ? USERR : SYERR);
	reserve_done = 1;

	/* mount and position the input tape */

	Ctape_kill_needed = 1;
	if (Ctape_mount (infil, vid, 0, dgn, NULL, NULL, WRITE_DISABLE,
	    NULL, "blp", 0))
		exit_prog ((serrno == EACCES || serrno == EINVAL) ? USERR : SYERR);
	if (Ctape_position (infil, TPPOSIT_FSEQ, 1, 1, 0, 0, 0, CHECK_FILE,
	    NULL, "U", 0, 0, 0, 0))
		exit_prog ((serrno == EINVAL) ? USERR : SYERR);
	Ctape_kill_needed = 0;
	if (Ctape_info (infil, NULL, NULL, aden, devtype, drive, NULL,
	    NULL, NULL, NULL))
		exit_prog (SYERR);

	printf (" DUMP - TAPE MOUNTED ON DRIVE %s\n", drive);

	/* Set default values */

	if (code == 0)
		code = SL;
	den = cvtden (aden);
	switch (den) {
	case D800:
		density = 800;
		gap = 0.6;
		break;
	case D1600:
		density = 1600;
		gap = 0.6;
		break;
	case D6250:
		density = 6250;
		gap = 0.3;
		break;
	case D38000:
		density = 38000;
		gap = 0.08;
		break;
	case D38KC:
		density = 38000;
		gap = 0.04;
		break;
	case D38KD:
	case D38KDC:
		density = 38000;
		gap = 0.02;
		break;
	case D8200:
	case D8200C:
	case D8500:
	case D8500C:
	case D2G:
	case D6G:
	case D10G:
	case D10GC:
	case D20G:
	case D20GC:
	case D25G:
	case D25GC:
	case D35G:
	case D35GC:
	case D50G:
	case D50GC:
	case DDS:
	case DDSC:
		gap = 0.0;
		break;
	default:
		fprintf (stderr, TP006, "-d");
		exit_prog (USERR);
	}

	if (blksiz < 0)
		if (strcmp (devtype, "SD3") == 0)
			blksiz = 262144;
		else
			blksiz = 65536;
#if defined(_AIX) && defined(RS6000PCTA)
	if (blksiz > 65535) blksiz = 65535;
#endif
	if (maxbyte < 0) maxbyte = 320;
	if (maxfile < 0)
		if ((den & 0xF) <= D38000 || (den & 0xF) == D38KD)
			maxfile = 0;
		else
			maxfile = 1;
	if (fromblock < 0) fromblock = 1;
	if (toblock < 0) toblock = 1;

	/* print headers */

	printf (" DUMP - PARAMETERS :\n");
	if (maxbyte == 0)
		printf (" DUMP - ALL BYTES\n");
	else
		printf (" DUMP - MAX. NB OF BYTES: %d\n", maxbyte);
	printf (" DUMP - FROM BLOCK: %d\n", fromblock);
	if (toblock == 0)
		printf (" DUMP - TO LAST BLOCK\n");
	else
		printf (" DUMP - TO BLOCK: %d\n", toblock);
	if (maxfile == 0)
		printf (" DUMP - ALL FILES\n");
	else
		printf (" DUMP - MAX. NB OF FILES: %d\n", maxfile);
	printf (" DUMP - %s INTERPRETATION\n", codes[code]);
	printf (" DUMP -\n");

	/* initialise flags and counters */

	if (strncmp (devtype, "DLT", 3) == 0 || strcmp (devtype, "3590") == 0 ||
	    strcmp (devtype, "SD3") == 0 || strcmp (devtype, "9840") == 0)
		dev1tm = 1;	/* one TM at EOI is enough */
	goodrec = 0;
	irec = 0;
	lcode = 0;
	max_block_length = 0;
	min_block_length = 0;
	nerr = 0;
	nfile = 0;
	qbov = 1;
	qlab = 0;
	sum_block_length = 0;
	tape_used = 0;
	buffer = malloc (blksiz);

	/* open path */

	if ((infd = open (infil, O_RDONLY)) < 0) {
		fprintf (stderr, " DUMP ! ERROR OPENING TAPE FILE: %s\n",
			sys_errlist[errno]);
		exit_prog (SYERR);
	}

	while (1) {
		nbytes = read (infd, buffer, blksiz);
		if (nbytes > 0) {		/* record found */
			irec++;
			if (strcmp (devtype, "SD3") == 0 || strcmp (devtype, "9840") == 0)
				tape_used = tape_used + (float) nbytes;
			else if (strncmp (devtype, "DLT", 3) == 0)
				tape_used = tape_used + ((float) ((nbytes + 4095) / 4096) * 4096.);
			else if (den == DDS || den == DDSC)
				tape_used = tape_used + (float) nbytes + 4.0;
			else if (den != D8200 && den != D8200C && den != D8500 && den != D8500C)
				tape_used = tape_used + ((float) nbytes / (float) density) + gap;
			else
				tape_used = tape_used + ((float) ((nbytes + 1023) / 1024) * 1024.);
			goodrec++;
			if (min_block_length == 0) min_block_length = nbytes;
			if (nbytes > max_block_length)
				max_block_length = nbytes;
			else if (nbytes < min_block_length)
				min_block_length = nbytes;
			sum_block_length += nbytes;

			if (qbov) {	/* beginning of tape */
				qbov = 0;
				if (nbytes == 80) {
					ebc_asc (buffer, label, 80);
					if (strncmp (label, "VOL1", 4) == 0)
						lcode = SL;
					else {
						asc_asc (buffer, label, 80);
						if (strncmp (label, "VOL1", 4) == 0)
							lcode = AL;
					}
				}
				if (lcode) {
					printf ("               %s HAS AN %s LABEL\n", vid, codes[lcode]);
					printf ("\n VOLUME LABEL :\n");
					printf (" VOLUME SERIAL NUMBER:       %.6s\n", label+4);
					printf (" OWNER IDENTIFIER:           %.14s\n", label+37);
					continue;
				} else {
					printf ("               %s IS UNLABELLED\n", vid);
				}

			} else if (irec < 6 && nbytes == 80) {
				if (lcode == SL)
					ebc_asc (buffer, label, 80);
				else
					asc_asc (buffer, label, 80);
				if (qlab = islabel (label)) continue;
				if (lcode == 0) {
					ebc_asc (buffer, label, 80);
					if (qlab = islabel (label)) continue;
				}
			}
			qlab = 0;
			if (irec >= fromblock &&
				(toblock == 0 || irec <= toblock)) {
				dumpblock (buffer, nbytes);
				printf (" BLOCK NUMBER %d  LENGTH = %d BYTES\n",
					irec, nbytes);
			}

		} else if (nbytes == 0) {	/* tape mark found */
#if defined(sun) || defined(linux)
			if (gettperror (infd, infil, &msgaddr) == ETBLANK) {
				if (dev1tm && !qbov && irec == 0) break;
				fflush (stdout);
				fprintf (stderr, " DUMP ! READ ERROR: %s (BLOCK # %d)\n",
					"Blank check", irec+1);
				break;
			}
#endif
			if (den <= D6250)
				tape_used += 6.;
			else if (den == D38000 || den == D38KC ||
				den == D38KD || den == D38KDC)
				tape_used += 0.1;
			else if (den == D8200 || den == D8200C)
				tape_used += (270. * 8192.);	/* long filemark */
			else if (den == D8500 || den == D8500C)
				tape_used += 48000.;
			else if (strncmp (devtype, "DLT", 3) == 0)
				tape_used += 4096.;
			else if (strcmp (devtype, "SD3") == 0)
				tape_used += 2048.;
			else if (strcmp (devtype, "9840") == 0)
				tape_used += 12345.;
			else if (den == DDS || den == DDSC)
				tape_used = tape_used + 4.0;
			if (qbov) {	/* beginning of tape */
				qbov = 0;
				printf ("               %s IS UNLABELLED\n", vid);
			}
			if (qlab) {
				printf (" ***** TAPE MARK READ *****      END OF LABEL GROUP\n");
				printf (" *********************************************************************************************************************\n");
				qlab = 0;
			} else {
				nfile++;
				printf (" ***** TAPE MARK READ *****      FILE %d CONTAINED %d BLOCKS.\n",
					nfile, irec);
				if (goodrec != 0) {
					avg_block_length = sum_block_length / goodrec;
					printf ("                                 MAXIMUM BLOCK LENGTH WAS %d BYTES\n",
						max_block_length);
					printf ("                                 MINIMUM BLOCK LENGTH WAS %d BYTES\n",
						min_block_length);
					printf ("                                 AVERAGE BLOCK LENGTH WAS %d BYTES\n",
						avg_block_length);
					printf (" *********************************************************************************************************************\n");
					if (maxfile != 0 && nfile >= maxfile) {
						printf (" DUMP - DUMPING PROGRAM COMPLETE.\n");
						exit_prog (0);
					}
				} else if (! ignoreeoi) {
					printf (" ***** DOUBLE TAPE MARK READ *****\n");
					break;
				}
			}
#if sgi
			close (infd);
			infd = open (infil, O_RDONLY);
#endif
			goodrec = 0;
			irec = 0;
			max_block_length = 0;
			min_block_length = 0;
			sum_block_length = 0;
		} else {
			if (errno == EIO)
				errcat = gettperror (infd, infil, &msgaddr);
			else
#if sgi || (__alpha && __osf__)
				if (errno == ENOSPC) {
					msgaddr = "Blank check";
					errcat = ETBLANK;
				} else
#endif
					msgaddr = (char *) sys_errlist[errno];
			if (errcat == ETBLANK && dev1tm && !qbov && irec == 0) break;
			irec++;
			fflush (stdout);
			fprintf (stderr, " DUMP ! READ ERROR: %s (BLOCK # %d)\n",
				msgaddr, irec);
			if (errcat == ETBLANK) break;
			if (++nerr >= 10) {
				fprintf (stderr, " DUMP ! TOO MANY READ ERRORS.\n");
				break;
			}
		}
	}
	if (irec) {
		if (goodrec) {
			avg_block_length = sum_block_length / goodrec;
			printf ("                                 MAXIMUM BLOCK LENGTH WAS %d BYTES\n",
				max_block_length);
			printf ("                                 MINIMUM BLOCK LENGTH WAS %d BYTES\n",
				min_block_length);
			printf ("                                 AVERAGE BLOCK LENGTH WAS %d BYTES\n",
				avg_block_length);
			printf (" *********************************************************************************************************************\n");
		}
		printf (" DUMP - DUMP ABORTED\n");
		exit_prog (USERR);
	}

	if (den <= D6250) {
		perc = (tape_used * 100) / (2400 * 12);
		printf ("\n ***** THE RECORDED DATA OCCUPIED ABOUT %d %%  OF A FULL 2400 FOOT TAPE *****\n",
			perc);
	} else if (den == D38000 || den == D38KC) {
		perc = (tape_used * 100) / (500 * 12);
		printf ("\n ***** THE RECORDED DATA OCCUPIED ABOUT %d %%  OF A STANDARD 3480 CARTRIDGE *****\n",
			perc);
	} else if (den == D38KD || den == D38KDC) {
		perc = (tape_used * 100) / (500 * 4 * 12);
		printf ("\n ***** THE RECORDED DATA OCCUPIED ABOUT %d %%  OF A DOUBLE LENGTH CARTRIDGE *****\n",
			perc);
	} else if (den == D8200 || den == D8200C) {
		perc = tape_used / 23000000;
		printf ("\n ***** THE RECORDED DATA OCCUPIED ABOUT %d %%  OF A STANDARD LENGTH (2.3GB) EXABYTE TAPE *****\n",
			perc);
	} else if (den == D8500 || den == D8500C) {
		perc = tape_used / 50000000;
		printf ("\n ***** THE RECORDED DATA OCCUPIED ABOUT %d %%  OF A STANDARD LENGTH (5GB) EXABYTE TAPE *****\n",
			perc);
	} else if (den == D2G) {
		perc = tape_used / 26000000;
		printf ("\n ***** THE RECORDED DATA OCCUPIED ABOUT %d %%  OF A CompacTapeIII (2.6GB) *****\n",
			perc);
	} else if (den == D6G) {
		perc = tape_used / 60000000;
		printf ("\n ***** THE RECORDED DATA OCCUPIED ABOUT %d %%  OF A CompacTapeIII (6GB) *****\n",
			perc);
	} else if (den == D10G || den == D10GC) {
	    if (strncmp (devtype, "DLT", 3) == 0) {
		perc = tape_used / 100000000;
		printf ("\n ***** THE RECORDED DATA OCCUPIED ABOUT %d %%  OF A CompacTapeIII (10GB) *****\n",
			perc);
	    } else if (strcmp (devtype, "SD3") == 0) {
		perc = tape_used / 100000000;
		printf ("\n ***** THE RECORDED DATA OCCUPIED ABOUT %d %%  OF A Redwood CARTRIDGE (10GB) *****\n",
			perc);
#if defined(_AIX) && defined(_IBMR2) && defined(ADSTAR)
	    } else if (strcmp (devtype, "3590") == 0) {
		unsigned int cpos;
		unsigned char sense[128];
		unsigned int tach;
		unsigned int wrap;

		if (ioctl (infd, SIOC_REQSENSE, sense) < 0) {
			fprintf (stderr, " DUMP ! UNABLE TO DETERMINE TAPE OCCUPANCY");
			exit_prog (SYERR);
		}
		tach = sense[25];
		wrap = sense[53];
		cpos = sense[53] * 235;
		if ((wrap % 2) == 0)
			cpos += (tach - 9);
		else
			cpos += (244 - tach);
		perc = (cpos * 100) / (235 * 8);
		printf ("\n ***** THE RECORDED DATA OCCUPIED ABOUT %d %%  OF A STANDARD 3590 CARTRIDGE *****\n",
			perc);
#endif
	    }
	} else if (den == D20G || den == D20GC) {
	    if (strncmp (devtype, "DLT", 3) == 0) {
		perc = tape_used / 200000000;
		printf ("\n ***** THE RECORDED DATA OCCUPIED ABOUT %d %%  OF A CompacTapeIV (20GB) *****\n",
			perc);
	    } else if (strcmp (devtype, "9840") == 0) {
		perc = tape_used / 200000000;
		printf ("\n ***** THE RECORDED DATA OCCUPIED ABOUT %d %%  OF A STK 9840 CARTRIDGE (20GB) *****\n",
			perc);
	    }
	} else if (den == D25G || den == D25GC) {
		perc = tape_used / 250000000;
		printf ("\n ***** THE RECORDED DATA OCCUPIED ABOUT %d %%  OF A Redwood CARTRIDGE (25GB) *****\n",
			perc);
	} else if (den == D35G || den == D35GC) {
		perc = tape_used / 350000000;
		printf ("\n ***** THE RECORDED DATA OCCUPIED ABOUT %d %%  OF A CompacTapeIV (35GB) *****\n",
			perc);
	} else if (den == D50G || den == D50GC) {
		perc = tape_used / 500000000;
		printf ("\n ***** THE RECORDED DATA OCCUPIED ABOUT %d %%  OF A Redwood CARTRIDGE (50GB) *****\n",
			perc);
	} else if (den == DDS || den == DDSC) {
		perc = tape_used / 40000000;
		printf ("\n ***** THE RECORDED DATA OCCUPIED ABOUT %d %%  OF A DDS2 CARTRIDGE (4GB) *****\n",
			perc);
	}
	printf (" DUMP - DUMPING PROGRAM COMPLETE.\n");
	exit_prog (0);
}

dumpblock (buffer, nbytes)
char *buffer;
int nbytes;
{
	char bufftr[33];
	int i, k, l;
	int iad;
	char *p;

	printf (" BLOCK NUMBER %d\n", irec);
	for (iad = 0, p = buffer; iad < nbytes; iad += 32) {
		if (maxbyte != 0 && iad >= maxbyte) break;
		l = iad + 31;
		if (l >= nbytes) l = nbytes - 1;
		k = l - iad + 1;
		if (code == SL)
			ebc_asc (p, bufftr, k);
		else
			asc_asc (p, bufftr, k);
		bufftr[k] = '\0';
		printf (" %.8x   ", iad);
		for (i = 0; i < k; i++) {
			if (i % 8 == 0) putchar (' ');
			printf ("%.2x", *(p++) & 0xff);
		}
		for (i = k; i < 32; i++)
			putchar (' ');
		printf ("    *%s*\n", bufftr);
	}
}

ebc_asc(p, q, n)
char *p;
char *q;
int n;
{
	int i;
	static char e2atab[256] = {

	'.','.','.','.','.','.','.','.','.','.','.','.','.','.','.','.',

	'.','.','.','.','.','.','.','.','.','.','.','.','.','.','.','.',

	'.','.','.','.','.','.','.','.','.','.','.','.','.','.','.','.',

	'.','.','.','.','.','.','.','.','.','.','.','.','.','.','.','.',

	' ','.','.','.','.','.','.','.','.','.','.','.','<','(','+','|',

	'&','.','.','.','.','.','.','.','.','.','!','$','*',')',';','^',

	'-','/','.','.','.','.','.','.','.','.','.',',','%','_','>','?',

	'.','.','.','.','.','.','.','.','.','`',':','#','@','\'','=','"',

	'.','a','b','c','d','e','f','g','h','i','.','.','.','.','.','.',

	'.','j','k','l','m','n','o','p','q','r','.','.','.','.','.','.',

	'.','~','s','t','u','v','w','x','y','z','.','.','.','[','.','.',

	'.','.','.','.','.','.','.','.','.','.','.','.','.',']','.','.',

	'{','A','B','C','D','E','F','G','H','I','.','.','.','.','.','.',

	'}','J','K','L','M','N','O','P','Q','R','.','.','.','.','.','.',

	'\\','.','S','T','U','V','W','X','Y','Z','.','.','.','.','.','.',

	'0','1','2','3','4','5','6','7','8','9','.','.','.','.','.','.',
	};

	for (i = 0; i < n; i++)
		*q++ = e2atab[*p++ & 0xff];
}

asc_asc(p, q, n)
char *p;
char *q;
int n;
{
	int i;
	static char a2atab[256] = {

	'.','.','.','.','.','.','.','.','.','.','.','.','.','.','.','.',

	'.','.','.','.','.','.','.','.','.','.','.','.','.','.','.','.',

	' ','!','"','#','$','%','&','\'','(',')','*','+',',','-','.','/',

	'0','1','2','3','4','5','6','7','8','9',':',';','<','=','>','?',

	'@','A','B','C','D','E','F','G','H','I','J','K','L','M','N','O',

	'P','Q','R','S','T','U','V','W','X','Y','Z','[','\\',']','^','_',

	'`','a','b','c','d','e','f','g','h','i','j','k','l','m','n','o',

	'p','q','r','s','t','u','v','w','x','y','z','{','|','}','~','.',

	'.','.','.','.','.','.','.','.','.','.','.','.','.','.','.','.',

	'.','.','.','.','.','.','.','.','.','.','.','.','.','.','.','.',

	'.','.','.','.','.','.','.','.','.','.','.','.','.','.','.','.',

	'.','.','.','.','.','.','.','.','.','.','.','.','.','.','.','.',

	'.','.','.','.','.','.','.','.','.','.','.','.','.','.','.','.',

	'.','.','.','.','.','.','.','.','.','.','.','.','.','.','.','.',

	'.','.','.','.','.','.','.','.','.','.','.','.','.','.','.','.',

	'.','.','.','.','.','.','.','.','.','.','.','.','.','.','.','.',
	};

	for (i = 0; i < n; i++)
		*q++ = a2atab[*p++ & 0xff];
}

islabel(label)
char *label;
{
	if (strncmp (label, "HDR1", 4) == 0) {
		printf ("\n HEADER LABEL 1:\n");
		printlabel1 (label);
		return (1);
	} else if (strncmp (label, "HDR2", 4) == 0) {
		printf ("\n HEADER LABEL 2:\n");
		printlabel2 (label);
		return (1);
	} else if (strncmp (label, "HDR3", 4) == 0) {
		printf ("\n HEADER LABEL 3:             %.76s\n", label+4);
		return (1);
	} else if (strncmp (label, "HDR4", 4) == 0) {
		printf ("\n HEADER LABEL 4:             %.76s\n", label+4);
		return (1);
	} else if (strncmp (label, "EOF1", 4) == 0) {
		printf ("\n TRAILER LABEL 1:\n");
		printlabel1 (label);
		return (1);
	} else if (strncmp (label, "EOF2", 4) == 0) {
		printf ("\n TRAILER LABEL 2:\n");
		printlabel2 (label);
		return (1);
	} else if (strncmp (label, "EOF3", 4) == 0) {
		printf ("\n TRAILER LABEL 3:            %.76s\n", label+4);
		return (1);
	} else if (strncmp (label, "EOF4", 4) == 0) {
		printf ("\n TRAILER LABEL 4:            %.76s\n", label+4);
		return (1);
	} else if (strncmp (label, "EOV1", 4) == 0) {
		printf ("\n END OF VOLUME LABEL 1:\n");
		printlabel1 (label);
		return (1);
	} else if (strncmp (label, "EOV2", 4) == 0) {
		printf ("\n END OF VOLUME LABEL 2:\n");
		printlabel2 (label);
		return (1);
	}
	return (0);
}

printlabel1 (label)
char *label;
{
	printf (" FILE ID:                    %.17s\n", label + 4);
	printf (" VOLUME SEQNO:               %.4s\n", label + 27);
	printf (" FILE SEQNO:                 %.4s\n", label + 31);
	printf (" CREATION DATE:              %.6s\n", label + 41);
	printf (" EXPIRATION DATE:            %.6s\n", label + 47);
	printf (" BLOCK COUNT:                %.6s\n", label + 54);
}

printlabel2 (label)
char *label;
{
	printf (" RECORD FORMAT:              %.1s\n", label + 4);
	printf (" BLOCK LENGTH:               %.5s\n", label + 5);
	printf (" RECORD LENGTH:              %.5s\n", label + 10);
	printf (" DENSITY:                    %.1s\n", label + 15);
	printf (" DATA RECORDING:             %.1s\n", label + 34);
	printf (" BLOCKING ATTRIBUTE:         %.1s\n", label + 38);
}

void
cleanup(sig)
int sig;
{
	signal (sig, SIG_IGN);
	if (Ctape_kill_needed)
		(void) Ctape_kill (infil);
	exit_prog (USERR);
}

exit_prog(exit_code)
int exit_code;
{
	if (infd >= 0)
		close (infd);
	if (reserve_done)
		(void) Ctape_rls (NULL, TPRLS_ALL);
	exit (exit_code);
}

usage(cmd)
char *cmd;
{
	fprintf (stderr, "usage: %s ", cmd);
	fprintf (stderr, "%s%s%s",
	    "[-B maxbyte] [-b max_block_size] [-C {ebcdic|ascii}]\n",
	    "[-d density] [-E ignoreeoi] [-F maxfile] [-g device_group_name]\n",
	    "[-N [fromblock,]toblock] [-v vsn] -V vid\n");
}
