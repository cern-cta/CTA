/*
 * Copyright (C) 1990-2002 by CERN/IT/PDP/DM
 * All rights reserved
 */


/*	Ctape_dmpfil - analyse the content of a tape file */
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <errno.h>
#include <unistd.h>
#if defined(ADSTAR)
#include <sys/Atape.h>
#endif
#include "h/Ctape.h"
#include "h/Ctape_api.h"
#include "h/serrno.h"
#include "h/u64subr.h"
void (*Ctape_dmpmsg) (int, const char *, ...) = NULL;
static char *buffer;
static char codes[4][7] = {"", "ASCII", "", "EBCDIC"};
static struct {
	char vid[CA_MAXVIDLEN+1];
	char devtype[CA_MAXDVTLEN+1];
	int maxblksize;
	int fromblock;
	int toblock;
	int maxbyte;
	int fromfile;
	int maxfile;
	int code;
	int flags;
} dmpparm;
static int infd = -1;
static int irec;

int Ctape_dmpinit(char *path,
                  char *vid,
                  char *aden,
                  char *drive,
                  char *devtype,
                  int maxblksize,
                  int fromblock,
                  int toblock,
                  int maxbyte,
                  int fromfile,
                  int maxfile,
                  int code,
                  int flags)
{
	int errflg = 0;

	if (! vid || *vid == '\0') {
		Ctape_dmpmsg (MSG_ERR, TP031);
		errflg++;
	}
	if (code != DMP_ASC && code != DMP_EBC) {
		Ctape_dmpmsg (MSG_ERR, TP006, "code");
		errflg++;
	}
	if (flags != 0 && flags != IGNOREEOI) {
		Ctape_dmpmsg (MSG_ERR, TP006, "flags");
		errflg++;
	}
	if (errflg) {
		serrno = EINVAL;
		return (-1);
	}

	Ctape_dmpmsg (MSG_OUT, " DUMP - TAPE MOUNTED ON DRIVE %s\n", drive);

	if (maxblksize < 0)
		maxblksize = DMP_DEFAULTBLOCKSIZE;
	if (maxbyte < 0) maxbyte = 320;
	if (maxfile < 0) {
			maxfile = 1;
        }
	if (fromblock < 0) fromblock = 1;
	if (toblock < 0) toblock = 1;

	buffer = malloc (maxblksize);

	/* print headers */

	Ctape_dmpmsg (MSG_OUT, " DUMP - PARAMETERS :\n");
	if (maxbyte == 0)
		Ctape_dmpmsg (MSG_OUT, " DUMP - ALL BYTES\n");
	else
		Ctape_dmpmsg (MSG_OUT, " DUMP - MAX. NB OF BYTES: %d\n", maxbyte);
	Ctape_dmpmsg (MSG_OUT, " DUMP - FROM BLOCK: %d\n", fromblock);
	if (toblock == 0)
		Ctape_dmpmsg (MSG_OUT, " DUMP - TO LAST BLOCK\n");
	else
		Ctape_dmpmsg (MSG_OUT, " DUMP - TO BLOCK: %d\n", toblock);
	if (fromfile > 0)
		Ctape_dmpmsg (MSG_OUT, " DUMP - FROM FILE %d\n", fromfile);
	if (maxfile == 0)
		Ctape_dmpmsg (MSG_OUT, " DUMP - ALL FILES\n");
	else
		Ctape_dmpmsg (MSG_OUT, " DUMP - MAX. NB OF FILES: %d\n", maxfile);
	Ctape_dmpmsg (MSG_OUT, " DUMP - %s INTERPRETATION\n", codes[code]);
	Ctape_dmpmsg (MSG_OUT, " DUMP -\n");

	/* open path */

	if (infd < 0 && (infd = open (path, O_RDONLY)) < 0) {
		Ctape_dmpmsg (MSG_ERR, " DUMP ! ERROR OPENING TAPE FILE: %s\n",
			strerror(errno));
		serrno = errno;
		return (-1);
	}

	(void) clear_compression_stats (infd, path, devtype);

	/* save dump parameters */

	strcpy (dmpparm.vid, vid);
	strcpy (dmpparm.devtype, devtype);
	dmpparm.maxblksize = maxblksize;
	dmpparm.fromblock = fromblock;
	dmpparm.toblock = toblock;
	dmpparm.maxbyte = maxbyte;
	dmpparm.fromfile = fromfile;
	dmpparm.maxfile = maxfile;
	dmpparm.code = code;
	dmpparm.flags = flags;
	return (0);
}

int ebc_asc(char *p,
            char *q,
            int n)
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
        return (0);
}

int asc_asc(char *p,
            char *q,
            int n)
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
        return (0);
}


int dumpblock (char *buffer,
               int nbytes)
{
	char bufftr[33];
	int i, k, l;
	int iad;
	char *p;

	Ctape_dmpmsg (MSG_OUT, " BLOCK NUMBER %d\n", irec);
	for (iad = 0, p = buffer; iad < nbytes; iad += 32) {
		if (dmpparm.maxbyte != 0 && iad >= dmpparm.maxbyte) break;
		l = iad + 31;
		if (l >= nbytes) l = nbytes - 1;
		k = l - iad + 1;
		if (dmpparm.code == DMP_EBC)
			ebc_asc (p, bufftr, k);
		else
			asc_asc (p, bufftr, k);
		bufftr[k] = '\0';
		Ctape_dmpmsg (MSG_OUT, " %.8x   ", iad);
		for (i = 0; i < k; i++) {
			if (i % 8 == 0) putchar (' ');
			Ctape_dmpmsg (MSG_OUT, "%.2x", *(p++) & 0xff);
		}
		for (i = k; i < 32; i++)
			putchar (' ');
		Ctape_dmpmsg (MSG_OUT, "    *%s*\n", bufftr);
	}
        return (0);
}

int printlabel1 (char *label)
{
	Ctape_dmpmsg (MSG_OUT, " FILE ID:                    %.17s\n", label + 4);
	Ctape_dmpmsg (MSG_OUT, " VOLUME SEQNO:               %.4s\n", label + 27);
	Ctape_dmpmsg (MSG_OUT, " FILE SEQNO:                 %.4s\n", label + 31);
	Ctape_dmpmsg (MSG_OUT, " CREATION DATE:              %.6s\n", label + 41);
	Ctape_dmpmsg (MSG_OUT, " EXPIRATION DATE:            %.6s\n", label + 47);
	Ctape_dmpmsg (MSG_OUT, " BLOCK COUNT:                %.6s\n", label + 54);

        return (0);
}

int printlabel2 (char *label)
{
	Ctape_dmpmsg (MSG_OUT, " RECORD FORMAT:              %.1s\n", label + 4);
	Ctape_dmpmsg (MSG_OUT, " BLOCK LENGTH:               %.5s\n", label + 5);
	Ctape_dmpmsg (MSG_OUT, " RECORD LENGTH:              %.5s\n", label + 10);
	Ctape_dmpmsg (MSG_OUT, " DENSITY:                    %.1s\n", label + 15);
	Ctape_dmpmsg (MSG_OUT, " DATA RECORDING:             %.1s\n", label + 34);
	Ctape_dmpmsg (MSG_OUT, " BLOCKING ATTRIBUTE:         %.1s\n", label + 38);

        return (0);
}

int printulabel1 (char *label)
{
	Ctape_dmpmsg (MSG_OUT, " FILE SEQNO:                 %.10s\n", label + 4);
	Ctape_dmpmsg (MSG_OUT, " BLOCK LENGTH:               %.10s\n", label + 14);
	Ctape_dmpmsg (MSG_OUT, " RECORD LENGTH:              %.10s\n", label + 24);
	Ctape_dmpmsg (MSG_OUT, " SITE:                       %.8s\n", label + 34);
	Ctape_dmpmsg (MSG_OUT, " TAPE MOVER HOSTNAME:        %.10s\n", label + 42);
	Ctape_dmpmsg (MSG_OUT, " DRIVE MANUFACTURER:         %.8s\n", label + 52);
	Ctape_dmpmsg (MSG_OUT, " DRIVE MODEL:                %.8s\n", label + 60);
	Ctape_dmpmsg (MSG_OUT, " DRIVE SERIAL NUMBER:        %.12s\n", label + 68);

        return (0);
}

int islabel(char *label)
{
	if (strncmp (label, "HDR1", 4) == 0) {
		Ctape_dmpmsg (MSG_OUT, "\n HEADER LABEL 1:\n");
		printlabel1 (label);
		return (1);
	} else if (strncmp (label, "HDR2", 4) == 0) {
		Ctape_dmpmsg (MSG_OUT, "\n HEADER LABEL 2:\n");
		printlabel2 (label);
		return (2);
	} else if (strncmp (label, "HDR3", 4) == 0) {
		Ctape_dmpmsg (MSG_OUT, "\n HEADER LABEL 3:             %.76s\n", label+4);
		return (3);
	} else if (strncmp (label, "HDR4", 4) == 0) {
		Ctape_dmpmsg (MSG_OUT, "\n HEADER LABEL 4:             %.76s\n", label+4);
		return (4);
	} else if (strncmp (label, "UHL1", 4) == 0) {
		Ctape_dmpmsg (MSG_OUT, "\n USER HEADER LABEL 1:\n");
		printulabel1 (label);
		return (5);
	} else if (strncmp (label, "EOF1", 4) == 0) {
		Ctape_dmpmsg (MSG_OUT, "\n TRAILER LABEL 1:\n");
		printlabel1 (label);
		return (-1);
	} else if (strncmp (label, "EOF2", 4) == 0) {
		Ctape_dmpmsg (MSG_OUT, "\n TRAILER LABEL 2:\n");
		printlabel2 (label);
		return (-2);
	} else if (strncmp (label, "EOF3", 4) == 0) {
		Ctape_dmpmsg (MSG_OUT, "\n TRAILER LABEL 3:            %.76s\n", label+4);
		return (-3);
	} else if (strncmp (label, "EOF4", 4) == 0) {
		Ctape_dmpmsg (MSG_OUT, "\n TRAILER LABEL 4:            %.76s\n", label+4);
		return (-4);
	} else if (strncmp (label, "EOV1", 4) == 0) {
		Ctape_dmpmsg (MSG_OUT, "\n END OF VOLUME LABEL 1:\n");
		printlabel1 (label);
		return (-1);
	} else if (strncmp (label, "EOV2", 4) == 0) {
		Ctape_dmpmsg (MSG_OUT, "\n END OF VOLUME LABEL 2:\n");
		printlabel2 (label);
		return (-2);
	} else if (strncmp (label, "UTL1", 4) == 0) {
		Ctape_dmpmsg (MSG_OUT, "\n USER TRAILER LABEL 1:\n");
		printulabel1 (label);
		return (-5);
	}
	return (0);
}

int report_comp_stats (int infd,
                       char *path,
                       char *devtype)
{
	COMPRESSION_STATS compstats;

	memset (&compstats, 0, sizeof(compstats));
	if (get_compression_stats (infd, path, devtype, &compstats) == 0) {
		if (compstats.from_tape)
			Ctape_dmpmsg (MSG_OUT, " ***** COMPRESSION RATE ON TAPE: %8.2f\n",
			    (float)compstats.to_host / (float)compstats.from_tape);
	}
	return (0);
}

int Ctape_dmpend()
{
	if (infd >= 0)
		close (infd);
	infd = -1;
	return (0);
}


int Ctape_dmpfil(char *path,
                 char *lbltype,
                 int *blksize,
                 char *fid,
                 int *fsec,
                 int *fseq,
                 int *lrecl,
                 char *recfm,
                 u_signed64 *Size)
{
	int avg_block_length;
	unsigned char blockid[4];
	int errcat = 0;
	u_signed64 filesize;
	int goodrec = 0;
	char label[81];
	static char labels[4][3] = {"nl", "al", "nl", "sl"};
	static int lcode = 0;
	int max_block_length = 0;
	int min_block_length = 0;
	char *msgaddr;
	int nbytes;
	static int nerr = 0;
	static int nfile = 0;
	char *p;
	static int qbov = 1;
	int qlab = 0;
	u_signed64 sum_block_length = 0;
	static float tape_used = 0;
	char tmpbuf[21];

	filesize = 0;
	if (read_pos (infd, path, blockid) == 0) {
		Ctape_dmpmsg (MSG_OUT, " BLOCKID:                    %02x%02x%02x%02x\n",
                      blockid[0], blockid[1], blockid[2], blockid[3]);
    } else {
        Ctape_dmpmsg(MSG_OUT, " BLOCKID: Retrieve error\n");
    }
	while (1) {
		nbytes = read (infd, buffer, dmpparm.maxblksize);
		if (nbytes > 0) {		/* record found */
			irec++;
			tape_used = tape_used + (float) nbytes;
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
					Ctape_dmpmsg (MSG_OUT, "               %s HAS AN %s LABEL\n",
					    dmpparm.vid, codes[lcode]);
					Ctape_dmpmsg (MSG_OUT, "\n VOLUME LABEL :\n");
					Ctape_dmpmsg (MSG_OUT, " VOLUME SERIAL NUMBER:       %.6s\n",
					    label+4);
					Ctape_dmpmsg (MSG_OUT, " OWNER IDENTIFIER:           %.14s\n",
					    label+37);
				} else {
					Ctape_dmpmsg (MSG_OUT, "               %s IS UNLABELLED\n",
					    dmpparm.vid);
				}
				if (lbltype)
					strcpy (lbltype, labels[lcode]);
				if (dmpparm.fromfile > 1) {
					int n;
					goodrec = 0;
					n = dmpparm.fromfile - 1;
					if (lcode == AL || lcode == SL)
						n *= 3;
					Ctape_dmpmsg (MSG_OUT, "\n DUMP - SKIPPING TO FILE %d\n",
					    dmpparm.fromfile);
					if (skiptpff (infd, path, n) < 0) {
						Ctape_dmpmsg (MSG_ERR, " DUMP ! SKIP ERROR\n");
						errcat = EIO;
						break;
					}
					irec = 0;
					max_block_length = 0;
					min_block_length = 0;
					sum_block_length = 0;
					continue;
				}
				if (lcode)
					continue;
			} else if (irec < 6 && nbytes == 80) {
				if (lcode == SL)
					ebc_asc (buffer, label, 80);
				else
					asc_asc (buffer, label, 80);
				if ((qlab = islabel (label)) == 0 &&
				    lcode == 0) {
					ebc_asc (buffer, label, 80);
					qlab = islabel (label);
				}
				switch (qlab) {
				case 0:	/* not a label */
					break;
				case 1:	/* HDR1 */
					if (fid) {
						strncpy (fid, label + 4, 17);
						*(fid+17) = '\0';
						if ((p = strchr (fid, ' '))) *p = '\0';
					}
					if (fsec)
						sscanf (label+27, "%4d", fsec);
					if (fseq)
						sscanf (label+31, "%4d", fseq);
					continue;
				case 2:	/* HDR2 */
					if (recfm) {
						memset (recfm, 0, 4);
						*recfm = label[4];
						if (lcode == SL && label[38] != ' ') {
							if (label[38] == 'R')
								memcpy (recfm + 1, "BS", 2);
							else
								*(recfm + 1) = label[38];
						}
					}
					if (blksize)
						sscanf (label+5, "%5d", blksize);
					if (lrecl)
						sscanf (label+10, "%5d", lrecl);
					continue;
				case 5: /* UHL1 */
					if (fseq)
						sscanf (label+4, "%10d", fseq);
					if (blksize)
						sscanf (label+14, "%10d", blksize);
					if (lrecl)
						sscanf (label+24, "%10d", lrecl);
					continue;
				default:
					continue;
				}
			}
			qlab = 0;
			filesize += nbytes;
			if (irec >= dmpparm.fromblock &&
				(dmpparm.toblock == 0 || irec <= dmpparm.toblock)) {
				dumpblock (buffer, nbytes);
				Ctape_dmpmsg (MSG_OUT, " BLOCK NUMBER %d  LENGTH = %d BYTES\n",
					irec, nbytes);
			}

		} else if (nbytes == 0) {	/* tape mark found */
#if defined(sun) || defined(linux)
			if (gettperror (infd, path, &msgaddr) == ETBLANK) {
				if (!qbov && irec == 0) break;
				fflush (stdout);
				Ctape_dmpmsg (MSG_ERR, " DUMP ! READ ERROR: %s (BLOCK # %d)\n",
					"Blank check", irec+1);
				break;
			}
#endif
			if (qbov) {	/* beginning of tape */
				Ctape_dmpmsg (MSG_OUT, "               %s IS UNLABELLED\n", dmpparm.vid);
				if (lbltype)
					strcpy (lbltype, labels[lcode]);
			}
			if (qlab) {
				Ctape_dmpmsg (MSG_OUT, " ***** TAPE MARK READ *****      END OF LABEL GROUP\n");
				Ctape_dmpmsg (MSG_OUT, " *********************************************************************************************************************\n");
				if (dmpparm.maxfile != 0 && nfile >= dmpparm.maxfile) {
					(void) report_comp_stats (infd, path, dmpparm.devtype);
					Ctape_dmpmsg (MSG_OUT, " DUMP - DUMPING PROGRAM COMPLETE.\n");
					close (infd);
					return (1);
				}
			} else {
				nfile++;
				Ctape_dmpmsg (MSG_OUT, " ***** TAPE MARK READ *****      FILE %d CONTAINED %d BLOCKS.\n",
					nfile, irec);
				if (Size)
					*Size = filesize;
				if (goodrec != 0) {
					avg_block_length = sum_block_length / goodrec;
					Ctape_dmpmsg (MSG_OUT, "                                 FILE SIZE WAS %s BYTES\n",
						u64tostr (filesize, tmpbuf, 0));
					Ctape_dmpmsg (MSG_OUT, "                                 MAXIMUM BLOCK LENGTH WAS %d BYTES\n",
						max_block_length);
					Ctape_dmpmsg (MSG_OUT, "                                 MINIMUM BLOCK LENGTH WAS %d BYTES\n",
						min_block_length);
					Ctape_dmpmsg (MSG_OUT, "                                 AVERAGE BLOCK LENGTH WAS %d BYTES\n",
						avg_block_length);
					Ctape_dmpmsg (MSG_OUT, " *********************************************************************************************************************\n");
					if (lcode == 0 && dmpparm.maxfile != 0 &&
					    nfile >= dmpparm.maxfile) {
						(void) report_comp_stats (infd, path, dmpparm.devtype);
						Ctape_dmpmsg (MSG_OUT, " DUMP - DUMPING PROGRAM COMPLETE.\n");
						close (infd);
						return (1);
					}
				} else if ((dmpparm.flags & IGNOREEOI) == 0 && !qbov) {
					Ctape_dmpmsg (MSG_OUT, " ***** DOUBLE TAPE MARK READ *****\n");
					break;
				}
			}
			irec = 0;
			qbov = 0;
			if (lcode == 0 || qlab < 0) return (0);
			goodrec = 0;
			max_block_length = 0;
			min_block_length = 0;
			qlab = 0;
			sum_block_length = 0;
		} else {
			errcat = 0;
			if (errno == ENOMEM) {
				msgaddr = "Large block";
				errcat = ENOMEM;
			} else if (errno == EIO)
				errcat = gettperror (infd, path, &msgaddr);
			else
					msgaddr = (char *) strerror(errno);
			if (errcat == ETBLANK && !qbov && irec == 0) break;
			irec++;
			fflush (stdout);
			Ctape_dmpmsg (MSG_ERR, " DUMP ! READ ERROR: %s (BLOCK # %d)\n",
				msgaddr, irec);
			if (errcat == ETBLANK) break;
			if (++nerr >= 10) {
				Ctape_dmpmsg (MSG_ERR, " DUMP ! TOO MANY READ ERRORS.\n");
				break;
			}
		}
	}
	if (irec) {
		if (goodrec) {
			avg_block_length = sum_block_length / goodrec;
			Ctape_dmpmsg (MSG_OUT, "                                 MAXIMUM BLOCK LENGTH WAS %d BYTES\n",
				max_block_length);
			Ctape_dmpmsg (MSG_OUT, "                                 MINIMUM BLOCK LENGTH WAS %d BYTES\n",
				min_block_length);
			Ctape_dmpmsg (MSG_OUT, "                                 AVERAGE BLOCK LENGTH WAS %d BYTES\n",
				avg_block_length);
			Ctape_dmpmsg (MSG_OUT, " *********************************************************************************************************************\n");
		}
		Ctape_dmpmsg (MSG_OUT, " DUMP - DUMP ABORTED\n");
		serrno = (errcat > 0) ? errcat : EIO;
		close (infd);
		return (-1);
	}

	Ctape_dmpmsg (MSG_OUT, "\n ***** THE RECORDED DATA OCCUPIED %lu bytes OF A CARTRIDGE  *****\n",
		(unsigned long)tape_used) ;

	(void) report_comp_stats (infd, path, dmpparm.devtype);
	Ctape_dmpmsg (MSG_OUT, " DUMP - DUMPING PROGRAM COMPLETE.\n");
	close (infd);
	return (1);
}
