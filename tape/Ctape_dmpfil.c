/*
 * Copyright (C) 1990-2002 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: Ctape_dmpfil.c,v $ $Revision: 1.22 $ $Date: 2005/03/01 13:52:06 $ CERN IT-PDP/DM Jean-Philippe Baud";
#endif /* not lint */

/*	Ctape_dmpfil - analyse the content of a tape file */
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <errno.h>
#if defined(ADSTAR)
#include <sys/Atape.h>
#endif
#include "Ctape.h"
#include "Ctape_api.h"
#include "serrno.h"
void DLL_DECL (*Ctape_dmpmsg) _PROTO((int, const char *, ...)) = NULL;
static char *buffer;
static int code;
static char codes[4][7] = {"", "ASCII", "", "EBCDIC"};
static int den;		/* density code */
static int density;	/* nb bytes/inch for 3420/3480/3490 */
static int dev1tm = 0;	/* by default 2 tapemarks are written at EOI */
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
static float gap;	/* inter record gap for 3420/3480/3490 */
static int infd = -1;
static int irec;

Ctape_dmpinit(path, vid, aden, drive, devtype, maxblksize, fromblock, toblock, maxbyte, fromfile, maxfile, code, flags)
char *path;
char *vid;
char *aden;
char *drive;
char *devtype;
int maxblksize;
int fromblock;
int toblock;
int maxbyte;
int fromfile;
int maxfile;
int code;
int flags;
{
	struct devinfo *devinfo;
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

	/* Set default values */

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
	case D40G:
	case D40GC:
	case D50G:
	case D50GC:
	case D60G:
	case D60GC:
	case D100G:
	case D100GC:
	case D110G:
	case D110GC:
	case D160G:
	case D160GC:
	case D200G:
	case D200GC:
	case D300G:
	case D300GC:
	case DDS:
	case DDSC:
		break;
	default:
		Ctape_dmpmsg (MSG_ERR, TP006, "density");
		serrno = EINVAL;
		return (-1);
	}

	devinfo = Ctape_devinfo (devtype);
	dev1tm = (devinfo->eoitpmrks == 1) ? 1 : 0;

	if (maxblksize < 0)
		if (strcmp (devtype, "SD3") == 0)
			maxblksize = 262144;
		else
			maxblksize = 65536;
#if defined(_AIX) && defined(RS6000PCTA)
	if (maxblksize > 65535) maxblksize = 65535;
#endif
	if (maxbyte < 0) maxbyte = 320;
	if (maxfile < 0)
		if ((den & 0xFF) <= D38000 || (den & 0xFF) == D38KD)
			maxfile = 0;
		else
			maxfile = 1;
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

Ctape_dmpfil(path, lbltype, blksize, fid, fsec, fseq, lrecl, recfm, Size)
char *path;
char *lbltype;
int *blksize;
char *fid;
int *fsec;
int *fseq;
int *lrecl;
char *recfm;
u_signed64 *Size;
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
	int perc;
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
			if (strcmp (dmpparm.devtype, "SD3") == 0 ||
			    strcmp (dmpparm.devtype, "9840") == 0 ||
			    strcmp (dmpparm.devtype, "9940") == 0 ||
			    strcmp (dmpparm.devtype, "3592") == 0 ||
			    strcmp (dmpparm.devtype, "SDLT") == 0 ||
			    strcmp (dmpparm.devtype, "LTO") == 0)
				tape_used = tape_used + (float) nbytes;
			else if (strncmp (dmpparm.devtype, "DLT", 3) == 0)
				tape_used = tape_used + ((float) ((nbytes + 4095) / 4096) * 4096.);
			else if (den == DDS || den == DDSC)
				tape_used = tape_used + (float) nbytes + 4.0;
			else if (den != D8200 && den != D8200C &&
			    den != D8500 && den != D8500C)
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
						if (p = strchr (fid, ' ')) *p = '\0';
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
				if (dev1tm && !qbov && irec == 0) break;
				fflush (stdout);
				Ctape_dmpmsg (MSG_ERR, " DUMP ! READ ERROR: %s (BLOCK # %d)\n",
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
			else if (strncmp (dmpparm.devtype, "DLT", 3) == 0)
				tape_used += 4096.;
			else if (strcmp (dmpparm.devtype, "SD3") == 0)
				tape_used += 2048.;
			else if (den == DDS || den == DDSC)
				tape_used = tape_used + 4.0;
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
#if sgi
			close (infd);
			infd = open (path, O_RDONLY);
#endif
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
#if sgi || (__alpha && __osf__)
				if (errno == ENOSPC) {
					msgaddr = "Blank check";
					errcat = ETBLANK;
				} else
#endif
					msgaddr = (char *) strerror(errno);
			if (errcat == ETBLANK && dev1tm && !qbov && irec == 0) break;
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

	if (den <= D6250) {
		perc = (tape_used * 100) / (2400 * 12);
		Ctape_dmpmsg (MSG_OUT, "\n ***** THE RECORDED DATA OCCUPIED ABOUT %d %%  OF A FULL 2400 FOOT TAPE *****\n",
			perc);
	} else if (den == D38000 || den == D38KC) {
		perc = (tape_used * 100) / (500 * 12);
		Ctape_dmpmsg (MSG_OUT, "\n ***** THE RECORDED DATA OCCUPIED ABOUT %d %%  OF A STANDARD 3480 CARTRIDGE *****\n",
			perc);
	} else if (den == D38KD || den == D38KDC) {
		perc = (tape_used * 100) / (500 * 4 * 12);
		Ctape_dmpmsg (MSG_OUT, "\n ***** THE RECORDED DATA OCCUPIED ABOUT %d %%  OF A DOUBLE LENGTH CARTRIDGE *****\n",
			perc);
	} else if (den == D8200 || den == D8200C) {
		perc = tape_used / 23000000;
		Ctape_dmpmsg (MSG_OUT, "\n ***** THE RECORDED DATA OCCUPIED ABOUT %d %%  OF A STANDARD LENGTH (2.3GB) EXABYTE TAPE *****\n",
			perc);
	} else if (den == D8500 || den == D8500C) {
		perc = tape_used / 50000000;
		Ctape_dmpmsg (MSG_OUT, "\n ***** THE RECORDED DATA OCCUPIED ABOUT %d %%  OF A STANDARD LENGTH (5GB) EXABYTE TAPE *****\n",
			perc);
	} else if (den == D2G) {
		perc = tape_used / 26000000;
		Ctape_dmpmsg (MSG_OUT, "\n ***** THE RECORDED DATA OCCUPIED ABOUT %d %%  OF A CompacTapeIII (2.6GB) *****\n",
			perc);
	} else if (den == D6G) {
		perc = tape_used / 60000000;
		Ctape_dmpmsg (MSG_OUT, "\n ***** THE RECORDED DATA OCCUPIED ABOUT %d %%  OF A CompacTapeIII (6GB) *****\n",
			perc);
	} else if (den == D10G || den == D10GC) {
	    if (strncmp (dmpparm.devtype, "DLT", 3) == 0) {
		perc = tape_used / 100000000;
		Ctape_dmpmsg (MSG_OUT, "\n ***** THE RECORDED DATA OCCUPIED ABOUT %d %%  OF A CompacTapeIII (10GB) *****\n",
			perc);
	    } else if (strcmp (dmpparm.devtype, "SD3") == 0) {
		perc = tape_used / 100000000;
		Ctape_dmpmsg (MSG_OUT, "\n ***** THE RECORDED DATA OCCUPIED ABOUT %d %%  OF A Redwood CARTRIDGE (10GB) *****\n",
			perc);
#if defined(_AIX) && defined(_IBMR2) && defined(ADSTAR)
	    } else if (strcmp (dmpparm.devtype, "3590") == 0) {
		unsigned int cpos;
		unsigned char sense[128];
		unsigned int tach;
		unsigned int wrap;

		if (ioctl (infd, SIOC_REQSENSE, sense) < 0) {
			Ctape_dmpmsg (MSG_ERR, " DUMP ! UNABLE TO DETERMINE TAPE OCCUPANCY");
			serrno = EIO;
			close (infd);
			return (-1);
		}
		tach = sense[25];
		wrap = sense[53];
		cpos = sense[53] * 235;
		if ((wrap % 2) == 0)
			cpos += (tach - 9);
		else
			cpos += (244 - tach);
		perc = (cpos * 100) / (235 * 8);
		Ctape_dmpmsg (MSG_OUT, "\n ***** THE RECORDED DATA OCCUPIED ABOUT %d %%  OF A STANDARD 3590 CARTRIDGE *****\n",
			perc);
#endif
	    }
	} else if (den == D20G || den == D20GC) {
	    if (strncmp (dmpparm.devtype, "DLT", 3) == 0) {
		perc = tape_used / 200000000;
		Ctape_dmpmsg (MSG_OUT, "\n ***** THE RECORDED DATA OCCUPIED ABOUT %d %%  OF A CompacTapeIV (20GB) *****\n",
			perc);
	    } else if (strcmp (dmpparm.devtype, "9840") == 0) {
		perc = tape_used / 200000000;
		Ctape_dmpmsg (MSG_OUT, "\n ***** THE RECORDED DATA OCCUPIED ABOUT %d %%  OF AN STK 9840 CARTRIDGE (20GB) *****\n",
			perc);
	    }
	} else if (den == D25G || den == D25GC) {
		perc = tape_used / 250000000;
		Ctape_dmpmsg (MSG_OUT, "\n ***** THE RECORDED DATA OCCUPIED ABOUT %d %%  OF A Redwood CARTRIDGE (25GB) *****\n",
			perc);
	} else if (den == D35G || den == D35GC) {
		perc = tape_used / 350000000;
		Ctape_dmpmsg (MSG_OUT, "\n ***** THE RECORDED DATA OCCUPIED ABOUT %d %%  OF A CompacTapeIV (35GB) *****\n",
			perc);
	} else if (den == D40G || den == D40GC) {
	    if (strncmp (dmpparm.devtype, "DLT", 3) == 0) {
		perc = tape_used / 400000000;
		Ctape_dmpmsg (MSG_OUT, "\n ***** THE RECORDED DATA OCCUPIED ABOUT %d %%  OF A CompacTapeIV (40GB) *****\n",
			perc);
	    }
	} else if (den == D50G || den == D50GC) {
		perc = tape_used / 500000000;
		Ctape_dmpmsg (MSG_OUT, "\n ***** THE RECORDED DATA OCCUPIED ABOUT %d %%  OF A Redwood CARTRIDGE (50GB) *****\n",
			perc);
	} else if (den == D60G || den == D60GC) {
		perc = tape_used / 600000000;
		Ctape_dmpmsg (MSG_OUT, "\n ***** THE RECORDED DATA OCCUPIED ABOUT %d %%  OF AN STK 9940 CARTRIDGE (60GB) *****\n",
			perc);
	} else if (den == D100G || den == D100GC) {
		perc = tape_used / 1000000000;
		Ctape_dmpmsg (MSG_OUT, "\n ***** THE RECORDED DATA OCCUPIED ABOUT %d %%  OF AN LTO CARTRIDGE (100GB) *****\n",
			perc);
	} else if (den == D110G || den == D110GC) {
		perc = tape_used / 1100000000;
		Ctape_dmpmsg (MSG_OUT, "\n ***** THE RECORDED DATA OCCUPIED ABOUT %d %%  OF AN SDLT CARTRIDGE (110GB) *****\n",
			perc);
	} else if (den == D160G || den == D160GC) {
		perc = tape_used / 1600000000;
		Ctape_dmpmsg (MSG_OUT, "\n ***** THE RECORDED DATA OCCUPIED ABOUT %d %%  OF AN SDLT CARTRIDGE (160GB) *****\n",
			perc);
	} else if (den == D200G || den == D200GC) {
		perc = tape_used / 2000000000;
		if (strcmp (dmpparm.devtype, "9940") == 0)
			Ctape_dmpmsg (MSG_OUT, "\n ***** THE RECORDED DATA OCCUPIED ABOUT %d %%  OF AN STK 9940 CARTRIDGE (200GB) *****\n",
				perc);
		else
			Ctape_dmpmsg (MSG_OUT, "\n ***** THE RECORDED DATA OCCUPIED ABOUT %d %%  OF AN LTO2 CARTRIDGE (200GB) *****\n",
				perc);
	} else if (den == D300G || den == D300GC) {
		perc = tape_used / 3000000000UL;
		Ctape_dmpmsg (MSG_OUT, "\n ***** THE RECORDED DATA OCCUPIED ABOUT %u %%  OF AN 3592 CARTRIDGE (300GB) *****\n",
			perc);
	} else if (den == DDS || den == DDSC) {
		perc = tape_used / 40000000;
		Ctape_dmpmsg (MSG_OUT, "\n ***** THE RECORDED DATA OCCUPIED ABOUT %d %%  OF A DDS2 CARTRIDGE (4GB) *****\n",
			perc);
	}
	(void) report_comp_stats (infd, path, dmpparm.devtype);
	Ctape_dmpmsg (MSG_OUT, " DUMP - DUMPING PROGRAM COMPLETE.\n");
	close (infd);
	return (1);
}

dumpblock (buffer, nbytes)
char *buffer;
int nbytes;
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

printlabel1 (label)
char *label;
{
	Ctape_dmpmsg (MSG_OUT, " FILE ID:                    %.17s\n", label + 4);
	Ctape_dmpmsg (MSG_OUT, " VOLUME SEQNO:               %.4s\n", label + 27);
	Ctape_dmpmsg (MSG_OUT, " FILE SEQNO:                 %.4s\n", label + 31);
	Ctape_dmpmsg (MSG_OUT, " CREATION DATE:              %.6s\n", label + 41);
	Ctape_dmpmsg (MSG_OUT, " EXPIRATION DATE:            %.6s\n", label + 47);
	Ctape_dmpmsg (MSG_OUT, " BLOCK COUNT:                %.6s\n", label + 54);
}

printlabel2 (label)
char *label;
{
	Ctape_dmpmsg (MSG_OUT, " RECORD FORMAT:              %.1s\n", label + 4);
	Ctape_dmpmsg (MSG_OUT, " BLOCK LENGTH:               %.5s\n", label + 5);
	Ctape_dmpmsg (MSG_OUT, " RECORD LENGTH:              %.5s\n", label + 10);
	Ctape_dmpmsg (MSG_OUT, " DENSITY:                    %.1s\n", label + 15);
	Ctape_dmpmsg (MSG_OUT, " DATA RECORDING:             %.1s\n", label + 34);
	Ctape_dmpmsg (MSG_OUT, " BLOCKING ATTRIBUTE:         %.1s\n", label + 38);
}

printulabel1 (label)
char *label;
{
	Ctape_dmpmsg (MSG_OUT, " FILE SEQNO:                 %.10s\n", label + 4);
	Ctape_dmpmsg (MSG_OUT, " BLOCK LENGTH:               %.10s\n", label + 14);
	Ctape_dmpmsg (MSG_OUT, " RECORD LENGTH:              %.10s\n", label + 24);
	Ctape_dmpmsg (MSG_OUT, " SITE:                       %.8s\n", label + 34);
	Ctape_dmpmsg (MSG_OUT, " TAPE MOVER HOSTNAME:        %.10s\n", label + 42);
	Ctape_dmpmsg (MSG_OUT, " DRIVE MANUFACTURER:         %.8s\n", label + 52);
	Ctape_dmpmsg (MSG_OUT, " DRIVE MODEL:                %.8s\n", label + 60);
	Ctape_dmpmsg (MSG_OUT, " DRIVE SERIAL NUMBER:        %.12s\n", label + 68);
}

report_comp_stats (infd, path, devtype)
int infd;
char *path;
char *devtype;
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

Ctape_dmpend()
{
	if (infd >= 0)
		close (infd);
	infd = -1;
	return (0);
}
