/*
 * Copyright (C) 1998-2003 by CERN/IT/PDP/DM
 * All rights reserved
 */
 
#ifndef lint
/* static char sccsid[] = "@(#)$RCSfile: smc.c,v $ $Revision: 1.11 $ $Date: 2007/03/26 12:14:53 $ CERN IT-PDP/DM Jean-Philippe Baud"; */
#endif /* not lint */

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "Ctape.h"
#include "rmc_api.h"
#include "serrno.h"
#include "smc.h"
			/* exit codes */

#define	USERR	1

extern char *optarg;

void usage(cmd)
char *cmd;
{
	fprintf (stderr, "usage: %s ", cmd);
	fprintf (stderr, "%s%s%s%s%s%s%s%s%s",
	    "-d -D drive_ordinal [-h rmcserver] -l loader [-V vid] [-v]\n",
	    "\t-e [-h rmcserver] -l loader -V vid [-v]\n",
	    "\t-i [-h rmcserver] -l loader [-V vid] [-v]\n",
	    "\t-m -D drive_ordinal [-h rmcserver] [-I] -l loader -V vid [-v]\n",
	    "\t-M -l loader  -S starting_slot -T end_slot [-v]\n",
	    "\t-q D [-D drive_ordinal] [-h rmcserver] -l loader [-v]\n",
	    "\t-q L [-h rmcserver] -l loader [-v]\n",
	    "\t-q S [-h rmcserver] -l loader [-N nbelem] [-S starting_slot] [-v]\n",
	    "\t-q V [-h rmcserver] -l loader [-N nbelem] [-V vid] [-v]\n");
}

int smc_qdrive (rmchost, fd, loader, robot_info, drvord, verbose)
char *rmchost;
int fd;
char *loader;
struct robot_info *robot_info;
int drvord;
int verbose;
{
        int c;
        struct smc_element_info *element_info;
	int i;
	char *msgaddr;
	int nbelem;
	char *pstatus;
	struct smc_status smc_status;
 
	if (drvord < 0) {
		drvord = 0;
		nbelem = robot_info->device_count;
	} else {
		nbelem = 1;
	}
	if ((element_info = malloc (nbelem * sizeof(struct smc_element_info))) == NULL) {
		fprintf (stderr, SR012);
		return (USERR);
	}
	if (*rmchost == '\0') {
		if ((c = smc_read_elem_status (fd, loader, 4,
		    robot_info->device_start+drvord, nbelem, element_info)) < 0) {
			c = smc_lasterror (&smc_status, &msgaddr);
			fprintf (stderr, SR020, "read_elem_status", msgaddr);
			free (element_info);
			return (c);
		}
	} else {
		if ((c = rmc_read_elem_status (rmchost, loader, 4,
		    robot_info->device_start+drvord, nbelem, element_info)) < 0) {
			free (element_info);
			return (c);
		}
	}
	if (verbose)
		printf ("Drive Ordinal\tElement Addr.\tStatus\t\tVid\n");
	for (i = 0; i < c; i++) {
		if (((element_info+i)->state & 0x1) == 0)
			pstatus = "free";
		else if ((element_info+i)->state & 0x4)
			pstatus = "error";
		else if ((element_info+i)->state & 0x8)
			pstatus = "unloaded";
		else
			pstatus = "loaded";
		printf ("	%2d\t    %d\t%s\t%s\n",
			(element_info+i)->element_address-robot_info->device_start,
			(element_info+i)->element_address, pstatus,
			(element_info+i)->name);
	}
	free (element_info);
	return (0);
}

int smc_qlib (rmchost, fd, loader, robot_info, verbose)
char *rmchost;
int fd;
char *loader;
struct robot_info *robot_info;
int verbose;
{
	printf ("Vendor/Product/Revision = <%s>\n", robot_info->inquiry);
	printf ("Transport Count = %d, Start = %d\n",
		robot_info->transport_count, robot_info->transport_start);
	printf ("Slot Count = %d, Start = %d\n",
		robot_info->slot_count, robot_info->slot_start);
	printf ("Port Count = %d, Start = %d\n",
		robot_info->port_count, robot_info->port_start);
	printf ("Device Count = %d, Start = %d\n",
		robot_info->device_count, robot_info->device_start);
	return (0);
}

int smc_qport (rmchost, fd, loader, robot_info, verbose)
char *rmchost;
int fd;
char *loader;
struct robot_info *robot_info;
int verbose;
{
    int c;
    struct smc_element_info *element_info;
	int i;
	char *msgaddr;
	int nbelem;
	char *pstatus;
	struct smc_status smc_status;

	nbelem = robot_info->port_count;
	if ((element_info = malloc (nbelem * sizeof(struct smc_element_info))) == NULL) {
		fprintf (stderr, SR012);
		return (USERR);
	}

	if (*rmchost == '\0') {
		if ((c = smc_read_elem_status (fd, loader, 3,
		    robot_info->port_start, nbelem, element_info)) < 0) {
			c = smc_lasterror (&smc_status, &msgaddr);
			fprintf (stderr, SR020, "read_elem_status", msgaddr);
			free (element_info);
			return (c);
		}
	} else {
		if ((c = rmc_read_elem_status (rmchost, loader, 3,
		    robot_info->port_start, nbelem, element_info)) < 0) {
			free (element_info);
			return (serrno - ERMCRBTERR);
		}
	}
	if (verbose)
		printf ("Element Addr.\tVid\tImpExp\n");
	for (i = 0; i < c; i++) {
		if (((element_info+i)->state & 0x1) == 0)
			pstatus = "";
		else if (((element_info+i)->state & 0x2) == 0)
			pstatus = "export";
		else
			pstatus = "import";
		printf ("    %4d\t%s\t%s\n",
			(element_info+i)->element_address,
			(element_info+i)->name, pstatus);
	}
	free (element_info);
	return (0);
}
 
int smc_qslot (rmchost, fd, loader, robot_info, slotaddr, nbelem, verbose)
char *rmchost;
int fd;
char *loader;
struct robot_info *robot_info;
int slotaddr;
int nbelem;
int verbose;
{
        int c;
        struct smc_element_info *element_info;
	int i;
	char *msgaddr;
	struct smc_status smc_status;
 
	if (nbelem == 0) {
		if (slotaddr < 0)
			nbelem = robot_info->slot_count;
		else
			nbelem = 1;
	}
	if (slotaddr < 0)
		slotaddr = 0;
	if ((element_info = malloc (nbelem * sizeof(struct smc_element_info))) == NULL) {
		fprintf (stderr, SR012);
		return (USERR);
	}

	if (*rmchost == '\0') {
		if ((c = smc_read_elem_status (fd, loader, 2, slotaddr,
		    nbelem, element_info)) < 0) {
			c = smc_lasterror (&smc_status, &msgaddr);
			fprintf (stderr, SR020, "read_elem_status", msgaddr);
			free (element_info);
			return (c);
		}
	} else {
		if ((c = rmc_read_elem_status (rmchost, loader, 2, slotaddr,
		    nbelem, element_info)) < 0) {
			free (element_info);
			return (serrno - ERMCRBTERR);
		}
	}
	if (verbose)
		printf ("Element Addr.\tVid\n");
	for (i = 0; i < c; i++) {
		printf ("    %4d\t%s\n",
			element_info[i].element_address, element_info[i].name);
	}
	free (element_info);
	return (0);
}

int smc_qvid (rmchost, fd, loader, robot_info, reqvid, nbelem, verbose)
char *rmchost;
int fd;
char *loader;
struct robot_info *robot_info;
char *reqvid;
int nbelem;
int verbose;
{
        int c;
        struct smc_element_info *element_info;
	int i;
	char *msgaddr;
	char *ptype;
	static char ptypes[5][6] = {"", "hand", "slot", "port", "drive"};
	struct smc_status smc_status;
	char *vid;
 
	if (*reqvid)
		vid = reqvid;
	else
		vid = "*";
	if (nbelem == 0) {
		if (strchr (vid, '*') || strchr (vid, '?'))	/* pattern matching */
			nbelem = robot_info->transport_count + robot_info->slot_count +
				 robot_info->port_count + robot_info->device_count;
		else
			nbelem = 1;
	}
	if ((element_info = malloc (nbelem * sizeof(struct smc_element_info))) == NULL) {
		fprintf (stderr, SR012);
		return (USERR);
	}

	if (*rmchost == '\0') {
		if ((c = smc_find_cartridge (fd, loader, vid, 0, 0, nbelem,
		    element_info)) < 0) {
			c = smc_lasterror (&smc_status, &msgaddr);
			fprintf (stderr, SR017, "find_cartridge", vid, msgaddr);
			free (element_info);
			return (c);
		}
	} else {
		if ((c = rmc_find_cartridge (rmchost, loader, vid, 0, 0, nbelem,
		    element_info)) < 0) {
			free (element_info);
			return (serrno - ERMCRBTERR);
		}
	}
	if (verbose)
		printf ("Vid\tElement Addr.\tElement Type\n");
	for (i = 0; i < c; i++) {
		ptype = ptypes[(element_info+i)->element_type];
		if ((element_info+i)->element_type == 3) {
			if (((element_info+i)->state & 0x2) == 0) {
				ptype = "export";
			} else {
				ptype = "import";
                        }
                }
		printf ("%s\t    %4d\t%s\n",
			(element_info+i)->name, (element_info+i)->element_address,
			ptype);
	}
	free (element_info);
	return (0);
}

int main(argc, argv)
int argc;
char **argv;
{
	int c;
	char *dp;
	int drvord = -1;
	int errflg = 0;
	int fd = -1;
	int invert = 0;
	char loader[32];
	char *msgaddr;
	int n;
	int nbelem = 0;
	char qry_type = 0;
	char req_type = 0;
	struct robot_info robot_info;
	char rmchost[CA_MAXHOSTNAMELEN+1];
	int slotaddr = -1;
	int targetslotaddr = -1;
	struct smc_status smc_status;
	int verbose = 0;
	char vid[7];

	/* parse and check command options */

	loader[0] = '\0';
	rmchost[0] = '\0';
	memset (vid, '\0', sizeof(vid));
	while ((c = getopt (argc, argv, "D:deh:Iil:mN:q:S:V:vMT:")) != EOF) {
		switch (c) {
		case 'D':	/* drive ordinal */
			drvord = strtol (optarg, &dp, 10);
			if (*dp != '\0' || drvord < 0) {
				fprintf (stderr, SR001);
				errflg++;
			}
			break;
		case 'd':	/* demount */
		case 'e':	/* export */
			if (req_type) {
				fprintf (stderr, SR002, req_type, c);
				errflg++;
			} else
				req_type = c;
			break;
		case 'h':	/* remote server */
			strcpy (rmchost, optarg);
			break;
		case 'i':	/* import */
			if (req_type) {
				fprintf (stderr, SR002, req_type, c);
				errflg++;
			} else
				req_type = c;
			break;
		case 'I':	/* invert */
			invert = 1;
			break;
		case 'l':	/* loader */
			strcpy (loader, optarg);
			break;
		case 'm':	/* mount */
			if (req_type) {
				fprintf (stderr, SR002, req_type, c);
				errflg++;
			} else
				req_type = c;
			break;
		case 'N':	/* number of elements */
			nbelem = strtol (optarg, &dp, 10);
			if (*dp != '\0' || nbelem <= 0) {
				fprintf (stderr, SR010);
				errflg++;
			}
			break;
		case 'q':	/* query */
			if (req_type) {
				fprintf (stderr, SR002, req_type, c);
				errflg++;
			} else {
				req_type = c;
				qry_type = *optarg;
				if (qry_type != 'D' && qry_type != 'L' &&
				    qry_type != 'P' && qry_type != 'S' &&
				    qry_type != 'V') {
					fprintf (stderr, SR003, qry_type);
					errflg++;
				}
			}
			break;
		case 'S':	/* starting slot */
			slotaddr = strtol (optarg, &dp, 10);
			if (*dp != '\0' || slotaddr < 0) {
				fprintf (stderr, SR001);
				errflg++;
			}
			break;
		case 'T':	/* Target slot */
			targetslotaddr = strtol (optarg, &dp, 10);
			if (*dp != '\0' || targetslotaddr < 0) {
				fprintf (stderr, SR001);
				errflg++;
			}
			break;
		case 'M':	/* move */
		  if (req_type) {
				fprintf (stderr, SR002, req_type, c);
				errflg++;
			} else
				req_type = c;
			break;
		case 'V':	/* vid */
			n = strlen (optarg);
			if (n > 6) {
				fprintf (stderr, SR004, optarg);
				errflg++;
			} else {
				strcpy (vid, optarg);
				UPPER (vid);
			}
			break;
		case 'v':
			verbose = 1;
			break;
		case '?':
			errflg++;
			break;
		}
	}
	if (req_type && *loader == '\0') {
		fprintf (stderr, SR005);
		errflg++;
	}
	if (req_type == 'd' && drvord < 0) {
		fprintf (stderr, SR006);
		errflg++;
	}
	if (req_type == 'e' && *vid =='\0') {
		fprintf (stderr, SR011);
		errflg++;
	}
	if (req_type == 'm' && (*vid =='\0' || drvord < 0)) {
		fprintf (stderr, SR007);
		errflg++;
	}
	if (req_type == 'M' && slotaddr == -1 && targetslotaddr == -1) {
		fprintf (stderr, SR021);
		errflg++;
	}
	if (errflg || req_type == 0) {
		usage (argv[0]);
		exit (USERR);
	}

	/* get robot geometry */

	if (*rmchost == '\0') {
#if defined(SOLARIS25) || defined(hpux)
		/* open the SCSI picker device
		   (open is done in send_scsi_cmd for the other platforms */

		if ((fd = open (loader, O_RDWR)) < 0) {
			if (errno == EBUSY)
				c = EBUSY;
			else
				c = RBT_NORETRY;
			fprintf (stderr, SR019, loader, "open", strerror(errno));
			exit (c);
		}
#endif
		if ((c = smc_get_geometry (fd, loader, &robot_info))) {
			c = smc_lasterror (&smc_status, &msgaddr);
			fprintf (stderr, SR020, "get_geometry", msgaddr);
			exit (c);
		}
	} else {
		if (rmc_get_geometry (rmchost, loader, &robot_info))
			exit (serrno);
	}

	if (drvord >= robot_info.device_count) {
		fprintf (stderr, SR008, robot_info.device_count);
		exit (USERR);
	}
	if (req_type != 'M' && slotaddr > (robot_info.slot_count + robot_info.slot_start)) {
		fprintf (stderr, SR016, robot_info.slot_count + robot_info.slot_start);
		exit (USERR);
	}

	/* process request */

	switch (req_type) {
	case 'd':
		if (*rmchost == '\0')
			c = smc_dismount (fd, loader, &robot_info, drvord, vid);
		else
			if ((c = rmc_dismount (rmchost, loader, vid, drvord, 0)) < 0)
				c = (serrno == SECOMERR) ? RBT_FAST_RETRY : serrno - ERMCRBTERR;
		break;	
	case 'e':
		if (*rmchost == '\0')
			c = smc_export (fd, loader, &robot_info, vid);
		else
			if ((c = rmc_export (rmchost, loader, vid)) < 0)
				c = (serrno == SECOMERR) ? RBT_FAST_RETRY : serrno - ERMCRBTERR;
		break;
	case 'i':
		if (*rmchost == '\0')
			c = smc_import (fd, loader, &robot_info, vid);
		else
			if ((c = rmc_import (rmchost, loader, vid)) < 0)
				c = (serrno == SECOMERR) ? RBT_FAST_RETRY : serrno - ERMCRBTERR;
		break;
	case 'm':
		if (*rmchost == '\0')
			c = smc_mount (fd, loader, &robot_info, drvord, vid, invert);
		else
			if ((c = rmc_mount (rmchost, loader, vid, invert, drvord)) < 0)
				c = (serrno == SECOMERR) ? RBT_FAST_RETRY : serrno - ERMCRBTERR;
		break;	
	case 'q':
		switch (qry_type) {
		case 'D':
			c = smc_qdrive (rmchost, fd, loader, &robot_info, drvord,
			    verbose);
			break;
		case 'L':
			c = smc_qlib (rmchost, fd, loader, &robot_info, verbose);
			break;
		case 'P':
			c = smc_qport (rmchost, fd, loader, &robot_info, verbose);
			break;
		case 'S':
			c = smc_qslot (rmchost, fd, loader, &robot_info, slotaddr,
			    nbelem, verbose);
			break;
		case 'V':
			c = smc_qvid (rmchost, fd, loader, &robot_info, vid,
			    nbelem, verbose);
			break;
		}
		break;
	case 'M':
	        if (*rmchost == '\0') {
		  int ret;
		  c = smc_move_medium(fd, loader, slotaddr, targetslotaddr,  invert);
		  if (c != 0) {
		    ret = smc_lasterror (&smc_status, &msgaddr);
		    fprintf(stderr, SR020, "move medium", msgaddr);
		  }
		
		} else {
		  	fprintf (stderr, "Remote move medium not implemented\n");
			exit (USERR);
		}
		break;	

	}
	exit (c);
}
