/*
 * Copyright (C) 1998-2003 by CERN/IT/PDP/DM
 * All rights reserved
 */
 
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
#include "getconfent.h"
			/* exit codes */

#define	USERR	1

extern char *optarg;

static void usage(const char *const cmd)
{
	fprintf (stderr, "usage: %s ", cmd);
	fprintf (stderr,
	    "-d -D drive_ordinal -h rmcserver [-V vid] [-v]\n"
	    "\t-e -h rmcserver -V vid [-v]\n"
	    "\t-i -h rmcserver [-V vid] [-v]\n"
	    "\t-m -D drive_ordinal -h rmcserver [-I] -V vid [-v]\n"
	    "\t-q D [-D drive_ordinal] -h rmcserver [-v]\n"
	    "\t-q L -h rmcserver [-v]\n"
	    "\t-q S -h rmcserver [-N nbelem] [-S starting_slot] [-v]\n"
	    "\t-q V -h rmcserver [-N nbelem] [-V vid] [-v]\n");
}

static int smc_qdrive (
	const char *const rmchost,
	const int fd,
	const struct robot_info *const robot_info,
	int drvord,
	const int verbose)
{
        int c;
        struct smc_element_info *element_info;
	int i;
	int nbelem;
	char *pstatus;
        char *smcLibraryType;
        char useSpectraLib;
 
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
	if ((c = rmc_read_elem_status (rmchost, 4,
	    robot_info->device_start+drvord, nbelem, element_info)) < 0) {
		free (element_info);
		return (c);
	}
	if (verbose)
		printf ("Drive Ordinal\tElement Addr.\tStatus\t\tVid\n");

        useSpectraLib=0;
        smcLibraryType = getconfent("SMC","LIBRARY_TYPE",0);
        if (NULL != smcLibraryType && 
            0 == strcasecmp(smcLibraryType,"SPECTRA")) {
          useSpectraLib = 1;
        }
 
	for (i = 0; i < c; i++) {
		if (((element_info+i)->state & 0x1) == 0)
			pstatus = "free";
		else if ((element_info+i)->state & 0x4)
			pstatus = "error";
		else if ((element_info+i)->state & 0x8 && !useSpectraLib)
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

static int smc_qlib (const struct robot_info *const robot_info)
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

static int smc_qport (
	const char *const rmchost,
	const int fd,
	const struct robot_info *const robot_info,
	const int verbose)
{
	int c;
	struct smc_element_info *element_info;
	int i;
	int nbelem;
	char *pstatus;

	nbelem = robot_info->port_count;
	if ((element_info = malloc (nbelem * sizeof(struct smc_element_info))) == NULL) {
		fprintf (stderr, SR012);
		return (USERR);
	}

	if ((c = rmc_read_elem_status (rmchost, 3,
	    robot_info->port_start, nbelem, element_info)) < 0) {
		free (element_info);
		return (serrno - ERMCRBTERR);
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
 
static int smc_qslot (
	const char *const rmchost,
	const int fd,
	const struct robot_info *robot_info,
	int slotaddr,
	int nbelem,
	const int verbose)
{
        int c;
        struct smc_element_info *element_info;
	int i;
 
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

	if ((c = rmc_read_elem_status (rmchost, 2, slotaddr,
	    nbelem, element_info)) < 0) {
		free (element_info);
		return (serrno - ERMCRBTERR);
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

static int smc_qvid (
	const char *const rmchost,
	const int fd,
	const struct robot_info *const robot_info,
	const char *reqvid,
	int nbelem,
	const int verbose)
{
        int c;
        struct smc_element_info *element_info;
	int i;
	char *ptype;
	static char ptypes[5][6] = {"", "hand", "slot", "port", "drive"};
	const char *vid;
 
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

	if ((c = rmc_find_cartridge (rmchost, vid, 0, 0, nbelem,
	    element_info)) < 0) {
		free (element_info);
		return (serrno - ERMCRBTERR);
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

int main(const int argc,
         char **argv)
{
	int c;
	char *dp;
	int drvord = -1;
	int errflg = 0;
	const int fd = -1;
	int invert = 0;
	int n;
	int nbelem = 0;
	char qry_type = 0;
	char req_type = 0;
	struct robot_info robot_info;
	char rmchost[CA_MAXHOSTNAMELEN+1];
	int slotaddr = -1;
	int targetslotaddr = -1;
	int verbose = 0;
	char vid[7];

	/* parse and check command options */

        memset(rmchost, '\0', sizeof(rmchost));
	memset (vid, '\0', sizeof(vid));
	while ((c = getopt (argc, argv, "D:deh:IimN:q:S:V:vT:")) != EOF) {
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
			if(strlen(optarg) > (sizeof(rmchost) - 1)) {
				fprintf(stderr,
				  "rmcserver %s must be at most %d characters long\n",
				  optarg, (int)(sizeof(rmchost) - 1));
				errflg++;
			} else {
				strncpy (rmchost, optarg, sizeof(rmchost) - 1);
			}
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
	if (req_type && *rmchost == '\0') {
		fprintf (stderr, "rmcserver must be specified\n");
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
	if (errflg || req_type == 0) {
		usage (argv[0]);
		exit (USERR);
	}

	/* get robot geometry */
	if (rmc_get_geometry (rmchost, &robot_info)) {
		exit (serrno);
	}

	if (drvord >= robot_info.device_count) {
		fprintf (stderr, SR008, robot_info.device_count);
		exit (USERR);
	}
	if (slotaddr > (robot_info.slot_count + robot_info.slot_start)) {
		fprintf (stderr, SR016, robot_info.slot_count + robot_info.slot_start);
		exit (USERR);
	}

	/* process request */

	switch (req_type) {
	case 'd':
		if ((c = rmc_dismount (rmchost, vid, drvord, 0)) < 0) {
			c = (serrno == SECOMERR) ? RBT_FAST_RETRY : serrno - ERMCRBTERR;
		}
		break;	
	case 'e':
		if ((c = rmc_export (rmchost, vid)) < 0) {
			c = (serrno == SECOMERR) ? RBT_FAST_RETRY : serrno - ERMCRBTERR;
		}
		break;
	case 'i':
		if ((c = rmc_import (rmchost, vid)) < 0) {
			c = (serrno == SECOMERR) ? RBT_FAST_RETRY : serrno - ERMCRBTERR;
		}
		break;
	case 'm':
		if ((c = rmc_mount (rmchost, vid, invert, drvord)) < 0) {
			c = (serrno == SECOMERR) ? RBT_FAST_RETRY : serrno - ERMCRBTERR;
		}
		break;	
	case 'q':
		switch (qry_type) {
		case 'D':
			c = smc_qdrive (rmchost, fd, &robot_info, drvord,
			    verbose);
			break;
		case 'L':
			c = smc_qlib (&robot_info);
			break;
		case 'P':
			c = smc_qport (rmchost, fd, &robot_info, verbose);
			break;
		case 'S':
			c = smc_qslot (rmchost, fd, &robot_info, slotaddr,
			    nbelem, verbose);
			break;
		case 'V':
			c = smc_qvid (rmchost, fd, &robot_info, vid,
			    nbelem, verbose);
			break;
		}
		break;

	}
	exit (c);
}
