/*
 * Copyright (C) 1998-2002 by CERN/IT/PDP/DM
 * All rights reserved
 */
 
#ifndef lint
static char sccsid[] = "@(#)$RCSfile: smc.c,v $ $Revision: 1.3 $ $Date: 2002/07/24 07:25:47 $ CERN IT-PDP/DM Jean-Philippe Baud";
#endif /* not lint */

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "Ctape.h"
#include "smc.h"
			/* exit codes */

#define	USERR	1

			/* error messages */

#define	SR001	"SR001 - drive ordinal must be a non negative integer\n"
#define	SR002	"SR002 - option -%c and -%c are mutually exclusive\n"
#define	SR003	"SR003 - invalid query type %c\n"
#define	SR004	"SR004 - vid %s must be at most 6 characters long\n"
#define	SR005	"SR005 - loader must be specified\n"
#define	SR006	"SR006 - drive ordinal is mandatory for demount operations\n"
#define	SR007	"SR007 - drive ordinal and vid are mandatory for mount operations\n"
#define	SR008	"SR008 - invalid device ordinal (must be < %d)\n"
#define	SR009	"SR009 - vid mismatch: %s on request, %s on drive\n"
#define	SR010	"SR010 - number of elements must be a positive integer\n"
#define	SR011	"SR011 - vid is mandatory for export operations\n"
#define	SR012	"SR012 - cannot allocate enough memory\n"
#define	SR013	"SR013 - export slots are full\n"
#define	SR014	"SR014 - slot ordinal must be a non negative integer\n"
#define	SR015	"SR015 - storage cells are full\n"
#define	SR016	"SR016 - invalid slot address (must be < %d)\n"
#define	SR017	"SR017 - %s %s failed : %s\n"
#define	SR018	"SR018 - %s of %s on drive %d failed : %s\n"
#define	SR019	"SR019 - %s : %s error : %s\n"
#define	SR020	"SR020 - %s failed : %s\n"
extern char *optarg;
#if !defined(linux)
extern char *sys_errlist[];
#endif
main(argc, argv)
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
	int slotaddr = -1;
	struct smc_status smc_status;
	int verbose = 0;
	char vid[7];

	/* parse and check command options */

	loader[0] = '\0';
	memset (vid, '\0', sizeof(vid));
	while ((c = getopt (argc, argv, "D:deIil:mN:q:S:V:v")) != EOF) {
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
	if (errflg || req_type == 0) {
		usage (argv[0]);
		exit (USERR);
	}

#if defined(SOLARIS25) || defined(hpux)
	/* open the SCSI picker device
	   (open is done in send_scsi_cmd for the other platforms */

	if ((fd = open (loader, O_RDWR)) < 0) {
		if (errno == EBUSY)
			c = EBUSY;
		else
			c = RBT_NORETRY;
		fprintf (stderr, SR019, loader, "open", sys_errlist[errno]);
		exit (c);
	}
#endif

	/* get robot geometry */

	if (c = smc_get_geometry (fd, loader, &robot_info)) {
		c = smc_lasterror (&smc_status, &msgaddr);
		fprintf (stderr, SR020, "get_geometry", msgaddr);
		exit (c);
	}

	if (drvord >= robot_info.device_count) {
		fprintf (stderr, SR008, robot_info.device_count);
		exit (USERR);
	}
	if (slotaddr >= robot_info.slot_count) {
		fprintf (stderr, SR016, robot_info.slot_count);
		exit (USERR);
	}

	/* process request */

	switch (req_type) {
	case 'd':
		c = smc_dismount (fd, loader, &robot_info, drvord, vid);
		break;	
	case 'e':
		c = smc_export (fd, loader, &robot_info, vid);
		break;
	case 'i':
		c = smc_import (fd, loader, &robot_info, vid);
		break;
	case 'm':
		c = smc_mount (fd, loader, &robot_info, drvord, vid, invert);
		break;	
	case 'q':
		switch (qry_type) {
		case 'D':
			c = smc_qdrive (fd, loader, &robot_info, drvord, verbose);
			break;
		case 'L':
			c = smc_qlib (fd, loader, &robot_info, verbose);
			break;
		case 'P':
			c = smc_qport (fd, loader, &robot_info, verbose);
			break;
		case 'S':
			c = smc_qslot (fd, loader, &robot_info, slotaddr, nbelem, verbose);
			break;
		case 'V':
			c = smc_qvid (fd, loader, &robot_info, vid, nbelem, verbose);
			break;
		}
	}
	exit (c);
}

smc_dismount (fd, loader, robot_info, drvord, vid)
int fd;
char *loader;
struct robot_info *robot_info;
int drvord;
char *vid;
{
        int c;
        struct smc_element_info element_info;
	char *msgaddr;
	struct smc_status smc_status;
 
	if ((c = smc_read_elem_status (fd, loader, 4, robot_info->device_start+drvord,
	    1, &element_info)) < 0) {
		c = smc_lasterror (&smc_status, &msgaddr);
		fprintf (stderr, SR020, "read_elem_status", msgaddr);
		return (c);
	}
	if (*vid && strcmp (element_info.name, vid)) {
		fprintf (stderr, SR009, vid, element_info.name);
		return (USERR);
	}
	if ((c = smc_move_medium (fd, loader, robot_info->device_start+drvord,
	    element_info.source_address, (element_info.flags & 0x40) ? 1 : 0)) < 0) {
		c = smc_lasterror (&smc_status, &msgaddr);
		fprintf (stderr, SR018, "demount", vid, drvord, msgaddr);
		return (c);
	}
	return (0);
}

smc_export (fd, loader, robot_info, vid)
int fd;
char *loader;
struct robot_info *robot_info;
char *vid;
{
        int c;
        struct smc_element_info element_info;
	int i;
        struct smc_element_info *impexp_info;
	char *msgaddr;
	int nbelem;
	struct smc_status smc_status;
 
	if ((c = smc_find_cartridge (fd, loader, vid, 0, 0, 1, &element_info)) < 0) {
		c = smc_lasterror (&smc_status, &msgaddr);
		fprintf (stderr, SR017, "find_cartridge", vid, msgaddr);
		return (c);
	}
	if (c == 0) {
		fprintf (stderr, SR017, "export", vid, "volume not in library");
		return (USERR);
	}
	if (element_info.element_type != 2) {
		fprintf (stderr, SR017, "export", vid, "volume in use");
		return (RBT_SLOW_RETRY);
	}
	/* look for a free export slot */

	nbelem = robot_info->port_count;
	if ((impexp_info = malloc (nbelem * sizeof(struct smc_element_info))) == NULL) {
		fprintf (stderr, SR012);
		return (USERR);
	}

	if ((c = smc_read_elem_status (fd, loader, 3, robot_info->port_start,
	    nbelem, impexp_info)) < 0) {
		c = smc_lasterror (&smc_status, &msgaddr);
		fprintf (stderr, SR020, "read_elem_status", msgaddr);
		free (impexp_info);
		return (c);
	}
	for (i = 0; i < nbelem; i++) {
		if (((impexp_info+i)->state & 0x1) == 0)	/* element free */
			break;
	}
	if (i >= nbelem) {	/* export slots are full */
		fprintf (stderr, SR013);
		free (impexp_info);
		return (USERR);
	}

	if ((c = smc_move_medium (fd, loader, element_info.element_address,
	    (impexp_info+i)->element_address, 0)) < 0) {
		c = smc_lasterror (&smc_status, &msgaddr);
		fprintf (stderr, SR017, "export", vid, msgaddr);
		free (impexp_info);
		return (c);
	}
	free (impexp_info);
	return (0);
}

smc_import (fd, loader, robot_info, vid)
int fd;
char *loader;
struct robot_info *robot_info;
char *vid;
{
        int c;
	int device_start;
        struct smc_element_info *element_info;
	int i, j;
	char *msgaddr;
	int nbelem;
	int port_start;
	int slot_start;
	struct smc_status smc_status;
 
	nbelem = robot_info->transport_count + robot_info->slot_count +
		 robot_info->port_count + robot_info->device_count;
	if ((element_info = malloc (nbelem * sizeof(struct smc_element_info))) == NULL) {
		fprintf (stderr, SR012);
		return (USERR);
	}

	/* get inventory */

	if ((c = smc_read_elem_status (fd, loader, 0, 0, nbelem, element_info)) < 0) {
		c = smc_lasterror (&smc_status, &msgaddr);
		fprintf (stderr, SR020, "read_elem_status", msgaddr);
		free (element_info);
		return (c);
	}
	for (i = 0; i < c; i++)
		if ((element_info+i)->element_type == 2) break;
	slot_start = i;
	for (i = 0; i < c; i++)
		if ((element_info+i)->element_type == 3) break;
	port_start = i;
	for (i = 0; i < c; i++)
		if ((element_info+i)->element_type == 4) break;
	device_start = i;

	/* mark home slots of cartridges currently on drives as non free */

	for (i = device_start; i < device_start+robot_info->device_count; i++) {
		if (((element_info+i)->state & 0x1) == 0) continue;
		for (j = slot_start; j < slot_start+robot_info->slot_count; j++)
			if ((element_info+i)->source_address ==
				(element_info+j)->element_address) break;
		(element_info+j)->state |= 1;
	}

	/* loop on all import slots */

	for (i = port_start; i < port_start+robot_info->port_count; i++) {
		if (*vid && strcmp (vid, (element_info+i)->name)) continue;
		if (*vid || (*vid == '\0' && ((element_info+i)->state & 2))) {

			/* find a free storage slot */

			for (j = slot_start; j < slot_start+robot_info->slot_count; j++)
				if (((element_info+j)->state & 0x1) == 0) break;
			if (j >= slot_start+robot_info->slot_count) {
				fprintf (stderr, SR015);
				free (element_info);
				return (USERR);
			}

			if ((c = smc_move_medium (fd, loader, (element_info+i)->element_address,
			    (element_info+j)->element_address, 0)) < 0) {
				c = smc_lasterror (&smc_status, &msgaddr);
				fprintf (stderr, SR017, "import",
				    (element_info+i)->name, msgaddr);
				free (element_info);
				return (c);
			}
			if (*vid || c) break;
			(element_info+j)->state |= 1;	/* dest slot is now full */
		}
	}
	free (element_info);
	return (c);
}

smc_mount (fd, loader, robot_info, drvord, vid, invert)
int fd;
char *loader;
struct robot_info *robot_info;
int drvord;
char *vid;
int invert;
{
        int c;
        struct smc_element_info element_info;
	char *msgaddr;
	struct smc_status smc_status;
 
	if ((c = smc_find_cartridge (fd, loader, vid, 0, 0, 1, &element_info)) < 0) {
		c = smc_lasterror (&smc_status, &msgaddr);
		fprintf (stderr, SR017, "find_cartridge", vid, msgaddr);
		return (c);
	}
	if (c == 0) {
		fprintf (stderr, SR018, "mount", vid, drvord, "volume not in library");
		return (RBT_OMSG_NORTRY);
	}
	if (element_info.element_type != 2) {
		fprintf (stderr, SR018, "mount", vid, drvord, "volume in use");
		return (RBT_OMSG_SLOW_R);
	}
	if ((c = smc_move_medium (fd, loader, element_info.element_address,
	    robot_info->device_start+drvord, invert)) < 0) {
		c = smc_lasterror (&smc_status, &msgaddr);
		fprintf (stderr, SR018, "mount", vid, drvord, msgaddr);
		return (c);
	}
	return (0);
}

smc_qdrive (fd, loader, robot_info, drvord, verbose)
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
	if ((c = smc_read_elem_status (fd, loader, 4, robot_info->device_start+drvord,
	    nbelem, element_info)) < 0) {
		c = smc_lasterror (&smc_status, &msgaddr);
		fprintf (stderr, SR020, "read_elem_status", msgaddr);
		free (element_info);
		return (c);
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

smc_qlib (fd, loader, robot_info, verbose)
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

smc_qport (fd, loader, robot_info, verbose)
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

	if ((c = smc_read_elem_status (fd, loader, 3, robot_info->port_start,
	    nbelem, element_info)) < 0) {
		c = smc_lasterror (&smc_status, &msgaddr);
fprintf(stderr, "rc=%d\n", smc_status.rc);
		fprintf (stderr, SR020, "read_elem_status", msgaddr);
		free (element_info);
		return (c);
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
 
smc_qslot (fd, loader, robot_info, slotaddr, nbelem, verbose)
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

	if ((c = smc_read_elem_status (fd, loader, 2, slotaddr, nbelem, element_info)) < 0) {
		c = smc_lasterror (&smc_status, &msgaddr);
		fprintf (stderr, SR020, "read_elem_status", msgaddr);
		free (element_info);
		return (c);
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

smc_qvid (fd, loader, robot_info, reqvid, nbelem, verbose)
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
		if (strchr (vid, '*'))	/* pattern matching */
			nbelem = robot_info->transport_count + robot_info->slot_count +
				 robot_info->port_count + robot_info->device_count;
		else
			nbelem = 1;
	}
	if ((element_info = malloc (nbelem * sizeof(struct smc_element_info))) == NULL) {
		fprintf (stderr, SR012);
		return (USERR);
	}

	if ((c = smc_find_cartridge (fd, loader, vid, 0, 0, nbelem, element_info)) < 0) {
		c = smc_lasterror (&smc_status, &msgaddr);
		fprintf (stderr, SR017, "find_cartridge", vid, msgaddr);
		free (element_info);
		return (c);
	}
	if (verbose)
		printf ("Vid\tElement Addr.\tElement Type\n");
	for (i = 0; i < c; i++) {
		ptype = ptypes[(element_info+i)->element_type];
		if ((element_info+i)->element_type == 3)
			if (((element_info+i)->state & 0x2) == 0)
				ptype = "export";
			else
				ptype = "import";
		printf ("%s\t    %4d\t%s\n",
			(element_info+i)->name, (element_info+i)->element_address,
			ptype);
	}
	free (element_info);
	return (0);
}

usage(cmd)
char *cmd;
{
	fprintf (stderr, "usage: %s ", cmd);
	fprintf (stderr, "%s%s%s%s%s%s%s%s",
	    "-d -D drive_ordinal -l loader [-V vid] [-v]\n",
	    "\t-e -l loader -V vid [-v]\n",
	    "\t-i -l loader [-V vid] [-v]\n",
	    "\t-m -D drive_ordinal -l loader -V vid [-v]\n",
	    "\t-q D [-D drive_ordinal] -l loader [-v]\n",
	    "\t-q L -l loader [-v]\n",
	    "\t-q S -l loader [-N nbelem] [-S starting_slot] [-v]\n",
	    "\t-q V -l loader [-V vid] [-v]\n");
}
