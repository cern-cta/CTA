/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 1998-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "mediachanger/castorrmc/h/rbtsubr_constants.h"
#include "mediachanger/castorrmc/h/rmc_api.h"
#include "mediachanger/castorrmc/h/serrno.h"
#include "mediachanger/castorrmc/h/smc_constants.h"
#include "mediachanger/castorrmc/h/getconfent.h"
#include "getopt.h"
#include "mediachanger/castorrmc/h/spectra_like_libs.h"

#include <ctype.h>
			/* exit codes */

#define	USERR	1
#define TEXT_RED    "\x1b[31;1m"
#define TEXT_NORMAL "\x1b[0m"

extern char *optarg;

static void smc_str_upper(char *const s) {
	char *c = NULL;

	for(c = s; *c; c++) {
		*c = toupper(*c);
	}
}

static void smc_usage(const char *const cmd)
{
	fprintf (stderr, "Usage:\n");
	fprintf (stderr,
	    "  %s -d -D drive_ordinal [-V vid]\n"
	    "  %s -e -V vid\n"
	    "  %s -i [-V vid]\n"
	    "  %s -m -D drive_ordinal -V vid\n"
	    "  %s -q D [-D drive_ordinal] [-j]\n"
	    "  %s -q L [-j]\n"
            "  %s -q P [-j]\n"
	    "  %s -q S [-N nbelem] [-S starting_slot] [-j]\n"
	    "  %s -q V [-N nbelem] [-V vid] [-j]\n",
            cmd, cmd, cmd, cmd, cmd, cmd, cmd, cmd, cmd);
}

void smc_qdrive_humanPrint(const struct robot_info *const robot_info,
  const struct smc_element_info *const element_info, const int numberOfElements,
  const int useSpectraLib) {
  char *pstatus;
  int i;
  printf (TEXT_RED "Drive Ordinal\tElement Addr.\t  Status     Vid" TEXT_NORMAL "\n");
  for (i = 0; i < numberOfElements; i++) {
    if (((element_info+i)->state & 0x1) == 0)
            pstatus = "free";
    else if ((element_info+i)->state & 0x4)
            pstatus = "error";
    else if ((element_info+i)->state & 0x8 && !useSpectraLib)
            pstatus = "unloaded";
    else
            pstatus = "loaded";
    printf ("%13d\t%13d\t%8s  %s\n",
            (element_info+i)->element_address-robot_info->device_start,
            (element_info+i)->element_address, pstatus,
            (element_info+i)->name);
  }
}
void smc_qdrive_jsonPrint(const struct robot_info *const robot_info,
  const struct smc_element_info *const element_info, const int numberOfElements,
  const int useSpectraLib) {
  char *pstatus;
  int i;
  printf ("[");
  for (i = 0; i < numberOfElements; i++) {
    if (((element_info+i)->state & 0x1) == 0)
            pstatus = "free";
    else if ((element_info+i)->state & 0x4)
            pstatus = "error";
    else if ((element_info+i)->state & 0x8 && !useSpectraLib)
            pstatus = "unloaded";
    else
            pstatus = "loaded";
    if (0 != i) {
      printf(",");
    }
    printf ("{\"driveOrdinal\":%d,"
            "\"elementAddress\":%d,"
            "\"status\":\"%s\","
            "\"vid\":\"%s\"}",
            (element_info+i)->element_address-robot_info->device_start,
            (element_info+i)->element_address,
             pstatus,
            (element_info+i)->name);
  }
  printf ("]");
}
static int smc_qdrive (
	const char *const rmchost,
	const int fd,
	const struct robot_info *const robot_info,
	int drvord,
        const int isJsonEnabled)
{
        int c;
        struct smc_element_info *element_info;
	int nbelem;
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
        if (is_library_spectra_like(robot_info)) {
          useSpectraLib = 1;
        }
        if (isJsonEnabled) {
          smc_qdrive_jsonPrint(robot_info, element_info, c, useSpectraLib);
	} else {
          smc_qdrive_humanPrint(robot_info, element_info, c, useSpectraLib);
        }
	free (element_info);
	return (0);
}

void smc_qlib_humanPrint(const struct robot_info *const robot_info) {
  printf ("Vendor/Product/Revision = <%s>\n", robot_info->inquiry);
  printf ("Transport Count = %d, Start = %d\n",
          robot_info->transport_count, robot_info->transport_start);
  printf ("Slot Count = %d, Start = %d\n",
          robot_info->slot_count, robot_info->slot_start);
  printf ("Port Count = %d, Start = %d\n",
          robot_info->port_count, robot_info->port_start);
  printf ("Device Count = %d, Start = %d\n",
          robot_info->device_count, robot_info->device_start);
}

void smc_qlib_jsonPrint(const struct robot_info *const robot_info) {
  char T10Vendor[9];
  char prodId[17];
  char prodRevLvl[5];
  memcpy (T10Vendor, robot_info->inquiry, 8);
  T10Vendor[8] = '\0';
  memcpy(prodId, robot_info->inquiry + 8, 16);
  prodId[16] = '\0';
  memcpy(prodRevLvl,robot_info->inquiry + 8 + 16, 4);
  prodRevLvl[4] = '\0';
  printf ("[");
  printf ("{\"inquiry\":{\"vendor\":\"%s\",\"product\":\"%s\",\"revision\":\"%s\"},",
          T10Vendor, prodId, prodRevLvl);
  printf ("\"transport\":{\"count\":%d,\"start\":%d},",
          robot_info->transport_count, robot_info->transport_start);
  printf ("\"slot\":{\"count\":%d,\"start\":%d},",
          robot_info->slot_count, robot_info->slot_start);
  printf ("\"port\":{\"count\":%d,\"start\":%d},",
          robot_info->port_count, robot_info->port_start);
  printf ("\"device\":{\"count\":%d,\"start\":%d}",
          robot_info->device_count, robot_info->device_start);
  printf ("}]");
}

static int smc_qlib (const struct robot_info *const robot_info,
  const int isJsonEnabled)
{
        if (isJsonEnabled) {
          smc_qlib_jsonPrint(robot_info);
	} else {
          smc_qlib_humanPrint(robot_info);
        }
	return (0);
}

void smc_qport_humanPrint(const struct smc_element_info *const element_info,
  const int numberOfElements) {
  char *pstatus;
  int i;
  printf (TEXT_RED "Element Addr.\tVid\tImpExp" TEXT_NORMAL "\n");
  for (i = 0; i < numberOfElements; i++) {
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
}

void smc_qport_jsonPrint(const struct smc_element_info *const element_info,
  const int numberOfElements) {
  char *pstatus;
  int i;
  printf ("[");
  for (i = 0; i < numberOfElements; i++) {
    if (((element_info+i)->state & 0x1) == 0)
            pstatus = "";
    else if (((element_info+i)->state & 0x2) == 0)
            pstatus = "export";
    else
            pstatus = "import";
    if (0 != i) {
      printf(",");
    }
    printf ("{\"elementAddress\":%d,"
            "\"vid\":\"%s\","
            "\"state\":\"%s\"}",
            (element_info+i)->element_address,
            (element_info+i)->name,
             pstatus);
  }
  printf ("]");
}

static int smc_qport (
	const char *const rmchost,
	const int fd,
	const struct robot_info *const robot_info,
        const int isJsonEnabled)
{
	int c;
	struct smc_element_info *element_info;
	int nbelem;

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
	if (isJsonEnabled) {
          smc_qport_jsonPrint(element_info, c);
	} else {
          smc_qport_humanPrint(element_info, c);
	}
	free (element_info);
	return (0);
}

void smc_qslot_humanPrint(const struct smc_element_info *element_info,
  const int numberOfElements) {
  int i;
  printf (TEXT_RED "Element Addr.\tVid" TEXT_NORMAL "\n");
  for (i = 0; i < numberOfElements; i++) {
    printf ("    %4d\t%s\n",
            element_info[i].element_address, element_info[i].name);
  }
}

void smc_qslot_jsonPrint(const struct smc_element_info *element_info,
  const int numberOfElements) {
  int i;
  printf ("[");
  for (i = 0; i < numberOfElements; i++) {
    if (0 != i) {
      printf(",");
    }
    printf ("{\"elementAddress\":%4d,"
            "\"vid\":\"%s\"}",
            element_info[i].element_address,
            element_info[i].name);
  }
  printf ("]");
}

static int smc_qslot (
	const char *const rmchost,
	const int fd,
	const struct robot_info *robot_info,
	int slotaddr,
	int nbelem,
        const int isJsonEnabled)
{
        int c;
        struct smc_element_info *element_info;

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
        if (isJsonEnabled) {
          smc_qslot_jsonPrint(element_info, c);
	} else {
          smc_qslot_humanPrint(element_info, c);
        }
	free (element_info);
	return (0);
}

void smc_qvid_humanPrint(const struct smc_element_info *const element_info,
  const int numberOfElements){
  int i;
  char *ptype;
  char ptypes[5][6] = {"", "hand", "slot", "port", "drive"};
  printf (TEXT_RED "Vid\tElement Addr.\tElement Type" TEXT_NORMAL "\n");
  for (i = 0; i < numberOfElements; i++) {
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
}

void smc_qvid_jsonPrint(const struct smc_element_info *const element_info,
  const int numberOfElements){
  int i;
  char *ptype;
  char ptypes[5][6] = {"", "hand", "slot", "port", "drive"};
  printf ("[");
  for (i = 0; i < numberOfElements; i++) {
    ptype = ptypes[(element_info+i)->element_type];
    if ((element_info+i)->element_type == 3) {
      if (((element_info+i)->state & 0x2) == 0) {
        ptype = "export";
      } else {
        ptype = "import";
      }
    }
    if (0 != i) {
      printf(",");
    }
    printf ("{\"vid\":\"%s\","
            "\"elementAddress\":%d,"
            "\"elementType\":\"%s\"}",
            (element_info+i)->name,
            (element_info+i)->element_address,
            ptype);
  }
  printf ("]");
}

static int smc_qvid (
	const char *const rmchost,
	const int fd,
	const struct robot_info *const robot_info,
	const char *reqvid,
	int nbelem,
        const int isJsonEnabled)
{
        int c;
        struct smc_element_info *element_info;
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
         if (isJsonEnabled) {
           smc_qvid_jsonPrint(element_info, c);
	} else {
           smc_qvid_humanPrint(element_info, c);
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
	const int invert = 0;
	int n;
	int nbelem = 0;
	char qry_type = 0;
	char req_type = 0;
	struct robot_info robot_info;
	const char *rmchost = "localhost";
	int slotaddr = -1;
	char vid[7];
        int isJsonEnabled = 0;

	/* parse and check command options */
        struct option longopts [] = {
          {"drive",  required_argument, NULL, 'D'},
          {"dismount",     no_argument, NULL, 'd'},
          {"export",       no_argument, NULL, 'e'},
          {"import",       no_argument, NULL, 'i'},
          {"mount",        no_argument, NULL, 'm'},
          {"nbelem", required_argument, NULL, 'N'},
          {"query",  required_argument, NULL, 'q'},
          {"slot",   required_argument, NULL, 'S'},
          {"vid",    required_argument, NULL, 'V'},
          {"json",         no_argument, NULL, 'j'},
          {NULL,                     0, NULL,   0}
        };
	memset (vid, '\0', sizeof(vid));
	while ((c = getopt_long(argc, argv, "D:deimN:q:S:V:j", longopts, NULL)) != EOF) {
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
		case 'i':	/* import */
			if (req_type) {
				fprintf (stderr, SR002, req_type, c);
				errflg++;
			} else
				req_type = c;
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
				smc_str_upper (vid);
			}
			break;
                case 'j':
                        isJsonEnabled = 1;
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
		smc_usage (argv[0]);
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
                            isJsonEnabled);
			break;
		case 'L':
			c = smc_qlib (&robot_info, isJsonEnabled);
			break;
		case 'P':
			c = smc_qport (rmchost, fd, &robot_info, isJsonEnabled);
			break;
		case 'S':
			c = smc_qslot (rmchost, fd, &robot_info, slotaddr,
			    nbelem, isJsonEnabled);
			break;
		case 'V':
			c = smc_qvid (rmchost, fd, &robot_info, vid,
			    nbelem, isJsonEnabled);
			break;
		}
		break;

	}
	exit (c);
}
