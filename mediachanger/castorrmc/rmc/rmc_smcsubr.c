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
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>

#include "getconfent.h"
#include "rbtsubr_constants.h"
#include "rmc_constants.h"
#include "rmc_logit.h"
#include "rmc_send_scsi_cmd.h"
#include "rmc_sendrep.h"
#include "rmc_smcsubr.h"
#include "rmc_smcsubr2.h"
#include "scsictl.h"
#include "serrno.h"
#include "smc_constants.h"
#include "spectra_like_libs.h"

#define	RBT_XTRA_PROC 10
static struct smc_status smc_status;
static const char *smc_msgaddr;

static void save_error(
	const int rc,
	const int nb_sense,
	const char *const sense,
	const char *const msgaddr)
{
	smc_msgaddr = msgaddr;
	smc_status.rc = rc;
	smc_status.skvalid = 0;
	smc_status.save_errno = serrno;
	if (rc == -4 && nb_sense >= 14) {
		smc_status.asc = sense[12];
		smc_status.ascq = sense[13];
		smc_status.sensekey = sense[2] &0xF;
		smc_status.skvalid = 1;
	} else {
		smc_status.asc = 0;
		smc_status.ascq = 0;
		smc_status.sensekey = 0;
	}
}

static int vmatch (const char *const pattern, const char *const vid)
{
	const char *p;
	const char *v;

	for (p = pattern, v = vid; *p; p++, v++) {
		if (*v == 0 && *p != '*')
			return (1);
		switch (*p) {
		case '?':	/* match any single character */
			continue;
		case '*':
			if (*(++p) == 0)
				return (0); /* trailing * matches the rest */
			while (vmatch (p, v)) {
				if (*(++v) == 0)
					return (1);
			}
			return (0);
		default:
			if (*p != *v)
				return (1);
		}
	}
	return (*v != 0);
}

static int get_element_size(
	const int fd,
	const char *const rbtdev,
	const int type)
{
	unsigned char buf[128];
	unsigned char cdb[12];
	const char *msgaddr;
	int nb_sense_ret;
	int rc;
	char sense[MAXSENSE];
	int voltag = 0x10;
        int pause_mode = 1;
        int nretries = 0;

 	memset (cdb, 0, sizeof(cdb));
 	cdb[0] = 0xB8;		/* read element status */
 	cdb[1] = voltag + type; /* we request volume tag info and this type */
 	cdb[5] = 1;             /* we only need one element */
 	cdb[9] = 128;           /* limit for the report */

        /* IBM library in pause mode  */
        while (pause_mode && nretries <= 900) {
 	     rc = rmc_send_scsi_cmd (fd, rbtdev, 0, cdb, 12, buf, 128,
 		  sense, 38, SCSI_IN, &nb_sense_ret, &msgaddr);
             if (rc < 0) {
               if (rc == -4 && nb_sense_ret >= 14 && (sense[12] == 0x04)  && (sense[13] == -0X7B )) {
                 sleep(60);
                 pause_mode = 1;
               } else  {
                 pause_mode = 0;
               }
             } else {
                pause_mode = 0;
             }
             nretries++;
        }

 	if (rc < 0) {
 		save_error (rc, nb_sense_ret, sense, msgaddr);
 		return (-1);
 	}
	return (buf[10] * 256 + buf[11]);
}

static int get_element_info(
	const char opcode,
	const int fd,
	const char *const rbtdev,
	const int type,
	const int start,
	const int nbelem,
	struct smc_element_info element_info[])
{
	int avail_elem;
	unsigned char cdb[12];
	unsigned char *data;
	int edl;
	int element_size;
	char func[16];
	int i;
	int len;
	const char *msgaddr;
	int nb_sense_ret;
	unsigned char *p;
	unsigned char *page_end, *page_start;
	unsigned char *q;
	int rc = 0;
	char sense[MAXSENSE];
        int pause_mode = 1;
        int nretries = 0;
	int nbReportBytesRemaining = 0;
	int nbElementsInReport = 0;

	strncpy (func, "get_elem_info", sizeof(func));
	func[sizeof(func) - 1] = '\0';
	if (type) {
		element_size = get_element_size (fd, rbtdev, type);
		if (element_size < 0) return (-1);
	} else {
		element_size = get_element_size (fd, rbtdev, 1); /* transport */
		if (element_size < 0) return (-1);
		i = get_element_size (fd, rbtdev, 2);	/* slot */
		if (i < 0) return (-1);
		if (i > element_size) element_size = i;
		i = get_element_size (fd, rbtdev, 3);	/* port */
		if (i < 0) return (-1);
		if (i > element_size) element_size = i;
		i = get_element_size (fd, rbtdev, 4);	/* device */
		if (i < 0) return (-1);
		if (i > element_size) element_size = i;
	}
	len = nbelem * element_size + 8;
	if (type != 0 || nbelem == 1)
		len += 8;	/* one element header */
	else
		len += 32;	/* possibly four element headers */
	data = (unsigned char *)malloc (len);
	memset (cdb, 0, sizeof(cdb));
	cdb[0] = opcode;	/* read element status or request volume element address */
	cdb[1] = 0x10 + type;
	cdb[2] = start >> 8;
	cdb[3] = start & 0xFF;
	cdb[4] = nbelem >> 8;
	cdb[5] = nbelem & 0xFF;
	cdb[7] = len >> 16;
	cdb[8] = (len >> 8) & 0xFF;
	cdb[9] = len & 0xFF;

        /* IBM library in pause mode  */
        while (pause_mode && nretries <= 900) {
	     rc = rmc_send_scsi_cmd (fd, rbtdev, 0, cdb, 12, data, len,
		  sense, 38, SCSI_IN, &nb_sense_ret, &msgaddr);
             if (rc < 0) {
               if (rc == -4 && nb_sense_ret >= 14 && (sense[12] == 0x04)  && (sense[13] == -0X7B )) {
                 sleep(60);
                 pause_mode = 1;
               } else  {
                 pause_mode = 0;
               }
             } else {
                pause_mode = 0;
             }
             nretries++;
        }

	if (rc < 0) {
		save_error (rc, nb_sense_ret, sense, msgaddr);
		free (data);
		return (-1);
	}
	avail_elem = *(data+2) * 256 + *(data+3);
	nbReportBytesRemaining = *(data+5) * 256 * 256 + *(data+6) * 256 + *(data+7);
	i = 0;
	p = data + 8;			/* point after data header */
	while (i < avail_elem && 0 < nbReportBytesRemaining) {
		nbReportBytesRemaining -= 8;
		edl = *(p+2) * 256 + *(p+3);
		page_start = p + 8;	/* point after page header */
		page_end = page_start +
			(((*(p+5) * 256 + *(p+6)) * 256) + *(p+7));
		if (page_end > (data + len)) page_end = data + len;
		for (p = page_start; p < page_end && i < avail_elem; p += edl, i++) {
			nbElementsInReport++;
			nbReportBytesRemaining -= edl;
			element_info[i].element_address = *p * 256 + *(p+1);
			element_info[i].element_type = *(page_start-8);
			element_info[i].state = *(p+2);
			element_info[i].asc = *(p+4);
			element_info[i].ascq = *(p+5);
			element_info[i].flags = *(p+9);
			element_info[i].source_address = *(p+10) * 256 + *(p+11);
			if ((*(page_start-7) & 0x80) == 0 ||
			    (*(p+12) == '\0') || (*(p+12) == ' '))
				element_info[i].name[0] = '\0';
			else {
				q = (unsigned char *) strchr ((char *)p+12, ' ');
				if (q) {
					strncpy (element_info[i].name, (char *)p+12, q-p-12);
					element_info[i].name[q-p-12] = '\0';
				} else
					strcpy (element_info[i].name, (char *)p+12);
				if (strlen (element_info[i].name) > CA_MAXVIDLEN)
					element_info[i].name[CA_MAXVIDLEN] = '\0';
			}
		}
	}
	free (data);
	return (nbElementsInReport);
}

int smc_get_geometry(
	const int fd,
	const char *const rbtdev,
        struct robot_info *const robot_info)
{
	unsigned char buf[36];
	unsigned char cdb[6];
	char func[16];
	const char *msgaddr;
	int nb_sense_ret;
	int rc;
	char sense[MAXSENSE];
        int pause_mode = 1;
        int nretries = 0;

	strncpy(func, "get_geometry", sizeof(func));
	func[sizeof(func) - 1] = '\0';

	memset (cdb, 0, sizeof(cdb));
	cdb[0] = 0x12;		/* inquiry */
	cdb[4] = 36;

        /* IBM library in pause mode  */
        while (pause_mode && nretries <= 900) {
	    rc = rmc_send_scsi_cmd (fd, rbtdev, 0, cdb, 6, buf, 36,
		sense, 38, SCSI_IN, &nb_sense_ret, &msgaddr);
            if (rc < 0) {
              if (rc == -4 && nb_sense_ret >= 14 && (sense[12] == 0x04)  && (sense[13] == -0X7B )) {
                 sleep(60);
                 pause_mode = 1;
              } else  {
                 pause_mode = 0;
              }
            } else {
                 pause_mode = 0;
            }
            nretries++;
        }

	if (rc < 0) {
		save_error (rc, nb_sense_ret, sense, msgaddr);
		return (-1);
	}
	memcpy (robot_info->inquiry, buf+8, 28);
	robot_info->inquiry[28] = '\0';
	memset (cdb, 0, sizeof(cdb));
	cdb[0] = 0x1A;		/* mode sense */
        cdb[1] = 0x08;          /* DBD bit - Disable block descriptors */
	cdb[2] = 0x1D;		/* element address assignment page */
	cdb[4] = 24;
        pause_mode = 1;
        nretries = 0;

        /* IBM library in pause mode  */
        while (pause_mode && nretries<=900) {
	     rc = rmc_send_scsi_cmd (fd, rbtdev, 0, cdb, 6, buf, 24,
		 sense, 38, SCSI_IN, &nb_sense_ret, &msgaddr);
             if (rc < 0) {
               if (rc == -4 && nb_sense_ret >= 14 && (sense[12] == 0x04)  && (sense[13] == -0X7B )) {
                 sleep(60);
                 pause_mode = 1;
               } else  {
                 pause_mode = 0;
               }
             } else {
                 pause_mode = 0;
             }
             nretries++;
        }

	if (rc < 0) {
		save_error (rc, nb_sense_ret, sense, msgaddr);
		return (-1);
	}
	robot_info->transport_start = buf[6] * 256 + buf[7];
	robot_info->transport_count = buf[8] * 256 + buf[9];
	robot_info->slot_start = buf[10] * 256 + buf[11];
	robot_info->slot_count = buf[12] * 256 + buf[13];
	robot_info->port_start = buf[14] * 256 + buf[15];
	robot_info->port_count = buf[16] * 256 + buf[17];
	robot_info->device_start = buf[18] * 256 + buf[19];
	robot_info->device_count = buf[20] * 256 + buf[21];
	return (0);
}

int smc_read_elem_status(
	const int fd,
	const char *const rbtdev,
	const int type,
	const int start,
	const int nbelem,
	struct smc_element_info element_info[])
{
	char func[16];

	strncpy(func, "read_elem_statu", sizeof(func));
	func[sizeof(func) - 1] = '\0';

	return get_element_info (0xB8, fd, rbtdev, type, start, nbelem, element_info);
}

int smc_find_cartridgeWithoutSendVolumeTag (
	const int fd,
	const char *const rbtdev,
	const char *const find_template,
	const int type,
	const int start,
	const int nbelem,
	struct smc_element_info element_info[])
{
	static char err_msgbuf[132];
	int nbFound = 0;
	char func[16];
	int i;
	struct smc_element_info *inventory_info;
	char *msgaddr;
	const int patternMatching = strchr (find_template, '*') || strchr (find_template, '?');
	struct robot_info robot_info;
	int tot_nbelem = 0;
	int nbElementsInReport = 0;

	strncpy(func, "findWithoutVT", sizeof(func));
	func[sizeof(func) - 1] = '\0';

	{
		const int smc_get_geometry_rc = smc_get_geometry (fd, rbtdev, &robot_info);
		if(smc_get_geometry_rc) {
			return smc_get_geometry_rc;
		}
	}

	tot_nbelem = robot_info.transport_count + robot_info.slot_count +
		robot_info.port_count + robot_info.device_count;

	if ((inventory_info = (struct smc_element_info *)malloc (tot_nbelem * sizeof(struct smc_element_info))) == NULL) {
		serrno = errno;
		sprintf (err_msgbuf, "malloc error: %s", strerror(errno));
		msgaddr = err_msgbuf;
		save_error (-1, 0, NULL, msgaddr);
		return (-1);
	}

	nbElementsInReport = smc_read_elem_status (fd, rbtdev, type, start, tot_nbelem, inventory_info);
	if(0 > nbElementsInReport) {
		free (inventory_info);
		return (nbElementsInReport);
	}
	for (i = 0 ; i < nbElementsInReport && nbFound < nbelem; i++) {
		if (inventory_info[i].state & 0x1) {
			if (patternMatching) {
				if (vmatch (find_template, inventory_info[i].name) == 0) {
					memcpy (&element_info[nbFound], &inventory_info[i],
						sizeof(struct smc_element_info));
					nbFound++;
				}
			} else {
				if (strcmp (find_template, inventory_info[i].name) == 0) {
					memcpy (element_info, &inventory_info[i],
						sizeof(struct smc_element_info));
					nbFound = 1;
					break;
				}
			}
		}
	}
	free (inventory_info);
	return (nbFound);
}


int smc_find_cartridge(
	const int fd,
	const char *const rbtdev,
	const char *const find_template,
	const int type,
	const int start,
	const int nbelem,
	struct smc_element_info element_info[],
	struct robot_info *const robot_info)
{
	unsigned char cdb[12];
	char func[16];
	const char *msgaddr;
	int nb_sense_ret;
	char plist[40];
	int rc;
	char sense[MAXSENSE];
        int pause_mode = 1;
        int nretries = 0;

	strncpy(func, "findWithVT", sizeof(func));
	func[sizeof(func) - 1] = '\0';

        /* Skip the 0xB6 cdb command if the tape library is Spectra like */
        if (is_library_spectra_like(robot_info)) {
          rc = smc_find_cartridgeWithoutSendVolumeTag (fd, rbtdev, find_template, type, start, nbelem,
                                    element_info);
          if (rc >= 0)
            return (rc);
          return (-1);
        }

	memset (cdb, 0, sizeof(cdb));
	cdb[0] = 0xB6;		/* send volume tag */
	cdb[1] = type;
	cdb[2] = start >> 8;
	cdb[3] = start & 0xFF;
	cdb[5] = 5;
	cdb[9] = 40;
	memset (plist, 0, sizeof(plist));
	strcpy (plist, find_template);

       /* IBM library in pause mode  */
        while (pause_mode && nretries <= 900) {
          rc = rmc_send_scsi_cmd (fd, rbtdev, 0, cdb, 12, (unsigned char*)plist, 40,
                           sense, 38, SCSI_OUT, &nb_sense_ret, &msgaddr);
            if (rc < 0) {
              if (rc == -4 && nb_sense_ret >= 14 && (sense[12] == 0x04)  && (sense[13] == -0X7B )) {
                 sleep(60);
                 pause_mode = 1;
              } else  {
                 pause_mode = 0;
              }
            } else {
                pause_mode = 0;
            }
            nretries++;
        }

	if (rc < 0) {
                save_error (rc, nb_sense_ret, sense, msgaddr);
		if (rc == -4 && nb_sense_ret >= 14 && (sense[2] & 0xF) == 5) {
			rc = smc_find_cartridgeWithoutSendVolumeTag (fd, rbtdev, find_template, type,
			    start, nbelem, element_info);
			if (rc >= 0)
				return (rc);
		}
		return (-1);
	}
	return get_element_info (0xB5, fd, rbtdev, type, start, nbelem, element_info);
}


/* SCSI 3 additional sense code and additional sense qualifier */
struct scsierr_codact {
	unsigned char sensekey;
	unsigned char asc;
	unsigned char ascq;
	short action;
	const char *txt;
};
static struct scsierr_codact scsierr_acttbl[] = {
    {0x02, 0x04, 0x00, RBT_FAST_RETRY, "Logical Unit Not Ready, Cause Not Reportable"},
    {0x02, 0x04, 0x01, RBT_FAST_RETRY, "Logical Unit Is In Process of Becoming Ready"},
    {0x02, 0x04, 0x02, RBT_NORETRY, "Logical Unit Not Ready, initialization required"},
    {0x02, 0x04, 0x03, RBT_NORETRY, "Logical Unit Not Ready, Manual Intervention Required"},
    {0x0B, 0x08, 0x00, RBT_NORETRY, "Logical Unit Communication Failure"},
    {0x0B, 0x08, 0x01, RBT_NORETRY, "Logical Unit Communication Time-out"},
    {0x05, 0x1A, 0x00, RBT_NORETRY, "Parameter List Length Error"},
    {0x05, 0x20, 0x00, RBT_NORETRY, "Invalid Command Operation Code"},
    {0x05, 0x21, 0x01, RBT_NORETRY, "Invalid Element Address"},
    {0x05, 0x24, 0x00, RBT_NORETRY, "Invalid field in CDB"},
    {0x05, 0x25, 0x00, RBT_NORETRY, "Logical Unit Not Supported"},
    {0x05, 0x26, 0x00, RBT_NORETRY, "Invalid field in Parameter List"},
    {0x05, 0x26, 0x01, RBT_NORETRY, "Parameter Not Supported"},
    {0x05, 0x26, 0x02, RBT_NORETRY, "Parameter Value Invalid"},
    {0x06, 0x28, 0x00, RBT_FAST_RETRY, "Not Ready to Ready Transition"},
    {0x06, 0x28, 0x01, RBT_FAST_RETRY, "Import or Export Element Accessed"},
    {0x06, 0x29, 0x00, RBT_FAST_RETRY, "Power On, Reset, or Bus Device Reset Occurred"},
    {0x06, 0x2A, 0x01, RBT_FAST_RETRY, "Mode Parameters Changed"},
    {0x05, 0x30, 0x00, RBT_NORETRY, "Incompatible Medium Installed"},
    {0x00, 0x30, 0x03, RBT_FAST_RETRY, "Cleaning Cartridge Installed"},
    {0x05, 0x39, 0x00, RBT_NORETRY, "Saving Parameters Not Supported"},
    {0x05, 0x3A, 0x00, RBT_XTRA_PROC, "Medium Not Present"},
    {0x05, 0x3B, 0x0D, RBT_XTRA_PROC, "Medium Destination Element Full"},
    {0x05, 0x3B, 0x0E, RBT_XTRA_PROC, "Medium Source Element Empty"},
    {0x04, 0x40, 0x01, RBT_NORETRY, "Hardware Error, General"},
    {0x04, 0x40, 0x02, RBT_NORETRY, "Hardware Error, Tape Transport"},
    {0x04, 0x40, 0x03, RBT_NORETRY, "Hardware Error, CAP"},
    {0x0B, 0x43, 0x00, RBT_NORETRY, "Message Error"},
    {0x02, 0x44, 0x00, RBT_NORETRY, "Internal Target Failure"},
    {0x0B, 0x45, 0x00, RBT_NORETRY, "Select or Reselect Failure"},
    {0x0B, 0x47, 0x00, RBT_NORETRY, "SCSI Parity Error"},
    {0x0B, 0x48, 0x00, RBT_NORETRY, "Initiator Detected Error"},
    {0x02, 0x4C, 0x00, RBT_NORETRY, "Logical Unit Failed Self-Configuration"},
    {0x05, 0x4E, 0x00, RBT_NORETRY, "Overlapped Commands Attempted"},
    {0x05, 0x53, 0x02, RBT_NORETRY, "Medium Removal Prevented"},
    {0x06, 0x54, 0x00, RBT_NORETRY, "SCSI To Host System Interface Failure"},
    {0x02, 0x5A, 0x01, RBT_NORETRY, "Operator Medium Removal Request"}
};

static const char* action_to_str(const short action) {
	switch(action) {
	case RBT_FAST_RETRY: return "RBT_FAST_RETRY";
	case RBT_NORETRY   : return "RBT_NORETRY";
	case RBT_XTRA_PROC : return "RBT_XTRA_PROC";
	default            : return "UNKNOWN";
	}
}

int smc_lasterror(
	struct smc_status *const smc_stat,
	const char **const msgaddr)
{
	unsigned int i;
	char func[16];

	strncpy (func, "lasterror", sizeof(func));
	func[sizeof(func) - 1] = '\0';

	rmc_logit(func, "Function entered:"
		" asc=%d ascq=%d save_errno=%d rc=%d sensekey=%d skvalid=%d\n",
		smc_status.asc, smc_status.ascq, smc_status.save_errno,
		smc_status.rc, smc_status.sensekey, smc_status.skvalid);

	smc_stat->rc = smc_status.rc;
	smc_stat->skvalid = smc_status.skvalid;
	*msgaddr = smc_msgaddr;
	if ((smc_status.rc == -1 || smc_status.rc == -2) &&
	    smc_status.save_errno == EBUSY)
		return (EBUSY);
	if (! smc_status.skvalid)
		return (RBT_NORETRY);
	smc_stat->sensekey = smc_status.sensekey;
	smc_stat->asc = smc_status.asc;
	smc_stat->ascq = smc_status.ascq;
	for (i = 0; i < sizeof(scsierr_acttbl)/sizeof(struct scsierr_codact); i++) {
		if (smc_status.asc == scsierr_acttbl[i].asc &&
		    smc_status.ascq == scsierr_acttbl[i].ascq &&
		    smc_status.sensekey == scsierr_acttbl[i].sensekey) {
			const char *const action_str =
				action_to_str(scsierr_acttbl[i].action);
			*msgaddr = scsierr_acttbl[i].txt;

			rmc_logit(func, "Entry found in scsierr_acttbl:"
				" action_str=%s\n", action_str);

			return (scsierr_acttbl[i].action);
		}
	}

	rmc_logit(func, "No matching entry in scsierr_acttbl\n");

	return (RBT_NORETRY);
}

int smc_move_medium(
	const int fd,
	const char *const rbtdev,
	const int from,
	const int to,
	const int invert)
{
	unsigned char cdb[12];
	char func[16];
	const char *msgaddr;
	int nb_sense_ret;
	int rc;
	char sense[MAXSENSE];
        int pause_mode = 1;
        int nretries = 0;

	strncpy(func, "move_medium", sizeof(func));
	func[sizeof(func) - 1] = '\0';

	memset (cdb, 0, sizeof(cdb));
	cdb[0] = 0xA5;		/* move medium */
	cdb[4] = from >> 8;
	cdb[5] = from & 0xFF;
	cdb[6] = to >> 8;
	cdb[7] = to & 0xFF;
	cdb[10] = invert;

        while (pause_mode) {
	     rc = rmc_send_scsi_cmd (fd, rbtdev, 0, cdb, 12, NULL, 0,
		sense, 38, SCSI_NONE, &nb_sense_ret, &msgaddr);
            if (rc < 0) {
              if (rc == -4 && nb_sense_ret >= 14 && (sense[12] == 0x04)  && (sense[13] == -0X7B )) {
                 sleep(60);
                 pause_mode = 1;
              } else  {
                 pause_mode = 0;
              }
            } else {
                 pause_mode = 0;
            }
            nretries++;
        }

	if (rc < 0) {
		save_error (rc, nb_sense_ret, sense, msgaddr);
		return (-1);
	}
	return (0);
}

static int rmc_usrmsg(
	const int rpfd,
	const char *func,
	const char *const msg,
	...)
{
	va_list args;
	char prtbuf[RMC_PRTBUFSZ];
	const int save_errno = errno;

	va_start (args, msg);
	snprintf (prtbuf, sizeof(prtbuf), "%s: ", func);
	prtbuf[sizeof(prtbuf) - 1] = '\0';
	{
		const size_t nbBytesUsed = strlen (prtbuf);

		/* If there is still space in the print buffer */
		if(nbBytesUsed < (sizeof(prtbuf))) {
			const size_t nbBytesRemaining = sizeof(prtbuf) -
				nbBytesUsed;
			char *const p = prtbuf + nbBytesUsed;
			vsnprintf (p, nbBytesRemaining, msg, args);
			prtbuf[sizeof(prtbuf) - 1] = '\0';
		}
	}
	rmc_sendrep (rpfd, MSG_ERR, "%s", prtbuf);
	va_end (args);
	errno = save_errno;
	return (0);
}

int smc_dismount (
	const int rpfd,
	const int fd,
	const char *const loader,
	struct robot_info *const robot_info,
	const int drvord,
	const char *const vid)
{
	const unsigned int max_element_status_reads = 20;
	const unsigned int dismount_status_read_delay = 1; /* In seconds */
	unsigned int nb_element_status_reads = 0;
	int drive_not_unloaded = 1;
	struct smc_element_info drive_element_info;
	char func[16];
	const char *msgaddr = 0;
	struct smc_status smc_status;

	strncpy (func, "smc_dismount", sizeof(func));
	func[sizeof(func) - 1] = '\0';

	memset(&smc_status, '\0', sizeof(smc_status));

	/* IBM libraries sometimes disagree with the eject of their drives. */
	/* Sometimes the access bit of the result of Read Element Status    */
	/* (XB8) indicates the gripper cannot access the tape even though   */
	/* the eject was successful.  Reading the element status at a later */
	/* point in time eventually indicates the tape is accessible.       */
	while(drive_not_unloaded && nb_element_status_reads < max_element_status_reads) {
		if (0 > smc_read_elem_status (fd, loader, 4, robot_info->device_start+drvord,
		    	1, &drive_element_info)) {
			const int smc_error = smc_lasterror (&smc_status, &msgaddr);
			rmc_usrmsg ( rpfd, func, SR020, "read_elem_status", msgaddr);
			return (smc_error);
		}
		if (0 == (drive_element_info.state & 0x1)) {
			rmc_usrmsg ( rpfd, func, SR018, "demount", vid, drvord, "Medium Not Present");
			return (RBT_OK);
		}

		drive_not_unloaded = (0 == (drive_element_info.state & 0x8));
		if (drive_not_unloaded) {
			rmc_usrmsg ( rpfd, func, "read_elem_status of %s on drive %d detected Drive Not Unloaded\n", vid, drvord);
		}

		nb_element_status_reads++;

		if(nb_element_status_reads < max_element_status_reads) {
			sleep(dismount_status_read_delay);
		}
	}
	if(drive_not_unloaded) {
		rmc_usrmsg ( rpfd, func, SR018, "demount", vid, drvord, "Drive Not Unloaded");
		return (RBT_UNLD_DMNT);
	}

	if (*vid && strcmp (drive_element_info.name, vid)) {
		rmc_usrmsg ( rpfd, func, SR009, vid, drive_element_info.name);
		return (RBT_NORETRY);
	}
	if (0 > smc_move_medium (fd, loader, robot_info->device_start+drvord,
	    drive_element_info.source_address, (drive_element_info.flags & 0x40) ? 1 : 0)) {
		const int smc_error = smc_lasterror (&smc_status, &msgaddr);
		rmc_usrmsg ( rpfd, func, SR018, "demount", vid, drvord, msgaddr);
		return (smc_error);
	}
    /* check that the vid is in a slot before returning */
    while (1) {
          struct smc_element_info vol_element_info;
          if (0 > smc_find_cartridge (fd, loader, drive_element_info.name, 0, 0, 1, &vol_element_info, robot_info)) {
              const int smc_error = smc_lasterror (&smc_status, &msgaddr);
              rmc_usrmsg ( rpfd, func, SR017, "find_cartridge", drive_element_info.name, msgaddr);
              return (smc_error);
          }

          /* vid is in a storage slot */
          if (vol_element_info.element_type == 2) break;
          /* give time for the tape enter the slot */
          sleep (2);
    }

	return (0);
}

int smc_export (
	const int rpfd,
	const int fd,
	const char *const loader,
	struct robot_info *const robot_info,
	const char *const vid)
{
        struct smc_element_info element_info;
	char func[16];
	int i = 0;
        struct smc_element_info *impexp_info;
	const char *msgaddr = NULL;
	int nbelem = 0;
	struct smc_status smc_status;

	strncpy (func, "smc_export", sizeof(func));
	func[sizeof(func) - 1] = '\0';

	{
		const int smc_find_cartridge_rc = smc_find_cartridge (fd, loader, vid, 0, 0, 1, &element_info, robot_info);
		if (0 > smc_find_cartridge_rc) {
			const int smc_lasterror_rc = smc_lasterror (&smc_status, &msgaddr);
			rmc_usrmsg ( rpfd, func, SR017, "find_cartridge", vid, msgaddr);
			return (smc_lasterror_rc);
		}
		if (0 == smc_find_cartridge_rc) {
			rmc_usrmsg ( rpfd, func, SR017, "export", vid, "volume not in library");
			return (RBT_NORETRY);
		}
	}
	if (element_info.element_type != 2) {
		rmc_usrmsg ( rpfd, func, SR017, "export", vid, "volume in use");
		return (RBT_SLOW_RETRY);
	}
	/* look for a free export slot */

	nbelem = robot_info->port_count;
	if ((impexp_info = (struct smc_element_info *)malloc (nbelem * sizeof(struct smc_element_info))) == NULL) {
		rmc_usrmsg ( rpfd, func, SR012);
		return (RBT_NORETRY);
	}

	{
		int foundAFreeExportSlot = 0;
		const int nbElementsInReport = smc_read_elem_status (fd, loader, 3, robot_info->port_start,
	    		nbelem, impexp_info);
		if (0 > nbElementsInReport) {
			const int smc_lasterror_rc = smc_lasterror (&smc_status, &msgaddr);
			rmc_usrmsg ( rpfd, func, SR020, "read_elem_status", msgaddr);
			free (impexp_info);
			return (smc_lasterror_rc);
		}
		for (i = 0; i < nbElementsInReport; i++) {
			if (((impexp_info+i)->state & 0x1) == 0) {
				foundAFreeExportSlot = 1;
				break;
			}
		}
		if (!foundAFreeExportSlot) {
			rmc_usrmsg ( rpfd, func, SR013);
			free (impexp_info);
			return (RBT_NORETRY);
		}
	}

	{
		const int smc_move_medium_rc = smc_move_medium (fd, loader, element_info.element_address,
	    		(impexp_info+i)->element_address, 0);
		if (0 > smc_move_medium_rc) {
			const int smc_lasterror_rc = smc_lasterror (&smc_status, &msgaddr);
			rmc_usrmsg ( rpfd, func, SR017, "export", vid, msgaddr);
			free (impexp_info);
			return (smc_lasterror_rc);
		}
	}

	free (impexp_info);
	return (0);
}

int smc_import (
	const int rpfd,
	const int fd,
	const char *const loader,
	struct robot_info *const robot_info,
	const char *const vid)
{
        int c;
	int device_start;
        struct smc_element_info *element_info;
	char func[16];
	int i, j;
	const char *msgaddr;
	int nbelem;
	int port_start;
	int slot_start;
	struct smc_status smc_status;

	strncpy (func, "smc_import", sizeof(func));
	func[sizeof(func) - 1] = '\0';

	nbelem = robot_info->transport_count + robot_info->slot_count +
		 robot_info->port_count + robot_info->device_count;
	if ((element_info = (struct smc_element_info *)malloc (nbelem * sizeof(struct smc_element_info))) == NULL) {
		rmc_usrmsg ( rpfd, func, SR012);
		return (RBT_NORETRY);
	}

	/* get inventory */

	if ((c = smc_read_elem_status (fd, loader, 0, 0, nbelem, element_info)) < 0) {
		c = smc_lasterror (&smc_status, &msgaddr);
		rmc_usrmsg ( rpfd, func, SR020, "read_elem_status", msgaddr);
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
				rmc_usrmsg ( rpfd, func, SR015);
				free (element_info);
				return (RBT_NORETRY);
			}

			if ((c = smc_move_medium (fd, loader, (element_info+i)->element_address,
			    (element_info+j)->element_address, 0)) < 0) {
				c = smc_lasterror (&smc_status, &msgaddr);
				rmc_usrmsg ( rpfd, func, SR017, "import",
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

int smc_mount (
	const int rpfd,
	const int fd,
	const char *const loader,
	struct robot_info *const robot_info,
	const int drvord,
	const char *const vid,
	const int invert)
{
    int c;
    struct smc_element_info element_info;
	char func[16];
	const char *msgaddr;
	struct smc_status smc_status;

	strncpy (func, "smc_mount", sizeof(func));
	func[sizeof(func) - 1] = '\0';

	if ((c = smc_find_cartridge (fd, loader, vid, 0, 0, 1, &element_info, robot_info)) < 0) {
		c = smc_lasterror (&smc_status, &msgaddr);
		rmc_usrmsg ( rpfd, func, SR017, "find_cartridge", vid, msgaddr);
		return (c);
	}
	if (c == 0) {
		rmc_usrmsg ( rpfd, func, SR018, "mount", vid, drvord, "volume not in library");
		return (RBT_NORETRY);
	}
	if (element_info.element_type != 2) {

                /* compare requested and replied vid   */
                rmc_usrmsg ( rpfd, func, "Asked for %s, got reply for %s\n",
                        vid, element_info.name );

                /* detail on a tape's current location */
                switch (element_info.element_type) {

                case 1:
                        rmc_usrmsg ( rpfd, func, "Location: medium transport element (0x%x)\n",
                                element_info.element_type );
                        break;
                case 2:
                        /* normal case: in its home slot, not possible inside the if */
                        break;
                case 3:
                        rmc_usrmsg ( rpfd, func, "Location: import/export element (0x%x)\n",
                                element_info.element_type );
                        break;
                case 4:
                        rmc_usrmsg ( rpfd, func, "Location: data transfer element (0x%x)\n",
                                element_info.element_type );
                        break;
                default:
                        rmc_usrmsg ( rpfd, func, "Location: unknown (0x%x)\n",
                                element_info.element_type );
                }

                rmc_usrmsg ( rpfd, func, SR018, "mount", vid, drvord, "volume in use");
		return (RBT_SLOW_RETRY);
	}
	if ((c = smc_move_medium (fd, loader, element_info.element_address,
	    robot_info->device_start+drvord, invert)) < 0) {
		c = smc_lasterror (&smc_status, &msgaddr);
		rmc_usrmsg ( rpfd, func, SR018, "mount", vid, drvord, msgaddr);
		return (c);
	}
	return (0);
}
