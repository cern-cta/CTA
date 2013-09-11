/*
 * Copyright (C) 1998-2003 by CERN/IT/PDP/DM
 * All rights reserved
 */
 

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include "Ctape.h"
#include "scsictl.h"
#include "serrno.h"
#include "smc.h"
#include "sendscsicmd.h"
#include "getconfent.h"

#define	RBT_XTRA_PROC 10
static struct smc_status smc_status;
static char *smc_msgaddr;

static void
save_error(rc, nb_sense, sense, msgaddr)
int rc;
int nb_sense;
char *sense;
char *msgaddr;
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

static int
vmatch (char *pattern, char *vid)
{
	char *p;
	char *v;

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

static int
get_element_size(int fd,
                            char *rbtdev,
                            int type)
{
	unsigned char buf[128];
	unsigned char cdb[12];
	char *msgaddr;
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
 	     rc = send_scsi_cmd (fd, rbtdev, 0, cdb, 12, buf, 128,
 		  sense, 38, 900000, SCSI_IN, &nb_sense_ret, &msgaddr);
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

static int
get_element_info(char opcode,
                            int fd,
                            char *rbtdev,
                            int type,
                            int start,
                            int nbelem,
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
	char *msgaddr;
	int nb_sense_ret;
	unsigned char *p;
	unsigned char *page_end, *page_start;
	unsigned char *q;
	int rc;
	char sense[MAXSENSE];
        int pause_mode = 1;
        int nretries = 0;

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
	data = malloc (len);
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
	     rc = send_scsi_cmd (fd, rbtdev, 0, cdb, 12, data, len,
		  sense, 38, 900000, SCSI_IN, &nb_sense_ret, &msgaddr);
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
	i = 0;
	p = data + 8;			/* point after data header */
	while (i < avail_elem) {
		edl = *(p+2) * 256 + *(p+3);
		page_start = p + 8;	/* point after page header */
		page_end = page_start +
			(((*(p+5) * 256 + *(p+6)) * 256) + *(p+7));
		if (page_end > (data + len)) page_end = data + len;
		for (p = page_start; p < page_end && i < avail_elem; p += edl, i++) {
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
	return (avail_elem);
}

int smc_get_geometry(int fd,
                     char *rbtdev,
                     struct robot_info *robot_info)
{
	unsigned char buf[36];
	unsigned char cdb[6];
	char func[16];
	char *msgaddr;
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
	    rc = send_scsi_cmd (fd, rbtdev, 0, cdb, 6, buf, 36,
		sense, 38, 900000, SCSI_IN, &nb_sense_ret, &msgaddr);
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
	     rc = send_scsi_cmd (fd, rbtdev, 0, cdb, 6, buf, 24,
		 sense, 38, 900000, SCSI_IN, &nb_sense_ret, &msgaddr);
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

int smc_read_elem_status(int fd,
			 char *rbtdev,
			 int type,
			 int start,
			 int nbelem,
			 struct smc_element_info element_info[])
{
	char func[16];
	int rc;

	strncpy(func, "read_elem_statu", sizeof(func));
	func[sizeof(func) - 1] = '\0';

	rc = get_element_info (0xB8, fd, rbtdev, type, start, nbelem, element_info);
	return (rc);
}

int smc_find_cartridge2 (int fd,
                         char *rbtdev,
                         char *template,
                         int type,
                         int start,
                         int nbelem,
                         struct smc_element_info element_info[])
{
	int c;
	static char err_msgbuf[132];
	int found;
	char func[16];
	int i;
	struct smc_element_info *inventory_info; 
	char *msgaddr;
	int pm = 0;
	struct robot_info robot_info;
	int tot_nbelem;

  (void)nbelem;
	strncpy(func, "find_cartridge2", sizeof(func));
	func[sizeof(func) - 1] = '\0';

	if ((c = smc_get_geometry (fd, rbtdev, &robot_info)))
		return (c);

	tot_nbelem = robot_info.transport_count + robot_info.slot_count +
		robot_info.port_count + robot_info.device_count;

	if ((inventory_info = malloc (tot_nbelem * sizeof(struct smc_element_info))) == NULL) {
		serrno = errno;
		sprintf (err_msgbuf, "malloc error: %s", strerror(errno));
		msgaddr = err_msgbuf;
		save_error (-1, 0, NULL, msgaddr);
		return (-1);
	}

	if ((c = smc_read_elem_status (fd, rbtdev, type, start, tot_nbelem, inventory_info)) < 0) {
		free (inventory_info);
		return (c);
	}
	found = 0;	
	if (strchr (template, '*') || strchr (template, '?'))	/* pattern matching */
		pm++;
	for (i = 0 ; i < tot_nbelem ; i++)
		if (inventory_info[i].state & 0x1) {
			if (! pm) {
				if (strcmp (template, inventory_info[i].name) == 0) {
					memcpy (element_info, &inventory_info[i],
						sizeof(struct smc_element_info));
					found++;
					break;
				}
			} else {
				if (vmatch (template, inventory_info[i].name) == 0) {
					memcpy (&element_info[found], &inventory_info[i],
						sizeof(struct smc_element_info));
					found++;
				}
			}
		}
	free (inventory_info);
	return (found);
}


int smc_find_cartridge(int fd,
                       char *rbtdev,
                       char *template,
                       int type,
                       int start,
                       int nbelem,
                       struct smc_element_info element_info[])
{
	unsigned char cdb[12];
	char func[16];
	char *msgaddr;
	int nb_sense_ret;
	char plist[40];
	int rc;
	char sense[MAXSENSE];
        int pause_mode = 1;
        int nretries = 0;
        char *smcLibraryType;

	strncpy(func, "find_cartridge", sizeof(func));
	func[sizeof(func) - 1] = '\0';
        
        /* for Spectra like library we will skip 0xB6 cdb command as soon as
           is does not supported to speed up the function */
        smcLibraryType = getconfent("SMC","LIBRARY_TYPE",0);
        if (NULL != smcLibraryType &&
            0 == strcasecmp(smcLibraryType,"SPECTRA")) {
          rc = smc_find_cartridge2 (fd, rbtdev, template, type, start, nbelem,
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
	strcpy (plist, template);
   
       /* IBM library in pause mode  */ 
        while (pause_mode && nretries <= 900) {
          rc = send_scsi_cmd (fd, rbtdev, 0, cdb, 12, (unsigned char*)plist, 40,
                           sense, 38, 900000, SCSI_OUT, &nb_sense_ret, &msgaddr);
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
			rc = smc_find_cartridge2 (fd, rbtdev, template, type,
			    start, nbelem, element_info);
			if (rc >= 0)
				return (rc);
		}
		return (-1);
	}
	rc = get_element_info (0xB5, fd, rbtdev, type, start, nbelem, element_info);
	return (rc);
}


/* SCSI 3 additional sense code and additional sense qualifier */
struct scsierr_codact {
	unsigned char sensekey;
	unsigned char asc;
	unsigned char ascq;
	short action;
	char *txt;
};
struct scsierr_codact scsierr_acttbl[] = {
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

int smc_lasterror(struct smc_status *smc_stat,
                  char **msgaddr)
{
	unsigned int i;

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
			*msgaddr = scsierr_acttbl[i].txt;
			return (scsierr_acttbl[i].action);
		}
	}
	return (RBT_NORETRY);
}

int smc_move_medium(int fd,
                    char *rbtdev,
                    int from,
                    int to,
                    int invert)
{
	unsigned char cdb[12];
	char func[16];
	char *msgaddr;
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
	     rc = send_scsi_cmd (fd, rbtdev, 0, cdb, 12, NULL, 0,
		sense, 38, 900000, SCSI_NONE, &nb_sense_ret, &msgaddr);
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
