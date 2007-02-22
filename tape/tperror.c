/*
 * Copyright (C) 1994-2000 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
/* static char sccsid[] = "@(#)$RCSfile: tperror.c,v $ $Revision: 1.10 $ $Date: 2007/02/22 17:26:25 $ CERN IT-PDP/DM Jean-Philippe Baud"; */
#endif /* not lint */

/*      gettperror - get drive status after I/O error and
			build error msg from sense key or sense bytes */
/*	return	ETBLANK		blank tape
 *		ETCOMPA		compatibility problem
 *		ETHWERR		device malfunction
 *		ETPARIT		parity error
 *		ETUNREC		unrecoverable media error
 *		ETNOSNS		no sense
 */
#include <errno.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <sys/types.h>
#if defined(_WIN32)
#include <windows.h>
#else
#include <sys/ioctl.h>
#if !defined(_AIX) || defined(RS6000PCTA)
#include <sys/mtio.h>
#if defined(DUXV4)
#ifndef EEI_VERSION
#include <io/common/devgeteei.h>
#endif
#include <io/cam/scsi_all.h>
#endif
#endif
#if defined(ADSTAR)
#include <sys/Atape.h>
#endif
#endif
#include "Ctape.h"
#include "Ctape_api.h"
#include "serrno.h"
#if defined(_WIN32)
gettperror(tapefd, path, msgaddr)
int tapefd;
char *path;
char **msgaddr;
{
	int last_error;
	int rc;
	static char tp_err_msgbuf[80];

	last_error = GetLastError();
	if (last_error == ERROR_END_OF_MEDIA) {
		*msgaddr = "Volume overflow";
		return (ENOSPC);
	}
	FormatMessage (FORMAT_MESSAGE_FROM_SYSTEM, NULL, last_error,
	    LANG_NEUTRAL, tp_err_msgbuf, 80, NULL);
	*msgaddr = tp_err_msgbuf;
	switch (last_error) {
	case ERROR_BEGINNING_OF_MEDIA:
		rc = ETUNREC;
		break;
	case ERROR_NO_DATA_DETECTED:
		rc = ETBLANK;
		break;
	case ERROR_BUS_RESET:
		rc = ETHWERR;
		break;
	default:
		rc = -1;
	}
	return (rc);
}
#else
static char nosensekey[] = "no sense key available";
static char bbot[] = "BOT hit";
#if ! defined(_AIX) || defined(ADSTAR)
struct sk_info {
	char *text;
	int errcat;
};
struct sk_info sk_codmsg[] = {
	{"No sense", ETNOSNS},
	{"Recovered error", 0},
	{"Not ready", 0},
	{"Medium error", ETPARIT},
	{"Hardware error", ETHWERR},
	{"Illegal request", ETHWERR},
	{"Unit attention", ETHWERR},
	{"Data protect", 0},
	{"Blank check", ETBLANK},
	{"Vendor unique", 0},
	{"Copy aborted", 0},
	{"Aborted command", 0},
	{"Equal", 0},
	{"Volume overflow", 0},
	{"Miscompare", 0},
	{"Reserved", 0},
	{"SCSI handshake failure", ETHWERR},
	{"Timeout", ETHWERR},
	{"EOF hit", 0},
	{"EOT hit", ETBLANK},
	{"Length error", ETCOMPA},
	{"BOT hit", ETUNREC},
	{"Wrong tape media", ETCOMPA}
};
#endif
#if defined(_AIX) && (defined(RS6000PCTA) || defined(ADSTAR))
struct era_info {
	int code;
	char *text;
	int errcat;
};
 
struct era_info era_codmsg[] = {
#if defined(RS6000PCTA)
	0x00, "Unsolicited Sense.", 0,
	0x21, "Data Streaming Not Operational.", 0,
	0x22, "Path Equipment Check.", ETHWERR,
	0x23, "Read Data Check.", ETPARIT,
	0x24, "Load Display Check.", ETHWERR,
	0x25, "Write Data Check.", ETPARIT,
	0x26, "Data Check.", ETPARIT,
#endif
	0x27, "Command Reject.", 0,
#if defined(RS6000PCTA)
	0x28, "Write ID Mark Check.", ETPARIT,
	0x29, "Function incompatible.", ETCOMPA,
	0x2A, "Unsolicited Environmental Data.", 0,
	0x2B, "Environmental Data Present.", 0,
	0x2C, "Permanent Equipment Check.", ETHWERR,
	0x2D, "Data Security Erase Failure.", 0,
	0x2E, "Tape is blank or in unrecognized recording format.", ETBLANK,
	0x30, "Write Protected.", 0,
	0x31, "Tape Void.", ETBLANK,
	0x32, "Tension Loss.", ETHWERR,
	0x33, "Intervention Required - Reload Cartridge.", 0,
	0x34, "Intervention Required - Unload Cartridge.", 0,
	0x35, "Equipment Check.", ETHWERR,
	0x36, "End of data.", ETBLANK,
	0x37, "Tape is too short to be safely used.", ETUNREC,
	0x38, "Physical End of Tape.", ETUNREC,
	0x39, "Backspaced into BOT.", ETUNREC,
	0x3A, "Drive Not Ready.", 0,
	0x3B, "Manual Rewind-Unload.", 0,
	0x40, "Overrun.", ETHWERR,
	0x41, "Block ID Sequence Error.", ETPARIT,
	0x42, "Subsystem in Degraded Mode.", 0,
	0x43, "Intervention Required - make drive ready.", 0,
	0x44, "Record not found.", 0,
	0x45, "Drive Assigned Elsewhere.", ETHWERR,
	0x46, "Drive Offline.", ETHWERR,
	0x47, "Volume Fenced.", ETUNREC,
	0x48, "Unsolicited Informational Data.", 0,
	0x49, "Bus Out Check.", ETHWERR,
	0x4A, "Control Unit ERP Failed.", ETHWERR,
	0x4B, "Control Unit and Drive Incompatible.", ETHWERR,
	0x4C, "Recovered Check-One Failure.", 0,
	0x4D, "Resetting Event.", 0,
	0x4E, "Maximum Block Size Exceeded.", ETUNREC,
	0x50, "Read Buffered Log (Overflow).", 0,
	0x51, "Read Buffered Log (EOV).", 0,
	0x52, "End of Volume Complete.", 0,
	0x53, "Global Command Intercept.", 0,
	0x54, "Channel Interface Error - Temporary.", 0,
	0x55, "Channel Interface Error - Permanent.", ETHWERR,
	0x56, "Channel Protocol Error.", 0,
	0x57, "Global Status Intercept.", 0,
	0x5A, "Tape is too long to be safely used on this device.", ETCOMPA,
	0x5B, "Format 3480 XF Incompatible.", ETCOMPA,
	0x5C, "Device Not Capable of Reading 3490 Formats.", ETCOMPA,
	0x5D, "Tape is too long to be safely used.", ETUNREC,
	0x5E, "Compaction Algorithm Incompatible.", ETCOMPA,
#endif
	0x60, "Library Attachement Facility Equipment Check.", 0,
	0x62, "Library Manager Offline to Subsystem.", 0,
	0x63, "Control Unit and Library Manager Incompatible.", 0,
	0x64, "Library Volser in Use.", 0,
	0x65, "Library Volser Reserved.", 0,
	0x66, "Library Volser Not in Library.", 0,
	0x67, "Library Category Empty.", 0,
	0x68, "Library Order Sequence Check.", 0,
	0x69, "Library Output Station Full.", 0,
	0x6B, "Library Volume Misplaced.", 0,
	0x6C, "Library Misplaced Volume Found.", 0,
	0x6D, "Library Drive Not Unloaded.", 0,
	0x6E, "Library Inaccessible Volume Restored.", 0,
	0x6F, "Library Vision Failure.", 0,
	0x70, "Library Manager Equipment check.", 0,
	0x71, "Library Equipment Check.", 0,
	0x72, "Library Not capable - Manual Mode.", 0,
	0x73, "Library Intervention Required.", 0,
	0x74, "Library Information Data.", 0,
	0x75, "Library Volume Inaccessible.", 0,
	0x76, "Library All Cells Full.", 0,
	0x77, "Library Duplicate Volser Ejected.", 0,
	0x78, "Library Duplicate Volser Left In Input Station.", 0,
	0x79, "Library Unreadable or Invalid Volser Left In Input Station.", 0,
	0x7A, "Read Library Statistics.", 0,
	0x7B, "Library Volume Manually Ejected.", 0,
	0x7C, "Library Out of Cleaner Volumes.", 0,
	0x7F, "Library Category In Use.", 0,
	0x80, "Library Unexpected Volume Ejected.", 0,
	0x81, "Library I/O Station Door Open.", 0,
	0x82, "Library Manager Program Exception.", 0,
	0x83, "Library Drive Exception.", 0,
	0x84, "Library Drive Failure.", 0,
	0x85, "Library Environmental Alert.", 0,
	0x86, "Library All Categories Reserved.", 0,
	0xFF, NULL, 0
};
	static char ifcck[] = "Interface Check";
	static char nosensedata[] = "no sense data available";
#endif
int mt_rescnt;
static char tp_err_msgbuf[32];

#if ! defined(_AIX) || defined(ADSTAR)
int get_sk_msg(char *, int, int, int, char **);
#endif 

int gettperror(tapefd, path, msgaddr)
int tapefd;
char *path;
char **msgaddr;
{
#ifndef NOTRACE
	extern char *devtype;
#else
	char *devtype;
#endif
	struct devlblinfo  *dlip;
	int rc;
	int save_errno;
#ifndef _AIX
#if defined(DUXV4) && EEI_VERSION == 1
	DEV_EEI_STATUS deveei;
#else
#if defined(IRIX64)
	char tpsense[MTSCSI_SENSE_LEN];
#else
	struct mtget mt_info;
#endif
#endif
#else
#if defined(RS6000PCTA)
	mt_return_error_state_t errstatus;
#endif
#if defined(ADSTAR)
	struct stsense_s stsense;
	struct request_sense *rs;
#endif
#endif

	save_errno = errno;
#ifdef NOTRACE
	if (getlabelinfo (path, &dlip) < 0) {
		devtype = NULL;
		errno = save_errno;
	} else
		devtype = dlip->devtype;
#endif
#ifndef _AIX
#if defined(DUXV4) && EEI_VERSION == 1
	deveei.version = EEI_VERSION;
	if (ioctl (tapefd, DEVGETEEI, &deveei) < 0) {
		*msgaddr = nosensekey;
		rc = -1;
	} else {
		if (deveei.status <= EEI_NO_STATUS ||
		    deveei.status >= EEI_DEVPATH_CONFLICT ||
		    (deveei.flags & EEI_CAM_DATA_VALID) == 0) {
			*msgaddr = nosensekey;
			rc = -1;
		} else if (deveei.flags & EEI_SCSI_SENSE_VALID) {
			ALL_REQ_SNS_DATA *sdp =
				(ALL_REQ_SNS_DATA *) deveei.arch.cam.scsi_sense;
			rc = get_sk_msg (devtype, sdp->sns_key, sdp->asc,
			    sdp->asq, msgaddr);
			mt_rescnt = ((sdp->info_byte3 * 256 + sdp->info_byte2) * 256 +
				      sdp->info_byte1) * 256 + sdp->info_byte0;
		} else if (deveei.flags & EEI_CAM_STATUS_VALID) {
			get_cs_msg (deveei.arch.cam.cam_status & 0x3F, msgaddr);
			rc = -1;
		} else {
			*msgaddr = nosensekey;
			rc = -1;
		}
	}
#else
#if defined(IRIX64)
        if (ioctl (tapefd, MTSCSI_SENSE, tpsense) < 0) {
        	*msgaddr = nosensekey;
                rc = -1;
        } else {
	        if (tpsense[0] & 0x80) {    /* Valid */
               		mt_rescnt = tpsense[3] << 24 | tpsense[4] << 16 |
          			tpsense[5] << 8 | tpsense[6];
	       }
	       if ((tpsense[0] & 0x70) &&
       		   ((tpsense[2] & 0xE0) == 0 ||
       		    (tpsense[2] & 0xF) != 0)) {
			rc = get_sk_msg (devtype, tpsense[2] & 0xF,
					 tpsense[12],
					 tpsense[13], msgaddr);
	       } else {
		       *msgaddr = nosensekey;
		       rc = -1;
		      }
	}	
#else
	if (ioctl (tapefd, MTIOCGET, &mt_info) < 0) {
		*msgaddr = nosensekey;
		rc = -1;
	} else {
#if hpux
		rc = get_sk_msg (devtype, (mt_info.mt_dsreg1 >> 24) & 0xF,
			(mt_info.mt_dsreg1 >> 16) & 0xFF,
			(mt_info.mt_dsreg1 >> 8) & 0xFF, msgaddr);
#else
#if linux
		if (((mt_info.mt_erreg >> 16) & 0xF) == 0 &&
		    ((mt_info.mt_erreg >> 16) & 0x40)) {
			*msgaddr = bbot;
			rc = ETUNREC;
		} else rc = get_sk_msg (devtype, (mt_info.mt_erreg >> 16) & 0xF,
			(mt_info.mt_erreg >> 8) & 0xFF,
			(mt_info.mt_erreg) & 0xFF, msgaddr);
#else
#if defined(DUXV4) && EEI_VERSION == 2
		if (mt_info.eei.status == EEI_NO_STATUS &&
		    (mt_info.mt_dsreg & DEV_BOM)) {
			*msgaddr = bbot;
			rc = ETUNREC;
		} else if (mt_info.eei.status <= EEI_NO_STATUS ||
		    mt_info.eei.status >= EEI_DEVPATH_CONFLICT ||
		    (mt_info.eei.flags & EEI_CAM_DATA_VALID) == 0) {
			*msgaddr = nosensekey;
			rc = -1;
		} else if (mt_info.eei.flags & EEI_SCSI_SENSE_VALID) {
			ALL_REQ_SNS_DATA *sdp =
				(ALL_REQ_SNS_DATA *) mt_info.eei.arch.cam.scsi_sense;
			rc = get_sk_msg (devtype, sdp->sns_key, sdp->asc, sdp->asq, msgaddr);
		} else if (mt_info.eei.flags & EEI_CAM_STATUS_VALID) {
			get_cs_msg (mt_info.eei.arch.cam.cam_status & 0x3F, msgaddr);
			rc = -1;
		} else {
			*msgaddr = nosensekey;
			rc = -1;
		}
#else
		rc = get_sk_msg (devtype, mt_info.mt_erreg, 0, 0, msgaddr);
#endif
#endif
#endif
#if (defined(__alpha) && defined(__osf__)) || defined(linux)
		mt_rescnt = mt_info.mt_resid;
#endif
	}
#endif
#endif
#else
#if defined(_IBMR2)
	if (strcmp (dvrname, "tape") == 0) {
		*msgaddr = nosensekey;
		rc = -1;
#if defined(RS6000PCTA)
	} else if (strncmp (dvrname, "mtdd", 4) == 0) {
		if (ioctl (tapefd, MTIOC_RETURN_ERR, &errstatus) == 0) {
			rc = get_era_msg (errstatus.number_sense, errstatus.sense_bytes,
			    msgaddr);
		} else {
			*msgaddr = nosensedata;
			rc = -1;
		}
#endif
#if defined(ADSTAR)
	} else if (strcmp (dvrname, "Atape") == 0) {
		stsense.sense_type = LASTERROR;
		if (ioctl (tapefd, STIOCQRYSENSE, &stsense) == 0) {
			rs = (struct request_sense *) stsense.sense;
			if (rs->key == 0 && rs->eom) {
				*msgaddr = bbot;
				rc = ETUNREC;
			} else {
				rc = get_sk_msg (devtype, rs->key, rs->asc, rs->ascq, msgaddr);
				mt_rescnt = stsense.residual_count;
			}
		} else {
			*msgaddr = nosensekey;
			rc = -1;
		}
#endif
	}
#endif
#endif
	errno = save_errno;
	return (rc);
}

#if defined(RS6000PCTA) || defined(ADSTAR)
get_era_msg(number_sense, sense_bytes, msgaddr)
int number_sense;
char sense_bytes[];
char **msgaddr;
{
	int i;
	int rc;

	if (number_sense == 0) {
		*msgaddr = ifcck;
		rc = -1;
	} else {
		for (i = 0; i < 256; i++) {
			if (era_codmsg[i].code == 0xFF) {
				sprintf (tp_err_msgbuf,
				    "Undefined ERA code %02X", sense_bytes[3]);
				*msgaddr = tp_err_msgbuf;
				rc = -1;
				break;
			} else if (sense_bytes[3] == era_codmsg[i].code) {
				*msgaddr = era_codmsg[i].text;
				rc = era_codmsg[i].errcat;
				break;
			}
		}
	}
	return (rc);
}
#endif

#if ! defined(_AIX) || defined(ADSTAR)
int get_sk_msg(devtype, key, asc, ascq, msgaddr)
char *devtype;
int key;
int asc;
int ascq;
char **msgaddr;
{
	int rc;
#if defined(sun)
	if (key >= 0 && key < 23) {
#else
	if (key >= 0 && key < 15) {
#endif
		if (asc == 0 && ascq == 0) {
			*msgaddr = sk_codmsg[key].text;
		} else {
			sprintf (tp_err_msgbuf, "%s ASC=%X ASCQ=%X",
				sk_codmsg[key].text, asc, ascq);
			*msgaddr = tp_err_msgbuf;
		}
		if ((devtype && strcmp (devtype, "SD3") == 0 &&
		    key == 3 && asc == 0x30 && ascq == 0x01) ||
		    (devtype && strcmp (devtype, "3590") == 0 &&
		    key == 3 && asc == 0x30 && ascq == 0x02))
			rc = ETBLANK;
		else
			rc = sk_codmsg[key].errcat;
	} else {
		sprintf (tp_err_msgbuf, "Undefined sense key %02X", key);
		*msgaddr = tp_err_msgbuf;
		rc = -1;
	}
	return (rc);
}
#endif
#endif

int rpttperror(func, tapefd, path, cmd)
char *func;
int tapefd;
char *path;
char *cmd;
{
	char *msgaddr;
	int rc;

#if defined(_WIN32)
	if ((rc = gettperror (tapefd, path, &msgaddr)) <= 0) rc = EIO;
#else
	if (errno == EIO) {
		if ((rc = gettperror (tapefd, path, &msgaddr)) <= 0) rc = EIO;
#if defined(_IBMR2)
	} else if (errno == ENXIO && strcmp (cmd, "write") == 0) {
		msgaddr = "Volume overflow";
		rc = ENOSPC;
#endif
	} else {
		msgaddr = strerror(errno);
		rc = errno;
#if defined(_IBMR2)
		if (errno == EMEDIA) rc = ETPARIT;
#endif
	}
#endif
	usrmsg (func, TP042, path, cmd, msgaddr);
	return (rc);
}
