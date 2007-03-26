/*
 * Copyright (C) 1993-2003 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
/* static char sccsid[] = "@(#)$RCSfile: rbtsubr.c,v $ $Revision: 1.23 $ $Date: 2007/03/26 15:33:02 $ CERN IT-PDP/DM Jean-Philippe Baud"; */
#endif /* not lint */

/*	rbtsubr - control routines for robot devices */
#include <errno.h>
#include <stdio.h>
#include <sys/types.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#if defined(_AIX) && defined(_IBMR2) && (defined(RS6000PCTA) || defined(ADSTAR))
#if defined(RS6000PCTA)
#include <sys/mtio.h>
#endif
#if defined(ADSTAR)
#include <sys/Atape.h>
#endif
#include <sys/mtlibio.h>
#endif
#if defined(CDK)
#include "acssys.h"
#include "acsapi.h"
#endif
#if defined(DMSCAPI)
#include "fbsuser.h"
#endif
#include <stdlib.h>
#include <string.h>
#include <pwd.h>
#include <netdb.h>              /* network "data base"                  */
#include <sys/types.h>          /* standard data types                  */
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>          /* arpa internet routines               */
#include "dmc.h"
#include "net.h"
#include "rmc_api.h"
#include "smc.h"
#include "Ctape.h"
#include "Ctape_api.h"
#include "tplogger_api.h"
extern char msg[];

static char action[8];
static char cur_unm[9];
static char cur_vid[7];
#if defined(_AIX) && defined(_IBMR2) && (defined(RS6000PCTA) || defined(ADSTAR))
static int mount_req_id_3495 = -1;
#endif
#if defined(CDK)
static REQ_ID dismount_req_id = 0;
static REQ_ID mount_req_id = 0;
#endif
struct rbterr_codact {
	int cc;
	short mnt_fail_action;
	short dmnt_fail_action;
};
/*	rbtmount/rbtdemount - mount/dismount a volume in a robotic drive */
/*	return: RBT_OK		Ok or error should be ignored
		RBT_NORETRY	Unrecoverable error (just log it)
		RBT_SLOW_RETRY	Should release drive & retry in 600 seconds
		RBT_FAST_RETRY	Should retry in 60 seconds
		RBT_DMNT_FORCE	Should do first a demount force
		RBT_CONF_DRV_DN	Should configure the drive down
		RBT_OMSG_NORTRY	Should send a msg to operator and exit
		RBT_OMSG_SLOW_R Ops msg (nowait) + release drive + slow retry
		RBT_OMSGR	Should send a msg to operator and wait
		RBT_UNLD_DMNT	Should unload the tape and retry demount
 */


/*
** Protoypes
*/
int acsmount( char*, char*, int );
int acsdismount( char*, char*, unsigned int );

int dmcmount( char*, char*, char* );
int dmcdismount( char*, char*, char*, unsigned int );

/* int smcmount( char*, int, char*, int ); */
int smcdismount( char*, char*, int, int );

int send2dmc( int*, DMCrequest_t * );
int fromdmc( int*, DMCreply_t * );


/*	rbtmount - mounts a volume on a specified drive  */

int rbtmount (vid, side, unm, dvn, ring, loader)
char *vid;
int side;
char *unm;
char *dvn;
int ring;
char *loader;
{
	char func[16];

	if (*loader == 'r')
		return (0);
	strcpy (action, "mount");
	strcpy (cur_unm, unm);
	strcpy (cur_vid, vid);
#if defined(_AIX) && defined(_IBMR2) && (defined(RS6000PCTA) || defined(ADSTAR))
	if (*loader == 'l')
		return (mount3495 (vid, dvn, loader));
#endif
#if defined(CDK)
	if (*loader == 'a')
		return (acsmount (vid, loader, ring));
#endif
#if defined(DMSCAPI)
	if (*loader == 'R')
		return (mountfbs (vid, loader));
#endif

	if (*loader == 'd')
		return(dmcmount(vid,unm,loader));

	if (*loader == 'n'
#if !defined(DMSCAPI)
	    || *loader == 'R'
#endif
			     ) {
		char buf[256];
		FILE *f, *popen();

		ENTRY (rbtmount);
		if (*loader == 'n')
			sprintf (buf, "nsrjb -l -n -f %s %s 2>&1", dvn, vid);
		else
			sprintf (buf, "/dms/fbs/bin/dmscmv C%s %s 2>&1", vid, loader);
		tplogit (func, "%s\n", buf);
                tl_tpdaemon.tl_log( &tl_tpdaemon, 111, 2,
                                    "func",    TL_MSG_PARAM_STR, func,
                                    "Message", TL_MSG_PARAM_STR, buf );
		if ((f = popen (buf, "r")) == NULL) {
			usrmsg (func, TP042, "", "popen", strerror(errno));
                        tl_tpdaemon.tl_log( &tl_tpdaemon, 42, 3,
                                            "func",    TL_MSG_PARAM_STR, func,
                                            "Message", TL_MSG_PARAM_STR, "popen",
                                            "Error",   TL_MSG_PARAM_STR, strerror(errno) );
			RETURN (-errno);
		}
		while (fgets (buf, sizeof(buf), f) != NULL) {
			usrmsg (func, "TP041 - %s of %s on %s failed : %s\n",
			    action, cur_vid, cur_unm, buf);
                        tl_tpdaemon.tl_log( &tl_tpdaemon, 41, 5,
                                            "func",    TL_MSG_PARAM_STR, func,
                                            "action",  TL_MSG_PARAM_STR, action,
                                            "cur_vid", TL_MSG_PARAM_STR, cur_vid,
                                            "cur_unm", TL_MSG_PARAM_STR, cur_unm,
                                            "Message", TL_MSG_PARAM_STR, buf );
                }
		if (pclose (f)) {
			RETURN (-EIO);
		} else {
			RETURN (0);
		}
	}
	if (*loader == 's') {
		int c;
		c = smcmount (vid, side, loader);
		closesmc();
		return (c);
	}
	ENTRY (rbtmount);
	usrmsg (func, "TP041 - %s of %s on %s failed : %s\n", action, cur_vid,
	    cur_unm, "invalid loader type");
        tl_tpdaemon.tl_log( &tl_tpdaemon, 41, 5,
                            "func",    TL_MSG_PARAM_STR, func,
                            "action",  TL_MSG_PARAM_STR, action,
                            "cur_vid", TL_MSG_PARAM_STR, cur_vid,
                            "cur_unm", TL_MSG_PARAM_STR, cur_unm,
                            "Message", TL_MSG_PARAM_STR, "invalid loader type" );
	RETURN (RBT_CONF_DRV_DN);
}

/*	rbtdemount - demounts a volume from a specified drive  */

int rbtdemount (vid, unm, dvn, loader, force, vsnretry)
char *vid;
char *unm;
char *dvn;
char *loader;
unsigned int force;
int vsnretry;
{
	char func[16];

	if (*loader == 'r')
		return (0);
	strcpy (action, "demount");
	strcpy (cur_unm, unm);
	strcpy (cur_vid, vid);
#if defined(_AIX) && defined(_IBMR2) && (defined(RS6000PCTA) || defined(ADSTAR))
	if (*loader == 'l') {
		int c;
		int status;

		if (mount_req_id_3495 < 0 || qmid3495 (&status) ||
		    (status != MT_PENDING_STS) || cancel3495()) {
			closelmcp();
			c = demount3495 (vid, dvn, loader, force);
		} else
			c = 0;
		closelmcp ();
		return (c);
	}
#endif
#if defined(CDK)
	if (*loader == 'a') {
		if (mount_req_id)
			wait4acsfinalresp();
		return (acsdismount (vid, loader, force));
	}
#endif
#if defined(DMSCAPI)
	if (*loader == 'R')
		return (dismountfbs (vid, loader, force));
#endif

	if (*loader == 'd')
		return(dmcdismount(vid, unm, loader, force));

	if (*loader == 'n'
#if !defined(DMSCAPI)
	    || *loader == 'R'
#endif
			     ) {
		char buf[256];
		FILE *f, *popen();

		ENTRY (rbtdemount);

		if (*loader == 'n')
			sprintf (buf, "nsrjb -u -f %s 2>&1", dvn);
		else
			sprintf (buf, "/dms/fbs/bin/dmscmv C%s 2>&1", vid);
		tplogit (func, "%s\n", buf);
                tl_tpdaemon.tl_log( &tl_tpdaemon, 111, 2,
                                    "func",    TL_MSG_PARAM_STR, func,
                                    "Message", TL_MSG_PARAM_STR, buf );
		if ((f = popen (buf, "r")) == NULL) {
			usrmsg (func, TP042, "", "popen", strerror(errno));
                        tl_tpdaemon.tl_log( &tl_tpdaemon, 42, 3,
                                            "func",    TL_MSG_PARAM_STR, func,
                                            "Message", TL_MSG_PARAM_STR, "popen",
                                            "Error",   TL_MSG_PARAM_STR, strerror(errno) );
			RETURN (-errno);
		}
		while (fgets (buf, sizeof(buf), f) != NULL) {
			usrmsg (func, "TP041 - %s of %s on %s failed : %s\n",
			    action, cur_vid, cur_unm, buf);
                        tl_tpdaemon.tl_log( &tl_tpdaemon, 41, 5,
                                            "func",    TL_MSG_PARAM_STR, func,
                                            "action",  TL_MSG_PARAM_STR, action,
                                            "cur_vid", TL_MSG_PARAM_STR, cur_vid,
                                            "cur_unm", TL_MSG_PARAM_STR, cur_unm,
                                            "Message", TL_MSG_PARAM_STR, buf );
                }
		if (pclose (f)) {
			RETURN (-EIO);
		} else {
			RETURN (0);
		}
	}
	if (*loader == 's') {
		int c;
		c = smcdismount (vid, loader, force, vsnretry);
		closesmc();
		return (c);
	}
	ENTRY (rbtdemount);
	usrmsg (func, "TP041 - %s of %s on %s failed : %s\n", action, cur_vid,
	    cur_unm, "invalid loader type");
        tl_tpdaemon.tl_log( &tl_tpdaemon, 41, 5,
                            "func",    TL_MSG_PARAM_STR, func,
                            "action",  TL_MSG_PARAM_STR, action,
                            "cur_vid", TL_MSG_PARAM_STR, cur_vid,
                            "cur_unm", TL_MSG_PARAM_STR, cur_unm,
                            "Message", TL_MSG_PARAM_STR, "invalid loader type" );
	RETURN (RBT_CONF_DRV_DN);
}

#if defined(_AIX) && defined(_IBMR2) && (defined(RS6000PCTA) || defined(ADSTAR))

/*	3495subr - I/O control routines for 3495 Library Devices */
static int device;
static char ldr[14];
static int lmcpfd = -1;
static struct mtlqmidarg mtlqmidarg;

/*	openlmcp - opens lmcp and gets ESA device number for the specified drive */

openlmcp (drive, loader)
char *drive;
char *loader;
{
	char func[16];
	int tapefd;

	ENTRY (openlmcp);
	if ((tapefd = open (drive, O_RDONLY|O_NDELAY)) < 0) {
		usrmsg (func, TP042, drive, "open", strerror(errno));
                tl_tpdaemon.tl_log( &tl_tpdaemon, 42, 4,
                                    "func",    TL_MSG_PARAM_STR, func,
                                    "drive",   TL_MSG_PARAM_STR, drive, 
                                    "Message", TL_MSG_PARAM_STR, "open",
                                    "Error",   TL_MSG_PARAM_STR, strerror(errno) );
		RETURN (-errno);
	}
	ioctl (tapefd, MTDEVICE, &device);
	close (tapefd);
	sprintf (ldr, "/dev/%s", loader);
	if ((lmcpfd = open (ldr, O_RDWR)) < 0) {
		usrmsg (func, TP042, ldr, "open", strerror(errno));
                tl_tpdaemon.tl_log( &tl_tpdaemon, 42, 4,
                                    "func",    TL_MSG_PARAM_STR, func,
                                    "ldr",     TL_MSG_PARAM_STR, ldr, 
                                    "Message", TL_MSG_PARAM_STR, "open",
                                    "Error",   TL_MSG_PARAM_STR, strerror(errno) );
		RETURN (-errno);
	}
	RETURN (lmcpfd);
}

/*	mount3495 - mounts a volume on a specified drive  */
/*	does not wait until the mount is complete */

mount3495 (vid, drive, loader)
char *vid;
char *drive;
char *loader;
{
	int c;
	char func[16];
	struct mtlmarg mtlmarg;

	ENTRY (mount3495);
	if (lmcpfd < 0) {
		if ((lmcpfd = openlmcp (drive, loader)) < 0)
			RETURN (RBT_CONF_DRV_DN);
		memset ((char *)&mtlmarg, 0, sizeof(mtlmarg));
		mtlmarg.device = device;
		memset (mtlmarg.volser, ' ', sizeof(mtlmarg.volser));
		memcpy (mtlmarg.volser, vid, strlen(vid));
	}
	if (ioctl (lmcpfd, MTIOCLM, &mtlmarg) < 0) {
		c = ibmrbterr (func, ldr, action, mtlmarg.mtlmret.cc,
			mtlmarg.mtlmret.number_sense, mtlmarg.mtlmret.sense_bytes);
		RETURN (c);
	}
	memset ((char *)&mtlqmidarg, 0, sizeof(mtlqmidarg));
	mtlqmidarg.device = device;
	mtlqmidarg.req_id = mtlmarg.mtlmret.req_id;
	mount_req_id_3495 = mtlmarg.mtlmret.req_id;
	RETURN (0);
}

/*	demount3495 - demounts a volume from a specified drive  */
/*	does not wait until the demount is complete */

demount3495 (vid, drive, loader, force)
char *vid;
char *drive;
char *loader;
unsigned int force;
{
	int c;
	char func[16];
	struct mtldarg mtldarg;
	int status;

	ENTRY (demount3495);
	if (lmcpfd < 0) {
		if ((lmcpfd = openlmcp (drive, loader)) < 0)
			RETURN (RBT_CONF_DRV_DN);
		memset ((char *)&mtldarg, 0, sizeof(mtldarg));
		mtldarg.device = device;
		memset (mtldarg.volser, ' ', sizeof(mtldarg.volser));
		if (! force)
			memcpy (mtldarg.volser, vid, strlen(vid));
	}
	if (ioctl (lmcpfd, MTIOCLDM, &mtldarg) < 0) {
		c = ibmrbterr (func, ldr, action, mtldarg.mtldret.cc,
			mtldarg.mtldret.number_sense, mtldarg.mtldret.sense_bytes);
		RETURN (c);
	}
	memset ((char *)&mtlqmidarg, 0, sizeof(mtlqmidarg));
	mtlqmidarg.device = device;
	mtlqmidarg.req_id = mtldarg.mtldret.req_id;
	while ((c = qmid3495 (&status)) == 0 && status == MT_PENDING_STS)
		sleep (UCHECKI);
	RETURN (c);
}

qmid3495 (status)
int *status;
{
	int c;
	char func[16];
	int libcc;
	char *msgaddr;

	strcpy (func, "qmid3495");
	if (ioctl (lmcpfd, MTIOCLQMID, &mtlqmidarg) < 0) {
		c = ibmrbterr (func, ldr, "qmid", mtlqmidarg.mtlqmidret.cc,
			mtlqmidarg.mtlqmidret.number_sense,
			mtlqmidarg.mtlqmidret.sense_bytes);
		RETURN (c);
	}
	if (mtlqmidarg.mtlqmidret.info.status_type == EXE_TYPE) {
		*status = mtlqmidarg.mtlqmidret.info.status_response.status;
		return (0);
	} else {
		*status = 0;
		libcc = mtlqmidarg.mtlqmidret.info.status_response.drm_status.cc;
		libcc2txt (libcc, &msgaddr);
		if (libcc < 4) {
			if (libcc) {
				tplogit (func, "TP041 - %s of %s on %s %s\n",
					action, cur_vid, cur_unm, msgaddr);
                                tl_tpdaemon.tl_log( &tl_tpdaemon, 41, 5,
                                                    "func",    TL_MSG_PARAM_STR, func,
                                                    "action",  TL_MSG_PARAM_STR, action,
                                                    "cur_vid", TL_MSG_PARAM_STR, cur_vid,
                                                    "cur_unm", TL_MSG_PARAM_STR, cur_unm,
                                                    "msgaddr", TL_MSG_PARAM_STR, msgaddr );
                        }
			return (0);
		} else {
			sprintf (msg, "TP041 - %s of %s on %s %s",
				action, cur_vid, cur_unm, msgaddr);
			usrmsg (func, "%s\n", msg);
                        tl_tpdaemon.tl_log( &tl_tpdaemon, 41, 5,
                                            "func",    TL_MSG_PARAM_STR, func,
                                            "action",  TL_MSG_PARAM_STR, action,
                                            "cur_vid", TL_MSG_PARAM_STR, cur_vid,
                                            "cur_unm", TL_MSG_PARAM_STR, cur_unm,
                                            "msgaddr", TL_MSG_PARAM_STR, msgaddr );
			c =  liberr2act (0, libcc);
			RETURN (c);
		}
	}
}

cancel3495 ()
{
	int c;
	char func[16];
	struct mtlcarg mtlcarg;

	ENTRY (cancel3495);
	memset ((char *)&mtlcarg, 0, sizeof(mtlcarg));
	mtlcarg.device = device;
	mtlcarg.req_id = mount_req_id_3495;
	mtlcarg.cancel_type = MIDC;
	if (ioctl (lmcpfd, MTIOCLC, &mtlcarg) < 0) {
		c = ibmrbterr (func, ldr, action, mtlcarg.mtlcret.cc,
			mtlcarg.mtlcret.number_sense, mtlcarg.mtlcret.sense_bytes);
		RETURN (c);
	}
	RETURN (0);
}

closelmcp ()
{
	if (lmcpfd >= 0)
		close (lmcpfd);
	lmcpfd = -1;
}

eracod2act(req_type, cc)
int req_type;	/* 0 --> mount, 1 --> dismount */
int cc;		/* error returned by the mount/dismount routine */
{
	struct rbterr_codact era_acttbl[] = {
	  0x60, RBT_OMSGR, RBT_OMSGR,	/* Library Attachement Facility Equipment Check */
	  0x62, RBT_OMSGR, RBT_OMSGR,	/* Library Manager Offline to Subsystem */
#if TMS
	  0x64, RBT_OMSG_SLOW_R, RBT_NORETRY,	/* Library Volser in Use */
#else
	  0x64, RBT_SLOW_RETRY, RBT_NORETRY, 
#endif
	  0x66, RBT_OMSG_NORTRY, RBT_OK,	/* Library Volser Not in Library */
	  0x68, RBT_DMNT_FORCE, RBT_OK,		/* Library Order Sequence Check */
	  0x6B, RBT_OMSG_SLOW_R, RBT_NORETRY,	/* Library Volume Misplaced */
	  0x6D, RBT_NORETRY, RBT_UNLD_DMNT,	/* Library Drive Not Unloaded */
	  0x75, RBT_OMSG_SLOW_R, RBT_OMSG_SLOW_R, /* Library Volume Inaccessible */
	};
	int i;

	for (i = 0; i < sizeof(era_acttbl)/sizeof(struct rbterr_codact); i++) {
		if (cc == era_acttbl[i].cc)
			return ((req_type == 0) ?
				era_acttbl[i].mnt_fail_action :
				era_acttbl[i].dmnt_fail_action);
	}
	return (RBT_NORETRY);
}

libcc2txt(cc, msgaddr)
int cc;
char **msgaddr;
{
	static char *liberrtxt[] = {
	  "Complete - No error.",
	  "Complete - Vision system not operational.",
	  "Complete - VOLSER not readable.",
	  "Complete - Category assignment not changed.",
	  "Cancelled - Program request.",
	  "Cancelled - Order sequence.",
	  "Cancelled - Manual mode.",
	  "Failed - Unexpected hardware failure.",
	  "Failed - Vision system not operational.",
	  "Failed - VOLSER not readable.",
	  "Failed - Volume inaccessible.",
	  "Failed - Volume misplaced.",
	  "Failed - Category empty.",
	  "Failed - Volume manually ejected.",
	  "Failed - Volume no longer in inventory.",
	  "Failed - Device no longer available.",
	  "Failed - Unrecoverable Load Failure.",
	  "Failed - Damaged Cartridge Ejected.",
	  "Undefined completion code.",
	  "Undefined completion code.",
	  "Undefined completion code.",
	  "Undefined completion code.",
	  "Undefined completion code.",
	  "Undefined completion code.",
	  "Undefined completion code.",
	  "Undefined completion code.",
	  "Undefined completion code.",
	  "Undefined completion code.",
	  "Undefined completion code.",
	  "Undefined completion code.",
	  "Undefined completion code.",
	  "Undefined completion code.",
	  "Error - LMCP is not configured.",
	  "Error - Device is not command-port LMCP.",
	  "Error - Device is not configured.",
	  "Error - Device is not in library.",
	  "Error - Not enough memory.",
	  "Error - Device is in use.",
	  "Error - I/O failed.",
	  "Error - Device is invalid.",
	  "Error - Device is not notification-port LMCP. ",
	  "Error - Invalid sub command parameter.",
	  "Error - No library device is configured.",
	  "Error - Internal error.",
	  "Error - Invalid cancel type.",
	  "Error - Not an LMCP device.",
	  "Error - Library is Offline to Host.",
	  "Undefined completion code."
	};
	if (cc >= 0 && cc < 47)
		*msgaddr = liberrtxt[cc];
	else
		*msgaddr = liberrtxt[47];
}

liberr2act(req_type, cc)
int req_type;	/* 0 --> mount, 1 --> dismount */
int cc;		/* error returned by the mount/dismount routine */
{
	struct rbterr_codact liberr_acttbl[] = {
	  MTCC_CANCEL_ORDERSEQ, RBT_DMNT_FORCE, RBT_OK,		/* Order sequence */
	  MTCC_FAILED_HARDWARE, RBT_OMSGR, RBT_OMSGR,		/* Unexpected hardware failure */
	  MTCC_FAILED_INACC, RBT_OMSG_SLOW_R, RBT_OMSG_SLOW_R,	/* Volume inaccessible */
	  MTCC_FAILED_MISPLACED, RBT_OMSG_SLOW_R, RBT_NORETRY,	/* Volume misplaced */
	  MTCC_FAILED_INVENTORY, RBT_OMSGR, RBT_OMSGR,		/* Volume not in inventory */
	  MTCC_FAILED_NOTAVAIL, RBT_CONF_DRV_DN, RBT_CONF_DRV_DN, /* Device no longer available */
	  MTCC_FAILED_LOADFAIL, RBT_CONF_DRV_DN, RBT_CONF_DRV_DN, /* Unrecoverable Load Failure */
	  MTCC_FAILED_DAMAGED, RBT_OMSGR, RBT_OMSGR,		/* Damaged Cartridge Ejected */
	  MTCC_NO_DEVLIB, RBT_CONF_DRV_DN, RBT_CONF_DRV_DN,	/* Device is not in library */
	  MTCC_LIB_OFFLINE, RBT_OMSGR, RBT_OMSGR,		/* Library is Offline to Host */
	};
	int i;

	for (i = 0; i < sizeof(liberr_acttbl)/sizeof(struct rbterr_codact); i++) {
		if (cc == liberr_acttbl[i].cc)
			return ((req_type == 0) ?
				liberr_acttbl[i].mnt_fail_action :
				liberr_acttbl[i].dmnt_fail_action);
	}
	return (RBT_NORETRY);
}

ibmrbterr(func, ldr, action, cc, number_sense, sense_bytes)
char *func;
char *ldr;
char *action;
int cc;
int number_sense;
char sense_bytes[];
{
	char *msgaddr;

	if (errno != EIO) {
		usrmsg (func, TP042, ldr, "ioctl", strerror(errno));
                tl_tpdaemon.tl_log( &tl_tpdaemon, 42, 4,
                                    "func",    TL_MSG_PARAM_STR, func,
                                    "ldr",     TL_MSG_PARAM_STR, ldr,
                                    "Message", TL_MSG_PARAM_STR, "ioctl",
                                    "Error",   TL_MSG_PARAM_STR, strerror(errno) );                
		return (-errno);
	} else {
		if (cc == MTCC_IO_FAILED) {
			get_era_msg (number_sense, sense_bytes, &msgaddr);
			sprintf (msg, TP041, action, cur_vid, cur_unm, msgaddr);
			usrmsg (func, "%s\n", msg);
                        tl_tpdaemon.tl_log( &tl_tpdaemon, 41, 5,
                                            "func",    TL_MSG_PARAM_STR, func,
                                            "action",  TL_MSG_PARAM_STR, action,
                                            "cur_vid", TL_MSG_PARAM_STR, cur_vid,
                                            "cur_unm", TL_MSG_PARAM_STR, cur_unm,
                                            "msgaddr", TL_MSG_PARAM_STR, msgaddr );                       
 			if (number_sense)
				return (eracod2act (*action == 'm' ? 0 : 1, sense_bytes[3]));
			else
				return (RBT_NORETRY);
		} else {
			libcc2txt (cc, &msgaddr);
			sprintf (msg, "TP041 - %s of %s on %s %s",
				action, cur_vid, cur_unm, msgaddr);
			usrmsg (func, "%s\n", msg);
                        tl_tpdaemon.tl_log( &tl_tpdaemon, 41, 5,
                                            "func",    TL_MSG_PARAM_STR, func,
                                            "action",  TL_MSG_PARAM_STR, action,
                                            "cur_vid", TL_MSG_PARAM_STR, cur_vid,
                                            "cur_unm", TL_MSG_PARAM_STR, cur_unm,
                                            "msgaddr", TL_MSG_PARAM_STR, msgaddr );
			return (liberr2act (*action == 'm' ? 0 : 1, cc));
		}
	}
}
#endif
#if defined(CDK)

/*	acssubr - I/O control routines for Storage Tek silos */
/*	loader should be of the form "acs"acs_id,lsm,panel,drive */

char acsloader[14];
ALIGNED_BYTES rbuf[MAX_MESSAGE_SIZE/sizeof(ALIGNED_BYTES)];

int acserr2act(req_type, cc)
int req_type;	/* 0 --> mount, 1 --> dismount */
int cc;		/* error returned by the mount/dismount routine */
{
	struct rbterr_codact acserr_acttbl[] = {
	  {STATUS_DRIVE_AVAILABLE, RBT_NORETRY, RBT_OK},		/* unload on empty drive */
	  {STATUS_DRIVE_IN_USE, RBT_DMNT_FORCE, RBT_DMNT_FORCE},
	  {STATUS_DRIVE_OFFLINE, RBT_CONF_DRV_DN, RBT_CONF_DRV_DN},
	  {STATUS_INVALID_VOLUME, RBT_NORETRY, RBT_NORETRY},	/* syntax error in vid */
	  {STATUS_IPC_FAILURE, RBT_OMSGR, RBT_OMSGR},
	  {STATUS_LIBRARY_FAILURE, RBT_CONF_DRV_DN, RBT_CONF_DRV_DN},
	  {STATUS_LIBRARY_NOT_AVAILABLE, RBT_FAST_RETRY, RBT_FAST_RETRY},
	  {STATUS_LSM_OFFLINE, RBT_OMSGR, RBT_OMSGR},
	  {STATUS_MISPLACED_TAPE, RBT_OMSG_NORTRY, RBT_OMSG_NORTRY},
	  {STATUS_PENDING, RBT_DMNT_FORCE, RBT_DMNT_FORCE},	/* corrupted database */
#if TMS
	  {STATUS_VOLUME_IN_DRIVE, RBT_OMSG_SLOW_R, RBT_NORETRY},	/* volume in use */
#else
	  {STATUS_VOLUME_IN_DRIVE, RBT_SLOW_RETRY, RBT_NORETRY}, 
#endif
	  {STATUS_VOLUME_NOT_IN_DRIVE, RBT_NORETRY, RBT_DMNT_FORCE}, /* vid mismatch on unload */
	  {STATUS_VOLUME_NOT_IN_LIBRARY, RBT_OMSG_NORTRY, RBT_OK},
	  {STATUS_VOLUME_IN_USE, RBT_FAST_RETRY, RBT_FAST_RETRY},	/* volume in transit */
	  {STATUS_NI_FAILURE, RBT_OMSGR, RBT_OMSGR},		/* contact with ACSLS lost */
	  {STATUS_INVALID_MEDIA_TYPE, RBT_OMSG_NORTRY, RBT_OMSG_NORTRY},
	  {STATUS_INCOMPATIBLE_MEDIA_TYPE, RBT_OMSGR, RBT_NORETRY},
	};
	int i;

	for (i = 0; i < sizeof(acserr_acttbl)/sizeof(struct rbterr_codact); i++) {
		if (cc == acserr_acttbl[i].cc)
			return ((req_type == 0) ?
				acserr_acttbl[i].mnt_fail_action :
				acserr_acttbl[i].dmnt_fail_action);
	}
	return (RBT_NORETRY);
}

char *
acsstatus(status)
STATUS status;
{
	char * acs_status();
	char *p;

	p = acs_status (status);
	if (strncmp (p, "STATUS_", 7) == 0)
		p = p + 7;
	return (p);
}

int acsmount(vid, loader, ring)
char *vid;
char *loader;
int ring;
{
	int c;
	DRIVEID drive_id;
	char func[16];
	SEQ_NO myseqnum = 0;
	STATUS status;
	VOLID vol_id;
    int use_read_only_flag = 1;
	ENTRY (acsmount);
    /* Check whether the "No read-only" option was specified */
    if (strstr(loader, ACS_NO_READ_ONLY) != NULL) {
        use_read_only_flag = 0;
    }
	strcpy (acsloader, loader);
	strcpy (vol_id.external_label, vid);
	drive_id.panel_id.lsm_id.acs = atoi (strtok (&acsloader[3], ","));
	drive_id.panel_id.lsm_id.lsm = atoi (strtok (NULL, ","));
	drive_id.panel_id.panel = atoi (strtok (NULL, ","));
	drive_id.drive = atoi (strtok (NULL, ","));
	tplogit (func, "vol_id = %s drive_id = %d,%d,%d,%d ROflag:%d\n", vol_id.external_label,
	    drive_id.panel_id.lsm_id.acs, drive_id.panel_id.lsm_id.lsm,
	    drive_id.panel_id.panel, drive_id.drive, use_read_only_flag);
        tl_tpdaemon.tl_log( &tl_tpdaemon, 111, 7,
                            "func",       TL_MSG_PARAM_STR, func,
                            "vol_id",     TL_MSG_PARAM_STR, vol_id.external_label,
                            "drive_id_0", TL_MSG_PARAM_INT, drive_id.panel_id.lsm_id.acs,
                            "drive_id_1", TL_MSG_PARAM_INT, drive_id.panel_id.lsm_id.lsm,
                            "drive_id_2", TL_MSG_PARAM_INT, drive_id.panel_id.panel,
                            "drive_id_3", TL_MSG_PARAM_INT, drive_id.drive,
                            "R0flag",     TL_MSG_PARAM_INT, use_read_only_flag );
	if ((status = acs_mount (++myseqnum, NO_LOCK_ID, vol_id, drive_id,
                                (ring || !use_read_only_flag) ? FALSE : TRUE, 0))) {
		sprintf (msg, TP041, action, cur_vid, cur_unm, acsstatus (status));
		usrmsg (func, "%s\n", msg);
                tl_tpdaemon.tl_log( &tl_tpdaemon, 41, 5,
                                    "func",      TL_MSG_PARAM_STR, func,
                                    "action",    TL_MSG_PARAM_STR, action,
                                    "cur_vid",   TL_MSG_PARAM_STR, cur_vid,
                                    "cur_unm",   TL_MSG_PARAM_STR, cur_unm,
                                    "acsstatus", TL_MSG_PARAM_STR, acsstatus( status ) );
		c = acserr2act (0, status);
		RETURN (c);
	}
	tplogit (func,"acs_mount Ok - returned %s\n", acs_status(status));
        tl_tpdaemon.tl_log( &tl_tpdaemon, 111, 3,
                            "func",     TL_MSG_PARAM_STR, func,
                            "Message",  TL_MSG_PARAM_STR, "acs_mount Ok",
                            "returned", TL_MSG_PARAM_STR, acs_status(status) );
	mount_req_id = -1;	/* no request id assigned by ACSLS yet */
	RETURN (0);
}

int acsdismount(vid, loader, force)
char *vid;
char *loader;
unsigned int force;
{
	int c;
	DRIVEID drive_id;
	char func[16];
	SEQ_NO myseqnum = 0;
	REQ_ID req_id;
	ACS_RESPONSE_TYPE rtype;
	SEQ_NO s;
	STATUS status;
	VOLID vol_id;

	ENTRY (acsdismount);
	strcpy (acsloader, loader);
	strcpy (vol_id.external_label, vid);
	drive_id.panel_id.lsm_id.acs = atoi (strtok (&acsloader[3], ","));
	drive_id.panel_id.lsm_id.lsm = atoi (strtok (NULL, ","));
	drive_id.panel_id.panel = atoi (strtok (NULL, ","));
	drive_id.drive = atoi (strtok (NULL, ","));
	tplogit (func, "vol_id = %s drive_id = %d,%d,%d,%d %s\n", vol_id.external_label,
	    drive_id.panel_id.lsm_id.acs, drive_id.panel_id.lsm_id.lsm,
	    drive_id.panel_id.panel, drive_id.drive, force ? "force" : "");
        tl_tpdaemon.tl_log( &tl_tpdaemon, 111, 7,
                            "func",       TL_MSG_PARAM_STR, func,
                            "vol_id",     TL_MSG_PARAM_STR, vol_id.external_label,
                            "drive_id_0", TL_MSG_PARAM_INT, drive_id.panel_id.lsm_id.acs,
                            "drive_id_1", TL_MSG_PARAM_INT, drive_id.panel_id.lsm_id.lsm,
                            "drive_id_2", TL_MSG_PARAM_INT, drive_id.panel_id.panel,
                            "drive_id_3", TL_MSG_PARAM_INT, drive_id.drive,
                            "Message",    TL_MSG_PARAM_STR, force ? "force" : "" );
	if ((status = acs_dismount (++myseqnum, NO_LOCK_ID, vol_id, drive_id, force))) {
		sprintf (msg, TP041, action, cur_vid, cur_unm, acsstatus (status));
		usrmsg (func, "%s\n", msg);
                tl_tpdaemon.tl_log( &tl_tpdaemon, 41, 5,
                                    "func",      TL_MSG_PARAM_STR, func,
                                    "action",    TL_MSG_PARAM_STR, action,
                                    "cur_vid",   TL_MSG_PARAM_STR, cur_vid,
                                    "cur_unm",   TL_MSG_PARAM_STR, cur_unm,
                                    "acsstatus", TL_MSG_PARAM_STR, acsstatus( status ) );
		c = acserr2act (1, status);
		RETURN (c);
	}
	tplogit (func,"acs_dismount Ok returned %s\n", acs_status(status));
        tl_tpdaemon.tl_log( &tl_tpdaemon, 111, 3,
                            "func",     TL_MSG_PARAM_STR, func,
                            "Message",  TL_MSG_PARAM_STR, "acs_dismount Ok",
                            "returned", TL_MSG_PARAM_STR, acs_status(status) );
	dismount_req_id = -1;	/* no request id assigned by ACSLS yet */
	do {
		status = acs_response (UCHECKI, &s, &req_id, &rtype, rbuf);
		tplogit (func, "acs_response returned %d/%s reqid:%d rtype:%d\n", 
			 status, acs_status(status), req_id, rtype);
                tl_tpdaemon.tl_log( &tl_tpdaemon, 111, 6,
                                    "func",       TL_MSG_PARAM_STR, func,
                                    "Message",    TL_MSG_PARAM_STR, "acs_response returned",
                                    "returned_1", TL_MSG_PARAM_INT, status,
                                    "returned_2", TL_MSG_PARAM_STR, acs_status(status),
                                    "reqid",      TL_MSG_PARAM_INT, req_id, 
                                    "rtype",      TL_MSG_PARAM_INT, rtype );                                    
		if (rtype == RT_ACKNOWLEDGE) {
			dismount_req_id = req_id;
			tplogit (func, "ACSLS req_id = %d\n", dismount_req_id);
                        tl_tpdaemon.tl_log( &tl_tpdaemon, 111, 3,
                                            "func",            TL_MSG_PARAM_STR, func,
                                            "Message",         TL_MSG_PARAM_STR, "ACSLS",
                                            "dismount req_id", TL_MSG_PARAM_INT, dismount_req_id );
		}
	} while (rtype != RT_FINAL);
	dismount_req_id = 0;
	if (status) {
	        sprintf (msg, TP041, action, cur_vid, cur_unm, acsstatus (status));
		usrmsg (func, "%s\n", msg);
                tl_tpdaemon.tl_log( &tl_tpdaemon, 41, 5,
                                    "func",      TL_MSG_PARAM_STR, func,
                                    "action",    TL_MSG_PARAM_STR, action,
                                    "cur_vid",   TL_MSG_PARAM_STR, cur_vid,
                                    "cur_unm",   TL_MSG_PARAM_STR, cur_unm,
                                    "acsstatus", TL_MSG_PARAM_STR, acsstatus( status ) );
		c = acserr2act (1, status);
		RETURN (c);
	}

	/* Added by BC 20050317 to process status code inside the rbuf */
	{
	  ACS_DISMOUNT_RESPONSE *dr;

	  dr = (ACS_DISMOUNT_RESPONSE *)rbuf;

	  if (dr->dismount_status) {
	    sprintf (msg, TP041, action, cur_vid, cur_unm, acsstatus (dr->dismount_status));
	    usrmsg (func, "%s\n", msg);
            tl_tpdaemon.tl_log( &tl_tpdaemon, 41, 5,
                                "func",      TL_MSG_PARAM_STR, func,
                                "action",    TL_MSG_PARAM_STR, action,
                                "cur_vid",   TL_MSG_PARAM_STR, cur_vid,
                                "cur_unm",   TL_MSG_PARAM_STR, cur_unm,
                                "acsstatus", TL_MSG_PARAM_STR, acsstatus (dr->dismount_status) );           
	    c = acserr2act (1, dr->dismount_status);
	    RETURN (c);
	  }
	}
	RETURN (0);
}

int acsmountresp()
{
	int c;
	char func[16];
	REQ_ID req_id;
	ACS_RESPONSE_TYPE rtype;
	SEQ_NO s;
	STATUS status;

	strcpy (func, "acsmountresp");
	status = acs_response (0, &s, &req_id, &rtype, rbuf);
	tplogit (func, "acs_response returned %d/%s reqid:%d rtype:%d\n", 
		 status, acs_status(status), req_id, rtype);
        tl_tpdaemon.tl_log( &tl_tpdaemon, 111, 6,
                            "func",       TL_MSG_PARAM_STR, func,
                            "Message",    TL_MSG_PARAM_STR, "acs_response returned",
                            "returned_1", TL_MSG_PARAM_INT, status,
                            "returned_2", TL_MSG_PARAM_STR, acs_status(status),
                            "reqid",      TL_MSG_PARAM_INT, req_id, 
                            "rtype",      TL_MSG_PARAM_INT, rtype );                                    
	if (status == STATUS_PENDING)
		return (0);
	if (rtype == RT_ACKNOWLEDGE) {
		mount_req_id = req_id;
		tplogit (func, "ACSLS req_id = %d\n", mount_req_id);
                tl_tpdaemon.tl_log( &tl_tpdaemon, 111, 3,
                                    "func",         TL_MSG_PARAM_STR, func,
                                    "Message",      TL_MSG_PARAM_STR, "ACSLS",
                                    "mount req_id", TL_MSG_PARAM_INT, mount_req_id );
		return (0);
	}
	/* final status */
	mount_req_id = 0;
	if (status) {
		sprintf (msg, TP041, action, cur_vid, cur_unm, acsstatus (status));
		usrmsg (func, "%s\n", msg);
                tl_tpdaemon.tl_log( &tl_tpdaemon, 41, 5,
                                    "func",      TL_MSG_PARAM_STR, func,
                                    "action",    TL_MSG_PARAM_STR, action,
                                    "cur_vid",   TL_MSG_PARAM_STR, cur_vid,
                                    "cur_unm",   TL_MSG_PARAM_STR, cur_unm,
                                    "acsstatus", TL_MSG_PARAM_STR, acsstatus( status ) );
		c = acserr2act (0, status);
		RETURN (c);
	}

	/* Added by BC 20050317 to process status code inside the rbuf */
	{
	  ACS_MOUNT_RESPONSE *dr;

	  dr = (ACS_MOUNT_RESPONSE *)rbuf;

	  if (dr->mount_status) {
	    sprintf (msg, TP041, action, cur_vid, cur_unm, acsstatus (dr->mount_status));
	    usrmsg (func, "%s\n", msg);
            tl_tpdaemon.tl_log( &tl_tpdaemon, 41, 5,
                                "func",      TL_MSG_PARAM_STR, func,
                                "action",    TL_MSG_PARAM_STR, action,
                                "cur_vid",   TL_MSG_PARAM_STR, cur_vid,
                                "cur_unm",   TL_MSG_PARAM_STR, cur_unm,
                                "acsstatus", TL_MSG_PARAM_STR, acsstatus( dr->mount_status ) );
	    c = acserr2act (1, dr->mount_status);
	    RETURN (c);
	  }
	}

	return (0);
}

int wait4acsfinalresp()
{
	int c;
	char func[16];
	REQ_ID req_id;
	ACS_RESPONSE_TYPE rtype;
	SEQ_NO s;
	STATUS status;

	if (mount_req_id == 0 && dismount_req_id == 0)
		return (0);
	strcpy (func, "wait4acsfinalr");
	do {
		status = acs_response (UCHECKI, &s, &req_id, &rtype, rbuf);
		tplogit (func, "acs_response returned %d/%s reqid:%d rtype:%d\n", 
			 status, acs_status(status), req_id, rtype);
                tl_tpdaemon.tl_log( &tl_tpdaemon, 111, 6,
                                    "func",       TL_MSG_PARAM_STR, func,
                                    "Message",    TL_MSG_PARAM_STR, "acs_response returned",
                                    "returned_1", TL_MSG_PARAM_INT, status,
                                    "returned_2", TL_MSG_PARAM_STR, acs_status(status),
                                    "reqid",      TL_MSG_PARAM_INT, req_id, 
                                    "rtype",      TL_MSG_PARAM_INT, rtype );                                    
		if (rtype == RT_ACKNOWLEDGE) {
			tplogit (func, "ACSLS req_id = %d\n", req_id);
                        tl_tpdaemon.tl_log( &tl_tpdaemon, 111, 3,
                                            "func",    TL_MSG_PARAM_STR, func,
                                            "Message", TL_MSG_PARAM_STR, "ACSLS",
                                            "req_id",  TL_MSG_PARAM_INT, req_id );
		}
	} while (rtype != RT_FINAL);
	mount_req_id = 0;
	dismount_req_id = 0;
	if (status) {
		sprintf (msg, TP041, action, cur_vid, cur_unm, acsstatus (status));
		usrmsg (func, "%s\n", msg);
                tl_tpdaemon.tl_log( &tl_tpdaemon, 41, 5,
                                    "func",      TL_MSG_PARAM_STR, func,
                                    "action",    TL_MSG_PARAM_STR, action,
                                    "cur_vid",   TL_MSG_PARAM_STR, cur_vid,
                                    "cur_unm",   TL_MSG_PARAM_STR, cur_unm,
                                    "acsstatus", TL_MSG_PARAM_STR, acsstatus( status ) );
		c = acserr2act (*action == 'm' ? 0 : 1, status);
		RETURN (c);
	}

	/* Added by BC 20050317 to process status code inside the rbuf */
	{
	  ACS_MOUNT_RESPONSE *dr;

	  dr = (ACS_MOUNT_RESPONSE *)rbuf;

	  if (dr->mount_status) {
	    sprintf (msg, TP041, action, cur_vid, cur_unm, acsstatus (dr->mount_status));
	    usrmsg (func, "%s\n", msg);
            tl_tpdaemon.tl_log( &tl_tpdaemon, 41, 5,
                                "func",      TL_MSG_PARAM_STR, func,
                                "action",    TL_MSG_PARAM_STR, action,
                                "cur_vid",   TL_MSG_PARAM_STR, cur_vid,
                                "cur_unm",   TL_MSG_PARAM_STR, cur_unm,
                                "acsstatus", TL_MSG_PARAM_STR, acsstatus( dr->mount_status ) );
	    c = acserr2act (1, dr->mount_status);
	    RETURN (c);
	  }
	}
	return (0);
}

#endif
#if DMSCAPI
static int hostid = -1;

fbserr2act(req_type, cc)
int req_type;	/* 0 --> mount, 1 --> dismount */
int cc;		/* error returned by the mount/dismount routine */
{
	struct rbterr_codact dmserr_acttbl[] = {
	  EFBS_COM_NOTEXE, RBT_OMSGR, RBT_OMSGR,	/* FBS not running */
	  EFBS_DMS_INIT, RBT_FAST_RETRY, RBT_FAST_RETRY, /* DMS initializing */
	  EFBS_DMS_DOPEN, RBT_OMSGR, RBT_OMSGR,		/* DMS door open */
	  EFBS_ME_DOROPN1, RBT_OMSGR, RBT_OMSGR,	/* door open 1 */
	  EFBS_DMP_DIRUSE, RBT_DMNT_FORCE, RBT_DMNT_FORCE, /* DIR is in use */
	  EFBS_DMP_NOSHCAS, RBT_OMSG_NORTRY, RBT_OK,	/* no such cassette */
	  EFBS_DMP_NOTREDY, RBT_OMSGR, RBT_OMSGR,	/* DMS not ready */
	  EFBS_DMP_DIRINTO, RBT_OMSGR, RBT_OMSGR,	/* DIR IN timeout */
	  EFBS_DMP_UKNCASS, RBT_DMNT_FORCE, RBT_DMNT_FORCE, /* unknown cassette */
	  EFBS_DMP_LOCAL, RBT_OMSGR, RBT_OMSGR,	/* DIR remote selector is OFF */
	  EFBS_PSY_STOP, RBT_OMSGR, RBT_OMSGR,		/* FBS will stop soon */
	};
	int i;

	for (i = 0; i < sizeof(dmserr_acttbl)/sizeof(struct rbterr_codact); i++) {
		if (cc == dmserr_acttbl[i].cc)
			return ((req_type == 0) ?
				dmserr_acttbl[i].mnt_fail_action :
				dmserr_acttbl[i].dmnt_fail_action);
	}
	return (RBT_NORETRY);
}

mountfbs(vid, loader)
char *vid;
char *loader;
{
	int c;
	char cassette[8];
	int dirno;
	char func[16];
	char msgbuf[128];

	ENTRY (mountfbs);
	if (hostid < 0) {
		if ((hostid = fbsinit (NULL)) < 0) {
			usrmsg (func, TP042, loader, "fbsinit",
				fbsstrerror (fbs_errno, msgbuf));
                        tl_tpdaemon.tl_log( &tl_tpdaemon, 42, 4,
                                            "func",    TL_MSG_PARAM_STR, func,
                                            "loader",  TL_MSG_PARAM_STR, loader,
                                            "Message", TL_MSG_PARAM_STR, "fbsinit", 
                                            "Error",   TL_MSG_PARAM_STR, fbsstrerror( fbs_errno, msgbuf ) );
			RETURN (-errno);
		}
	}
	sprintf (cassette, "C%s", vid);
	dirno = atoi (loader+1) - 1;
	if (dmsthread (hostid, cassette, dirno) < 0) {
		sprintf (msg, TP041, action, cur_vid, cur_unm,
			fbsstrerror (fbs_errno, msgbuf));
		usrmsg (func, "%s\n", msg);
                tl_tpdaemon.tl_log( &tl_tpdaemon, 41, 5,
                                    "func",    TL_MSG_PARAM_STR, func,
                                    "action",  TL_MSG_PARAM_STR, action,
                                    "cur_vid", TL_MSG_PARAM_STR, cur_vid,
                                    "cur_unm", TL_MSG_PARAM_STR, cur_unm,
                                    "Error",   TL_MSG_PARAM_STR, fbsstrerror( fbs_errno, msgbuf ) );
		c = fbserr2act (0, fbs_errno);
		RETURN (c);
	}
	RETURN (0);
}

dismountfbs(vid, loader, force)
char *vid;
char *loader;
unsigned int force;
{
	int c;
	char cassette[8];
	char func[16];
	char msgbuf[128];

	ENTRY (dismountfbs);
	if (hostid < 0) {
		if ((hostid = fbsinit (NULL)) < 0) {
			usrmsg (func, TP042, loader, "fbsinit",
				fbsstrerror (fbs_errno, msgbuf));
                        tl_tpdaemon.tl_log( &tl_tpdaemon, 42, 4,
                                            "func",    TL_MSG_PARAM_STR, func,
                                            "loader",  TL_MSG_PARAM_STR, loader,
                                            "Message", TL_MSG_PARAM_STR, "fbsinit", 
                                            "Error",   TL_MSG_PARAM_STR, fbsstrerror( fbs_errno, msgbuf ) );
			RETURN (-errno);
		}
	}
	if (! force)
		sprintf (cassette, "C%s", vid);
	else
		strcpy (cassette, loader);
	if (dmsuthread (hostid, cassette) < 0) {
		sprintf (msg, TP041, action, cur_vid, cur_unm,
			fbsstrerror (fbs_errno, msgbuf));
		usrmsg (func, "%s\n", msg);
                tl_tpdaemon.tl_log( &tl_tpdaemon, 41, 5,
                                    "func",    TL_MSG_PARAM_STR, func,
                                    "action",  TL_MSG_PARAM_STR, action,
                                    "cur_vid", TL_MSG_PARAM_STR, cur_vid,
                                    "cur_unm", TL_MSG_PARAM_STR, cur_unm,
                                    "Error",   TL_MSG_PARAM_STR, fbsstrerror( fbs_errno, msgbuf ) );
		c = fbserr2act (1, fbs_errno);
		RETURN (c);
	}
	RETURN (0);
}

closefbs(loader)
char *loader;
{
	char func[16];
	char msgbuf[128];

	ENTRY (closefbs);
	if (hostid >= 0) {
		if (fbsterm (hostid) < 0) {
			usrmsg (func, TP042, loader, "fbsterm",
				fbsstrerror (fbs_errno, msgbuf));
                        tl_tpdaemon.tl_log( &tl_tpdaemon, 42, 4,
                                            "func",    TL_MSG_PARAM_STR, func,
                                            "loader",  TL_MSG_PARAM_STR, loader,
                                            "Message", TL_MSG_PARAM_STR, "fbsterm", 
                                            "Error",   TL_MSG_PARAM_STR, fbsstrerror( fbs_errno, msgbuf ) );
			RETURN (-EIO);
		}
	}
	RETURN (0);
}
#endif
/*
 * Code for DEC Media Changer (TL820) robots. Works together with
 * the dmcserv server. Depends on the "dmc.h" include file.
 */
extern char *getconfent();
int dmcmount(vid,unm,loader)
char *vid;
char *unm;
char *loader;
{
	DMCrequest_t req;
	DMCreply_t rep;
	char func[16];
	int s,rc;

	ENTRY(dmcmount);
	memset(&req,'\0',sizeof(DMCrequest_t));
	memset(&rep,'\0',sizeof(DMCreply_t));
	req.reqtype = DMC_MOUNT;
	strcpy(req.vid,vid);
	strcpy(req.loader,loader);
	strcat(req.loader,",");
	strcat(req.loader,unm);
	rc = send2dmc(&s,&req);
	if ( rc ) RETURN(rc);
	rc = fromdmc(&s,&rep);
	if ( rc ) RETURN(rc);
	if  ( rep.log_info != NULL && rep.status && rep.log_info_l ) {
		char *p = strtok(rep.log_info,"\n");
		char *last = rep.log_info;
		/* Print only the last line. Otherwise tplogit may truncate the output. */
		while (p != NULL && (p=strtok(NULL,"\n")) != NULL ) last = p; 
		if ( last != NULL && *last != '\0' ) {
			sprintf(msg,TP041,action, cur_vid, cur_unm, last);
			usrmsg(func,"%s\n",msg); 
                        tl_tpdaemon.tl_log( &tl_tpdaemon, 41, 5,
                                            "func",    TL_MSG_PARAM_STR, func,
                                            "action",  TL_MSG_PARAM_STR, action,
                                            "cur_vid", TL_MSG_PARAM_STR, cur_vid,
                                            "cur_unm", TL_MSG_PARAM_STR, cur_unm,
                                            "last",    TL_MSG_PARAM_STR, last );
		}
	}
	if ( rep.log_info != NULL && rep.log_info_l ) free(rep.log_info);
	RETURN(rep.status);
}

int dmcdismount(vid, unm, loader, force)
char *vid;
char *unm;
char *loader;
unsigned int force;
{
	DMCrequest_t req;
	DMCreply_t rep;
	char func[16];
	int s,rc;

	ENTRY(dmcdismount);
	memset(&req,'\0',sizeof(DMCrequest_t));
	memset(&rep,'\0',sizeof(DMCreply_t));
	req.reqtype = DMC_UNMOUNT;
	req.jid = getpid();
	if ( !force ) strcpy(req.vid,vid);
	strcpy(req.loader,loader);
	strcat(req.loader,",");
	strcat(req.loader,unm);
	tplogit(func,"vol_id = %s drive_id = %s %s\n",req.vid,req.loader,force ? "force" : "");
        tl_tpdaemon.tl_log( &tl_tpdaemon, 111, 4,
                            "func",     TL_MSG_PARAM_STR, func,
                            "vol_id",   TL_MSG_PARAM_STR, req.vid,req,
                            "drive_id", TL_MSG_PARAM_STR, req.loader,
                            "Message",  TL_MSG_PARAM_STR, force ? "force" : "" );
	rc = send2dmc(&s,&req);
	if ( rc ) RETURN(rc);
	rc = fromdmc(&s,&rep);
	if ( rc ) RETURN(rc);
	if ( rep.log_info != NULL && rep.status && rep.log_info_l ) {
		char *p = strtok(rep.log_info,"\n");
		char *last = rep.log_info;
		/* Print only the last line. Otherwise tplogit may truncate the output. */
		while (p != NULL && (p=strtok(NULL,"\n")) != NULL ) last = p; 
		if ( last != NULL && *last != '\0' ) {
			sprintf(msg, TP041, action, cur_vid, cur_unm, last);
			usrmsg(func,"%s\n",msg);
                        tl_tpdaemon.tl_log( &tl_tpdaemon, 41, 5,
                                            "func",    TL_MSG_PARAM_STR, func,
                                            "action",  TL_MSG_PARAM_STR, action,
                                            "cur_vid", TL_MSG_PARAM_STR, cur_vid,
                                            "cur_unm", TL_MSG_PARAM_STR, cur_unm,
                                            "last",    TL_MSG_PARAM_STR, last );
		}
	}
	if ( rep.log_info != NULL && rep.log_info_l ) free(rep.log_info);
	RETURN(rep.status);
}
int send2dmc(sock,req)
int *sock;
DMCrequest_t *req;
{
#if SERVICESDB
	struct servent *sp;
#endif
	struct hostent *hp;
	struct sockaddr_in sin;
	int s,j;
	char *p;
	char *dmc_host;
	char func[16];
	int dmc_port = DMC_PORT;

	ENTRY(send2dmc);
	req->magic = C_MAGIC;
	req->jid = getpid();
	req->cartridge_side = 1;
	dmc_host = NULL;
	if ( (p = getenv("DMC_HOST")) == NULL ) {
		if ( (p = getconfent("DMC","HOST",0)) == NULL ) {
			dmc_host = (char *)malloc(strlen(DMC_HOST)+1);
			strcpy(dmc_host,DMC_HOST);
		}
	}
	if ( dmc_host == NULL ) {
		dmc_host = (char *)malloc(strlen(p)+1);
		strcpy(dmc_host,p);
	}
	if ( (p = getenv("DMC_PORT")) != NULL ) dmc_port = atoi(p);
#if SERVICESDB
	else {
		if ( (sp = getservbyname(DMC_NAME,DMC_PROTO)) == NULL ) {
			tplogit(func,"getservbyname: %s\n",neterror());
                        tl_tpdaemon.tl_log( &tl_tpdaemon, 103, 3,
                                            "func",    TL_MSG_PARAM_STR, func,
                                            "Message", TL_MSG_PARAM_STR, "getservbyname",
                                            "Error",   TL_MSG_PARAM_STR, neterror() );
			RETURN(RBT_FAST_RETRY);
		}
		dmc_port = ntohs(sp->s_port);
	}
#endif
	sin.sin_family = AF_INET;
	if ( (hp = gethostbyname(dmc_host)) == NULL ) {
		tplogit(func,"gethostbyname: %s\n",neterror());
                tl_tpdaemon.tl_log( &tl_tpdaemon, 103, 3,
                                    "func",    TL_MSG_PARAM_STR, func,
                                    "Message", TL_MSG_PARAM_STR, "gethostbyname",
                                    "Error",   TL_MSG_PARAM_STR, neterror() );
		free(dmc_host);
		RETURN(RBT_FAST_RETRY);
	}
	sin.sin_addr.s_addr = ((struct in_addr *)(hp->h_addr))->s_addr;
	sin.sin_port = htons(dmc_port);
	if ((s = socket(AF_INET,SOCK_STREAM,0)) == -1) {
		tplogit(func,"socket: %s\n",neterror());
                tl_tpdaemon.tl_log( &tl_tpdaemon, 103, 3,
                                    "func",    TL_MSG_PARAM_STR, func,
                                    "Message", TL_MSG_PARAM_STR, "socket",
                                    "Error",   TL_MSG_PARAM_STR, neterror() );
		free(dmc_host);
		RETURN(RBT_FAST_RETRY);
	}
	if ( connect(s,(struct sockaddr *)&sin,sizeof(struct sockaddr_in)) == -1 ){
		tplogit(func,"connect: %s\n",neterror());
                tl_tpdaemon.tl_log( &tl_tpdaemon, 103, 3,
                                    "func",    TL_MSG_PARAM_STR, func,
                                    "Message", TL_MSG_PARAM_STR, "connect",
                                    "Error",   TL_MSG_PARAM_STR, neterror() );
		shutdown(s,2);
		close(s);
		free(dmc_host);
		RETURN(RBT_FAST_RETRY);
	}
	req->magic = htonl(req->magic);
	req->jid = htonl(req->jid);
	req->reqtype = htonl(req->reqtype);
	req->cartridge_side = htons(req->cartridge_side);
	j = sizeof(DMCrequest_t);
	if ( send(s,(char *)req,j,0) != j ) {
		tplogit(func,"send: %s\n",neterror());
                tl_tpdaemon.tl_log( &tl_tpdaemon, 103, 3,
                                    "func",    TL_MSG_PARAM_STR, func,
                                    "Message", TL_MSG_PARAM_STR, "send",
                                    "Error",   TL_MSG_PARAM_STR, neterror() );
		shutdown(s,2);
		close(s);
		free(dmc_host);
		RETURN(RBT_FAST_RETRY);
	}
	free(dmc_host);
	*sock = s;
	RETURN(0);
}

int fromdmc(sock,rep)
int *sock;
DMCreply_t *rep;
{
	int s = *sock;
	int j,ntot;
	char func[16];

	ENTRY(fromdmc);
	if ( (j = recv(s,(char *)rep,sizeof(DMCreply_t),0)) != sizeof(DMCreply_t) ) {
		tplogit(func,"recv: %s\n",neterror());
                tl_tpdaemon.tl_log( &tl_tpdaemon, 103, 3,
                                    "func",    TL_MSG_PARAM_STR, func,
                                    "Message", TL_MSG_PARAM_STR, "recv",
                                    "Error",   TL_MSG_PARAM_STR, neterror() );
		shutdown(s,2);
		close(s);
		RETURN(RBT_FAST_RETRY);
	}
	rep->magic = ntohl(rep->magic);
	rep->status = ntohl(rep->status);
	rep->log_info_l = ntohl(rep->log_info_l);
	if ( rep->log_info_l ) {
		rep->log_info = (char *)malloc(sizeof(char)*(rep->log_info_l+1));
		memset(rep->log_info,'\0',sizeof(char)*rep->log_info_l+1);
		ntot = 0;
		do {
			if ( (j = recv(s,(char *)&rep->log_info[ntot],rep->log_info_l-ntot,0)) < 0 ) {
	tplogit(func,"recv: %s\n",neterror());
        tl_tpdaemon.tl_log( &tl_tpdaemon, 103, 3,
                            "func",    TL_MSG_PARAM_STR, func,
                            "Message", TL_MSG_PARAM_STR, "recv",
                            "Error",   TL_MSG_PARAM_STR, neterror() );
	free(rep->log_info);
	shutdown(s,2);
	close(s);
	RETURN(RBT_FAST_RETRY);
			}
			ntot+=j;
		} while (ntot < rep->log_info_l);
	}
	shutdown(s,2);
	close(s);
	if ( rep->magic != S_MAGIC ) {
		tplogit(func,"Wrong magic number (0x%x) from DMC server. Should be 0x%x\n",rep->magic,S_MAGIC);
                {
                        char cur[16], exp[16];
                        sprintf( cur, "0x%x", rep->magic );
                        sprintf( exp, "0x%x", S_MAGIC );
                        tl_tpdaemon.tl_log( &tl_tpdaemon, 103, 4,
                                            "func",     TL_MSG_PARAM_STR, func,
                                            "Message",  TL_MSG_PARAM_STR, "Wrong magic number from DMC server",
                                            "Current",  TL_MSG_PARAM_STR, cur,
                                            "Expected", TL_MSG_PARAM_STR, exp );
                }
		if ( rep->log_info_l ) free(rep->log_info);
		RETURN(RBT_NORETRY);
	}
	RETURN(0);
}

static int drvord;
static int got_robot_info = 0;
static char *rmc_host = NULL;
static int smc_fd = -1;
static char rmc_errbuf[256];
static char smc_ldr[CA_MAXRBTNAMELEN+6];
static struct robot_info robot_info;

int opensmc(loader)
char *loader;
{
	int c;
	char *dp;
	char func[16];
	char *msgaddr;
	char *p;
	struct smc_status smc_status;

	ENTRY (opensmc);
	sprintf (smc_ldr, "/dev/%s", loader);
	if ((p = strchr (smc_ldr, ',')) == 0) {
		usrmsg (func, "TP041 - %s of %s on %s failed : %s\n", action,
		    cur_vid, cur_unm, "invalid loader");
                tl_tpdaemon.tl_log( &tl_tpdaemon, 41, 5,
                                    "func",    TL_MSG_PARAM_STR, func,
                                    "action",  TL_MSG_PARAM_STR, action,
                                    "cur_vid", TL_MSG_PARAM_STR, cur_vid,
                                    "cur_unm", TL_MSG_PARAM_STR, cur_unm,
                                    "Message", TL_MSG_PARAM_STR, "invalid loader" );
		RETURN (RBT_NORETRY);
	}
	*p = '\0';
	drvord = strtol (p + 1, &dp, 10);
	if (*dp != '\0' || drvord < 0) {
		usrmsg (func, "TP041 - %s of %s on %s failed : %s\n", action,
		    cur_vid, cur_unm, "invalid loader");
                tl_tpdaemon.tl_log( &tl_tpdaemon, 41, 5,
                                    "func",    TL_MSG_PARAM_STR, func,
                                    "action",  TL_MSG_PARAM_STR, action,
                                    "cur_vid", TL_MSG_PARAM_STR, cur_vid,
                                    "cur_unm", TL_MSG_PARAM_STR, cur_unm,
                                    "Message", TL_MSG_PARAM_STR, "invalid loader" );
		RETURN (RBT_NORETRY);
	}
	if ((p = strchr (smc_ldr, '@'))) {
		*p = '\0';
		rmc_host = p + 1;
	}
	if (rmc_host) {
		(void) rmc_seterrbuf (rmc_errbuf, sizeof(rmc_errbuf));
		RETURN (0);
	}
#if defined(SOLARIS25) || defined(hpux)
        /* open the SCSI picker device
           (open is done in send_scsi_cmd for the other platforms */
 
        if ((smc_fd = open (smc_ldr, O_RDWR)) < 0) {
		if (errno == EBUSY)
			c = RBT_FAST_RETRY;
		else
			c = RBT_NORETRY;
                usrmsg (func, TP042, smc_ldr, "open", strerror(errno));
                tl_tpdaemon.tl_log( &tl_tpdaemon, 42, 4,
                                    "func",    TL_MSG_PARAM_STR, func,
                                    "smc_ldr", TL_MSG_PARAM_STR, smc_ldr,
                                    "Message", TL_MSG_PARAM_STR, "open", 
                                    "Error",   TL_MSG_PARAM_STR, strerror( errno ) );
		RETURN (c);
        }
#endif

	/* get robot geometry */

	if (! got_robot_info) {
		if ((c = smc_get_geometry (smc_fd, smc_ldr, &robot_info))) {
			c = smc_lasterror (&smc_status, &msgaddr);
			if (smc_status.rc == -1 || smc_status.rc == -2) {
				usrmsg (func, "%s\n", msg);
                                tl_tpdaemon.tl_log( &tl_tpdaemon, 103, 2,
                                                    "func",    TL_MSG_PARAM_STR, func,
                                                    "Message", TL_MSG_PARAM_STR, msg ); 
                        } else {
				usrmsg (func, TP042, smc_ldr, "get_geometry",
					strrchr (msgaddr, ':') + 2);
                                tl_tpdaemon.tl_log( &tl_tpdaemon, 42, 4,
                                                    "func",    TL_MSG_PARAM_STR, func,
                                                    "smc_ldr", TL_MSG_PARAM_STR, smc_ldr,
                                                    "Message", TL_MSG_PARAM_STR, "get_geometry", 
                                                    "Error",   TL_MSG_PARAM_STR, strrchr (msgaddr, ':') + 2 );
                        }
			RETURN (c);
		}
		got_robot_info = 1;
	}

	if (drvord >= robot_info.device_count) {
		usrmsg (func, "TP041 - %s of %s on %s failed : %s\n", action,
		    cur_vid, cur_unm, "invalid loader");
                tl_tpdaemon.tl_log( &tl_tpdaemon, 41, 5,
                                    "func",    TL_MSG_PARAM_STR, func,
                                    "action",  TL_MSG_PARAM_STR, action,
                                    "cur_vid", TL_MSG_PARAM_STR, cur_vid,
                                    "cur_unm", TL_MSG_PARAM_STR, cur_unm,
                                    "Message", TL_MSG_PARAM_STR, "invalid loader" );
		RETURN (RBT_NORETRY);
	}
	RETURN (0);
}

int smcmount(vid, side, loader, vsnretry)
char *vid;
int side;
char *loader;
int vsnretry;
{
	int c;
	struct smc_element_info element_info;
	char func[16];
	char *msgaddr;
	char *p;
	struct smc_status smc_status;

	ENTRY (smcmount);
        
	if ((c = opensmc (loader)) != 0)
		RETURN (c);
	if (rmc_host) {
		c = rmc_mount (rmc_host, smc_ldr, vid, side, drvord);
                if (c == EBUSY) {
                        RETURN(RBT_FAST_RETRY);
                } else if (c) {
			p = strrchr (rmc_errbuf, ':');
			sprintf (msg, TP041, "mount", vid, cur_unm,
                                 p ? p + 2 : rmc_errbuf);
                        /* Just send message to operator after two retries*/ 
                        if (vsnretry > 3 && (serrno == SECOMERR)) {  
                                usrmsg (func, "%s\n", msg);
                                tl_tpdaemon.tl_log( &tl_tpdaemon, 41, 5,
                                                    "func",    TL_MSG_PARAM_STR, func,
                                                    "action",  TL_MSG_PARAM_STR, "mount",
                                                    "cur_vid", TL_MSG_PARAM_STR, vid,
                                                    "cur_unm", TL_MSG_PARAM_STR, cur_unm,
                                                    "Message", TL_MSG_PARAM_STR, p ? p + 2 : rmc_errbuf );
                        } else if (vsnretry <= 3 && (serrno == SECOMERR)) {
                                tplogit (func, "%s", msg);
                                tl_tpdaemon.tl_log( &tl_tpdaemon, 41, 5,
                                                    "func",    TL_MSG_PARAM_STR, func,
                                                    "action",  TL_MSG_PARAM_STR, "mount",
                                                    "cur_vid", TL_MSG_PARAM_STR, vid,
                                                    "cur_unm", TL_MSG_PARAM_STR, cur_unm,
                                                    "Message", TL_MSG_PARAM_STR, p ? p + 2 : rmc_errbuf );
                        } else {
                                usrmsg (func, "%s\n", msg);
                                tl_tpdaemon.tl_log( &tl_tpdaemon, 41, 5,
                                                    "func",    TL_MSG_PARAM_STR, func,
                                                    "action",  TL_MSG_PARAM_STR, "mount",
                                                    "cur_vid", TL_MSG_PARAM_STR, vid,
                                                    "cur_unm", TL_MSG_PARAM_STR, cur_unm,
                                                    "Message", TL_MSG_PARAM_STR, p ? p + 2 : rmc_errbuf );
                        }
                        c = (serrno == SECOMERR) ? RBT_FAST_RETRY : serrno - ERMCRBTERR;
		}
                RETURN (c);
	}
	if ((c = smc_find_cartridge (smc_fd, smc_ldr, vid, 0, 0, 1, &element_info)) < 0) {
		c = smc_lasterror (&smc_status, &msgaddr);
        if (c == EBUSY) {
		    usrmsg (func, "%s\n", msgaddr);
                    tl_tpdaemon.tl_log( &tl_tpdaemon, 103, 2,
                                        "func",    TL_MSG_PARAM_STR, func,
                                        "msgaddr", TL_MSG_PARAM_STR, msgaddr ); 
                    RETURN(RBT_FAST_RETRY);
		} else if (smc_status.rc == -1 || smc_status.rc == -2) {
			usrmsg (func, "%s\n", msgaddr);
                        tl_tpdaemon.tl_log( &tl_tpdaemon, 103, 2,
                                            "func",    TL_MSG_PARAM_STR, func,
                                            "msgaddr", TL_MSG_PARAM_STR, msgaddr ); 
		} else {
			p = strrchr (msgaddr, ':');
			usrmsg (func, TP042, smc_ldr, "find_cartridge",
				p ? p + 2 : msgaddr);
                        tl_tpdaemon.tl_log( &tl_tpdaemon, 42, 4,
                                            "func",    TL_MSG_PARAM_STR, func,
                                            "smc_ldr", TL_MSG_PARAM_STR, smc_ldr,
                                            "Message", TL_MSG_PARAM_STR, "find_cartridge", 
                                            "Error",   TL_MSG_PARAM_STR, p ? p + 2 : msgaddr );
		}
		RETURN (c);
	}
	if (c == 0) {
		sprintf (msg, TP041, "mount", vid, cur_unm, "volume not in library");
		usrmsg (func, "%s\n", msg);
                tl_tpdaemon.tl_log( &tl_tpdaemon, 41, 5,
                                    "func",    TL_MSG_PARAM_STR, func,
                                    "action",  TL_MSG_PARAM_STR, "mount",
                                    "cur_vid", TL_MSG_PARAM_STR, vid,
                                    "cur_unm", TL_MSG_PARAM_STR, cur_unm,
                                    "Message", TL_MSG_PARAM_STR, "volume not in library" );
		RETURN (RBT_OMSG_NORTRY);
	}
	if (element_info.element_type != 2) {
		sprintf (msg, TP041, "mount", vid, cur_unm, "volume in use");
		usrmsg (func, "%s\n", msg);
                tl_tpdaemon.tl_log( &tl_tpdaemon, 41, 5,
                                    "func",    TL_MSG_PARAM_STR, func,
                                    "action",  TL_MSG_PARAM_STR, "mount",
                                    "cur_vid", TL_MSG_PARAM_STR, vid,
                                    "cur_unm", TL_MSG_PARAM_STR, cur_unm,
                                    "Message", TL_MSG_PARAM_STR, "volume in use" );
		RETURN (RBT_OMSG_SLOW_R);
	}
	if ((c = smc_move_medium (smc_fd, smc_ldr, element_info.element_address,
	    robot_info.device_start+drvord, side)) < 0) {
		c = smc_lasterror (&smc_status, &msgaddr);
        if (c == EBUSY) {
			usrmsg (func, "%s\n", msgaddr);
                        tl_tpdaemon.tl_log( &tl_tpdaemon, 103, 2,
                                        "func",    TL_MSG_PARAM_STR, func,
                                        "msgaddr", TL_MSG_PARAM_STR, msgaddr ); 
                        RETURN (RBT_FAST_RETRY);
		} else if (smc_status.rc == -1 || smc_status.rc == -2){
			usrmsg (func, "%s\n", msgaddr);
                        tl_tpdaemon.tl_log( &tl_tpdaemon, 103, 2,
                                            "func",    TL_MSG_PARAM_STR, func,
                                            "msgaddr", TL_MSG_PARAM_STR, msgaddr ); 
		} else {
			p = strrchr (msgaddr, ':');
			sprintf (msg, TP041, "mount", vid, cur_unm,
				p ? p + 2 : msgaddr);
			usrmsg (func, "%s\n", msg);
                        tl_tpdaemon.tl_log( &tl_tpdaemon, 41, 5,
                                            "func",    TL_MSG_PARAM_STR, func,
                                            "action",  TL_MSG_PARAM_STR, "mount",
                                            "cur_vid", TL_MSG_PARAM_STR, vid,
                                            "cur_unm", TL_MSG_PARAM_STR, cur_unm,
                                            "Message", TL_MSG_PARAM_STR, p ? p + 2 : msgaddr );
                }
		RETURN (c);
	}
	RETURN (0);
}

int smcdismount(vid, loader, force, vsnretry)
char *vid;
char *loader;
int force;
int vsnretry;
{
	int c;
	struct smc_element_info element_info;
	char func[16];
	char *msgaddr;
	char *p;
	struct smc_status smc_status;
	
	ENTRY (smcdismount);
	if ((c = opensmc (loader)) != 0)
		RETURN (c);
	if (rmc_host) {
		c = rmc_dismount (rmc_host, smc_ldr, vid, drvord, force);
        if (c == EBUSY) {
           RETURN (RBT_FAST_RETRY);
        } else if (c) {
			 p = strrchr (rmc_errbuf, ':');
			 sprintf (msg, TP041, "demount", vid, cur_unm,
				  p ? p + 2 : rmc_errbuf);
            /* Just send message to operator after two retries on connection reset*/ 
            if (vsnretry > 3 && (serrno == SECOMERR)) {
			  usrmsg (func, "%s\n", msg);
                          tl_tpdaemon.tl_log( &tl_tpdaemon, 41, 5,
                                              "func",    TL_MSG_PARAM_STR, func,
                                              "action",  TL_MSG_PARAM_STR, "demount",
                                              "cur_vid", TL_MSG_PARAM_STR, vid,
                                              "cur_unm", TL_MSG_PARAM_STR, cur_unm,
                                              "Message", TL_MSG_PARAM_STR, p ? p + 2 : rmc_errbuf );
            } else if (vsnretry <= 3 && (serrno == SECOMERR)){
			   tplogit (func, "%s", msg);
                           tl_tpdaemon.tl_log( &tl_tpdaemon, 41, 5,
                                               "func",    TL_MSG_PARAM_STR, func,
                                               "action",  TL_MSG_PARAM_STR, "demount",
                                               "cur_vid", TL_MSG_PARAM_STR, vid,
                                               "cur_unm", TL_MSG_PARAM_STR, cur_unm,
                                               "Message", TL_MSG_PARAM_STR, p ? p + 2 : rmc_errbuf );
            } else {
			   usrmsg (func, "%s\n", msg);
                           tl_tpdaemon.tl_log( &tl_tpdaemon, 41, 5,
                                               "func",    TL_MSG_PARAM_STR, func,
                                               "action",  TL_MSG_PARAM_STR, "demount",
                                               "cur_vid", TL_MSG_PARAM_STR, vid,
                                               "cur_unm", TL_MSG_PARAM_STR, cur_unm,
                                               "Message", TL_MSG_PARAM_STR, p ? p + 2 : rmc_errbuf );
            }
			c = (serrno == SECOMERR) ? RBT_FAST_RETRY : serrno - ERMCRBTERR;
		}
		RETURN (c);
	}
	if ((c = smc_read_elem_status (smc_fd, smc_ldr, 4,
	    robot_info.device_start+drvord, 1, &element_info)) < 0) {
		c = smc_lasterror (&smc_status, &msgaddr);
        if (c == EBUSY) {
            usrmsg (func, "%s\n", msgaddr);
            tl_tpdaemon.tl_log( &tl_tpdaemon, 103, 2,
                                "func",    TL_MSG_PARAM_STR, func,
                                "msgaddr", TL_MSG_PARAM_STR, msgaddr );             
            RETURN (RBT_FAST_RETRY);
		} else if (smc_status.rc == -1 || smc_status.rc == -2) {
			usrmsg (func, "%s\n", msgaddr);
                        tl_tpdaemon.tl_log( &tl_tpdaemon, 103, 2,
                                            "func",    TL_MSG_PARAM_STR, func,
                                            "msgaddr", TL_MSG_PARAM_STR, msgaddr ); 
		} else {
			p = strrchr (msgaddr, ':');
			usrmsg (func, TP042, smc_ldr, "read_elem_status",
				p ? p + 2 : msgaddr);
                        tl_tpdaemon.tl_log( &tl_tpdaemon, 42, 4,
                                            "func",    TL_MSG_PARAM_STR, func,
                                            "smc_ldr", TL_MSG_PARAM_STR, smc_ldr,
                                            "Message", TL_MSG_PARAM_STR, "read_elem_status", 
                                            "Error",   TL_MSG_PARAM_STR, p ? p + 2 : msgaddr );
		}
		RETURN (c);
	}
	if ((element_info.state & 0x1) == 0) {
		usrmsg (func, TP041, "demount", vid, cur_unm, "Medium Not Present");
                tl_tpdaemon.tl_log( &tl_tpdaemon, 41, 5,
                                    "func",    TL_MSG_PARAM_STR, func,
                                    "action",  TL_MSG_PARAM_STR, "demount",
                                    "cur_vid", TL_MSG_PARAM_STR, vid,
                                    "cur_unm", TL_MSG_PARAM_STR, cur_unm,
                                    "Message", TL_MSG_PARAM_STR, "Medium Not Present" );
		RETURN (RBT_OK);
	}
	if ((element_info.state & 0x8) == 0) {
		usrmsg (func, TP041, "demount", vid, cur_unm, "Drive Not Unloaded");
                tl_tpdaemon.tl_log( &tl_tpdaemon, 41, 5,
                                    "func",    TL_MSG_PARAM_STR, func,
                                    "action",  TL_MSG_PARAM_STR, "demount",
                                    "cur_vid", TL_MSG_PARAM_STR, vid,
                                    "cur_unm", TL_MSG_PARAM_STR, cur_unm,
                                    "Message", TL_MSG_PARAM_STR, "Drive Not Unloaded" );
		RETURN (RBT_UNLD_DMNT);
	}
	if (*vid && !force && strcmp (element_info.name, vid)) {
		usrmsg (func, TP050, vid, element_info.name);
                tl_tpdaemon.tl_log( &tl_tpdaemon, 50, 4,
                                    "func",  TL_MSG_PARAM_STR, func,
                                    "vid_1", TL_MSG_PARAM_STR, vid,
                                    "vid_2", TL_MSG_PARAM_STR, element_info.name );
		RETURN (RBT_DMNT_FORCE);
	}
	if ((c = smc_move_medium (smc_fd, smc_ldr,
                                  robot_info.device_start+drvord, element_info.source_address,
                                  (element_info.flags & 0x40) ? 1 : 0)) < 0) {
		c = smc_lasterror (&smc_status, &msgaddr);
                if (c == EBUSY) {
			usrmsg (func, "%s\n", msgaddr);
                        tl_tpdaemon.tl_log( &tl_tpdaemon, 103, 2,
                                            "func",    TL_MSG_PARAM_STR, func,
                                            "msgaddr", TL_MSG_PARAM_STR, msgaddr ); 
                        RETURN(RBT_FAST_RETRY);
                } else if (smc_status.rc == -1 || smc_status.rc == -2) {
                        usrmsg (func, "%s\n", msgaddr);
                        tl_tpdaemon.tl_log( &tl_tpdaemon, 103, 2,
                                            "func",    TL_MSG_PARAM_STR, func,
                                            "msgaddr", TL_MSG_PARAM_STR, msgaddr ); 
                
                }
                else {
                        p = strrchr (msgaddr, ':');
                        sprintf (msg, TP041, "demount", vid, cur_unm,
                                 p ? p + 2 : msgaddr);
                        usrmsg (func, "%s\n", msg);
                        tl_tpdaemon.tl_log( &tl_tpdaemon, 41, 5,
                                            "func",    TL_MSG_PARAM_STR, func,
                                            "action",  TL_MSG_PARAM_STR, "demount",
                                            "cur_vid", TL_MSG_PARAM_STR, vid,
                                            "cur_unm", TL_MSG_PARAM_STR, cur_unm,
                                            "Message", TL_MSG_PARAM_STR, p ? p + 2 : msgaddr );
                }
                RETURN (c);
	}
	RETURN (0);
}

void closesmc ()
{
	if (smc_fd >= 0)
		close (smc_fd);
	smc_fd = -1;
}
