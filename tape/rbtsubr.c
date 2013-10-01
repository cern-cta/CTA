/*
 * Copyright (C) 1993-2003 by CERN/IT/PDP/DM
 * All rights reserved
 */

/*	rbtsubr - control routines for robot devices */
#include <errno.h>
#include <stdio.h>
#include <sys/types.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#if defined(CDK)
#include "acssys.h"
#include "acsapi.h"
#endif
#include <stdlib.h>
#include <string.h>
#include <pwd.h>
#include <netdb.h>              /* network "data base"                  */
#include <sys/types.h>          /* standard data types                  */
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>          /* arpa internet routines               */
#include "h/net.h"
#include "h/rmc_api.h"
#include "h/Ctape.h"
#include "h/Ctape_api.h"
#include "h/tplogger_api.h"
#include "h/serrno.h"

static char action[8];
static char cur_unm[9];
static char cur_vid[7];
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
#if defined(CDK)
static int acsmount( char*, char*, int );
static int acsdismount( char*, char*, unsigned int );
#endif

static int smcmount( char*, int, char* );
static int smcdismount( char*, char*, int, int );

static int istapemounted( char*, int, char* );
static int show_element_info( struct smc_element_info* );

/*	rbtmount - mounts a volume on a specified drive  */

int rbtmount (char *vid,
              int side,
              char *unm,
              char *dvn,
              int ring,
              char *loader)
{
	char func[16];

	strcpy (action, "mount");
	strcpy (cur_unm, unm);
	strcpy (cur_vid, vid);
#if defined(CDK)
	if (*loader == 'a')
		return (acsmount (vid, loader, ring));
#else
	(void)ring;
#endif
	if (*loader == 's') {
		int c;
		c = smcmount (vid, side, loader);
		return (c);
	}
	ENTRY (rbtmount);
	usrmsg (func, "TP041 - %s of %s on %s failed : %s\n", action, cur_vid,
	    cur_unm, "invalid loader type");
        tl_tpdaemon.tl_log( &tl_tpdaemon, 41, 5,
                            "func",    TL_MSG_PARAM_STR, func,
                            "action",  TL_MSG_PARAM_STR, action,
                            "cur_vid", TL_MSG_PARAM_STR, cur_vid,
                            "Drive",   TL_MSG_PARAM_STR, cur_unm,
                            "Message", TL_MSG_PARAM_STR, "invalid loader type" );
	RETURN (RBT_CONF_DRV_DN);
}

/*	rbtdemount - demounts a volume from a specified drive  */

int rbtdemount (char *vid,
                char *unm,
                char *dvn,
                char *loader,
                unsigned int force,
                int vsnretry)
{
	char func[16];

	strcpy (action, "demount");
	strcpy (cur_unm, unm);
	strcpy (cur_vid, vid);
#if defined(CDK)
	if (*loader == 'a') {
		if (mount_req_id)
			wait4acsfinalresp();
		return (acsdismount (vid, loader, force));
	}
#endif
	if (*loader == 's') {
		int c;
		c = smcdismount (vid, loader, force, vsnretry);
		return (c);
	}
	ENTRY (rbtdemount);
	usrmsg (func, "TP041 - %s of %s on %s failed : %s\n", action, cur_vid,
	    cur_unm, "invalid loader type");
        tl_tpdaemon.tl_log( &tl_tpdaemon, 41, 5,
                            "func",    TL_MSG_PARAM_STR, func,
                            "action",  TL_MSG_PARAM_STR, action,
                            "cur_vid", TL_MSG_PARAM_STR, cur_vid,
                            "Drive",   TL_MSG_PARAM_STR, cur_unm,
                            "Message", TL_MSG_PARAM_STR, "invalid loader type" );
	RETURN (RBT_CONF_DRV_DN);
}

#if defined(CDK)

/*	acssubr - I/O control routines for Storage Tek silos */
/*	loader should be of the form "acs"acs_id,lsm,panel,drive */

static char acsloader[14];
ALIGNED_BYTES rbuf[MAX_MESSAGE_SIZE/sizeof(ALIGNED_BYTES)];

extern int getconfent_multi();

static int actionChanged(int cc, int rt, short *act, int *slp, int *rtr) {
        
        int changed   = 0;
        int count     = 0;
        int rc        = 0;
        char parm[64] = "NULL";
        char **result = NULL;
        
        switch (cc) {
                
        case STATUS_LIBRARY_FAILURE:
                if (0 == rt) {
                        snprintf(parm, 64, "%s", "ACS_MOUNT_LIBRARY_FAILURE_HANDLING");
                } else if (1 == rt) {
                        snprintf(parm, 64, "%s", "ACS_UNMOUNT_LIBRARY_FAILURE_HANDLING");
                }
                break;
        default:
                return changed;
        }

        rc = getconfent_multi("TAPE", parm, 1, &result, &count);
        if (!rc && count) {
                if ((1 == count) && (0 == strcasecmp("down", result[0]))) {
                        *act = RBT_CONF_DRV_DN;
                        changed = 1;
                } else if ((3 == count) && (0 == strcasecmp("retry", result[0]))) {
                        *act = RBT_FAST_RETRY;
                        *rtr = atoi(result[1]);
                        if (*rtr<0) {*rtr = 0;}
                        *slp = atoi(result[2]);
                        if (*slp<60) {*slp = 60;}
                        changed = 1;
                }
        } else {
          *act = RBT_FAST_RETRY;
          *rtr = 3;
          *slp = 300;
          changed = 1;
        }
        return changed;
}


static int acserr2act(int req_type,	/* 0 --> mount, 1 --> dismount */
	       int cc)		/* error returned by the mount/dismount routine */
{
	struct rbterr_codact acserr_acttbl[] = {
	  {STATUS_DRIVE_AVAILABLE, RBT_NORETRY, RBT_OK},		/* unload on empty drive */
	  {STATUS_DRIVE_IN_USE, RBT_DMNT_FORCE, RBT_DMNT_FORCE},
	  {STATUS_DRIVE_OFFLINE, RBT_CONF_DRV_DN, RBT_CONF_DRV_DN},
	  {STATUS_INVALID_VOLUME, RBT_NORETRY, RBT_NORETRY},	/* syntax error in vid */
	  {STATUS_IPC_FAILURE, RBT_NORETRY, RBT_FAST_RETRY},
	  {STATUS_LIBRARY_FAILURE, RBT_CONF_DRV_DN, RBT_CONF_DRV_DN},
	  {STATUS_LIBRARY_NOT_AVAILABLE, RBT_FAST_RETRY, RBT_FAST_RETRY},
	  {STATUS_LSM_OFFLINE, RBT_NORETRY, RBT_FAST_RETRY},
	  {STATUS_MISPLACED_TAPE, RBT_NORETRY, RBT_NORETRY},
	  {STATUS_PENDING, RBT_DMNT_FORCE, RBT_DMNT_FORCE},	/* corrupted database */
	  {STATUS_VOLUME_IN_DRIVE, RBT_SLOW_RETRY, RBT_NORETRY}, 
	  {STATUS_VOLUME_NOT_IN_DRIVE, RBT_NORETRY, RBT_DMNT_FORCE}, /* vid mismatch on unload */
	  {STATUS_VOLUME_NOT_IN_LIBRARY, RBT_NORETRY, RBT_OK},
	  {STATUS_VOLUME_IN_USE, RBT_FAST_RETRY, RBT_FAST_RETRY},	/* volume in transit */
	  {STATUS_NI_FAILURE, RBT_NORETRY, RBT_FAST_RETRY},		/* contact with ACSLS lost */
	  {STATUS_INVALID_MEDIA_TYPE, RBT_NORETRY, RBT_NORETRY},
	  {STATUS_INCOMPATIBLE_MEDIA_TYPE, RBT_NORETRY, RBT_NORETRY},
	};
	unsigned int i;
        char func[16];

        static int ctr = 0;
        int        slp = 0;
        int        rtr = 0;
        short      act = 0;

        ENTRY (acserr2act);

        if (actionChanged(cc, req_type, &act, &slp, &rtr)) {

                char tmpStr[128];
                if ((ctr<rtr) && (slp>=60)) {

                        sprintf(tmpStr, "Retry %d (of %d), waiting %d seconds", ctr+1, rtr, slp);
                        tplogit(func, "%s\n", tmpStr);
                        tl_tpdaemon.tl_log( &tl_tpdaemon, 111, 2,
                                            "func",    TL_MSG_PARAM_STR, func,
                                            "Message", TL_MSG_PARAM_STR, tmpStr );

                        sleep(slp-60); /* RBT_FAST_RETRY will add 60 more seconds */                        
                        ctr++;

                } else {
                        
                        sprintf(tmpStr, "Maximum number of retries (%d) reached, drive put down\n", rtr);
                        tplogit (func, "%s\n", tmpStr);
                        tl_tpdaemon.tl_log( &tl_tpdaemon, 111, 2,
                                            "func",    TL_MSG_PARAM_STR, func,
                                            "Message", TL_MSG_PARAM_STR, tmpStr );
                        act = RBT_CONF_DRV_DN;
                        ctr = 0;
                }
                return act;

        } else {
                ctr = 0;
        }

	for (i = 0; i < sizeof(acserr_acttbl)/sizeof(struct rbterr_codact); i++) {
		if (cc == acserr_acttbl[i].cc)
			return ((req_type == 0) ?
				acserr_acttbl[i].mnt_fail_action :
				acserr_acttbl[i].dmnt_fail_action);
	}
	RETURN (RBT_NORETRY);
}

static char *
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

static int acsmount(char *vid,
             char *loader,
             int ring)
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
			char msg[OPRMSGSZ];
                        snprintf (msg, sizeof(msg), TP041, action, cur_vid, cur_unm, acsstatus (status));
			msg[sizeof(msg) - 1] = '\0';
                        usrmsg (func, "%s\n", msg);
                        tl_tpdaemon.tl_log( &tl_tpdaemon, 41, 5,
                                            "func",      TL_MSG_PARAM_STR, func,
                                            "action",    TL_MSG_PARAM_STR, action,
                                            "cur_vid",   TL_MSG_PARAM_STR, cur_vid,
                                            "Drive",     TL_MSG_PARAM_STR, cur_unm,
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

static int acsdismount(char *vid,
                char *loader,
                unsigned int force)
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
		char msg[OPRMSGSZ];
		snprintf (msg, sizeof(msg), TP041, action, cur_vid, cur_unm, acsstatus (status));
		msg[sizeof(msg) - 1] = '\0';
		usrmsg (func, "%s\n", msg);
                tl_tpdaemon.tl_log( &tl_tpdaemon, 41, 5,
                                    "func",      TL_MSG_PARAM_STR, func,
                                    "action",    TL_MSG_PARAM_STR, action,
                                    "cur_vid",   TL_MSG_PARAM_STR, cur_vid,
                                    "Drive",     TL_MSG_PARAM_STR, cur_unm,
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
		char msg[OPRMSGSZ];
	        snprintf (msg, sizeof(msg), TP041, action, cur_vid, cur_unm, acsstatus (status));
		msg[sizeof(msg) - 1] = '\0';
		usrmsg (func, "%s\n", msg);
                tl_tpdaemon.tl_log( &tl_tpdaemon, 41, 5,
                                    "func",      TL_MSG_PARAM_STR, func,
                                    "action",    TL_MSG_PARAM_STR, action,
                                    "cur_vid",   TL_MSG_PARAM_STR, cur_vid,
                                    "Drive",     TL_MSG_PARAM_STR, cur_unm,
                                    "acsstatus", TL_MSG_PARAM_STR, acsstatus( status ) );
		c = acserr2act (1, status);
		RETURN (c);
	}

	/* Added by BC 20050317 to process status code inside the rbuf */
	{
	  ACS_DISMOUNT_RESPONSE *dr;

	  dr = (ACS_DISMOUNT_RESPONSE *)rbuf;

	  if (dr->dismount_status) {
	    char msg[OPRMSGSZ];
	    snprintf (msg, sizeof(msg), TP041, action, cur_vid, cur_unm, acsstatus (dr->dismount_status));
	    msg[sizeof(msg) - 1] = '\0';
	    usrmsg (func, "%s\n", msg);
            tl_tpdaemon.tl_log( &tl_tpdaemon, 41, 5,
                                "func",             TL_MSG_PARAM_STR, func,
                                "action",           TL_MSG_PARAM_STR, action,
                                "cur_vid",          TL_MSG_PARAM_STR, cur_vid,
                                "Drive",            TL_MSG_PARAM_STR, cur_unm,
                                "acsstatus (rbuf)", TL_MSG_PARAM_STR, acsstatus (dr->dismount_status) );           
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

	strncpy (func, "acsmountresp", 16);
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
		char msg[OPRMSGSZ];
		snprintf (msg, sizeof(msg), TP041, action, cur_vid, cur_unm, acsstatus (status));
		msg[sizeof(msg) - 1] = '\0';
		usrmsg (func, "%s\n", msg);
                tl_tpdaemon.tl_log( &tl_tpdaemon, 41, 5,
                                    "func",      TL_MSG_PARAM_STR, func,
                                    "action",    TL_MSG_PARAM_STR, action,
                                    "cur_vid",   TL_MSG_PARAM_STR, cur_vid,
                                    "Drive",     TL_MSG_PARAM_STR, cur_unm,
                                    "acsstatus", TL_MSG_PARAM_STR, acsstatus( status ) );
		c = acserr2act (0, status);
		RETURN (c);
	}

	/* Added by BC 20050317 to process status code inside the rbuf */
	{
	  ACS_MOUNT_RESPONSE *dr;

	  dr = (ACS_MOUNT_RESPONSE *)rbuf;

	  if (dr->mount_status) {
	    char msg[OPRMSGSZ];
	    snprintf (msg, sizeof(msg), TP041, action, cur_vid, cur_unm, acsstatus (dr->mount_status));
	    msg[sizeof(msg) - 1] = '\0';
	    usrmsg (func, "%s\n", msg);
            tl_tpdaemon.tl_log( &tl_tpdaemon, 41, 5,
                                "func",             TL_MSG_PARAM_STR, func,
                                "action",           TL_MSG_PARAM_STR, action,
                                "cur_vid",          TL_MSG_PARAM_STR, cur_vid,
                                "Drive",            TL_MSG_PARAM_STR, cur_unm,
                                "acsstatus (rbuf)", TL_MSG_PARAM_STR, acsstatus( dr->mount_status ) );
	    c = acserr2act (0, dr->mount_status);
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
	strncpy (func, "wait4acsfinalr", 16);
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
		char msg[OPRMSGSZ];
		snprintf (msg, sizeof(msg), TP041, action, cur_vid, cur_unm, acsstatus (status));
		msg[sizeof(msg) - 1] = '\0';
		usrmsg (func, "%s\n", msg);
                tl_tpdaemon.tl_log( &tl_tpdaemon, 41, 5,
                                    "func",      TL_MSG_PARAM_STR, func,
                                    "action",    TL_MSG_PARAM_STR, action,
                                    "cur_vid",   TL_MSG_PARAM_STR, cur_vid,
                                    "Drive",     TL_MSG_PARAM_STR, cur_unm,
                                    "acsstatus", TL_MSG_PARAM_STR, acsstatus( status ) );
		c = acserr2act (*action == 'm' ? 0 : 1, status);
		RETURN (c);
	}

	/* Added by BC 20050317 to process status code inside the rbuf */
        
        if (*action == 'm') {

                ACS_MOUNT_RESPONSE *dr;                        
                dr = (ACS_MOUNT_RESPONSE *)rbuf;
                
                if (dr->mount_status) {
			char msg[OPRMSGSZ];
                        snprintf (msg, sizeof(msg), TP041, action, cur_vid, cur_unm, acsstatus (dr->mount_status));
			msg[sizeof(msg) - 1] = '\0';
                        usrmsg (func, "%s\n", msg);
                        tl_tpdaemon.tl_log( &tl_tpdaemon, 41, 5,
                                            "func",             TL_MSG_PARAM_STR, func,
                                            "action",           TL_MSG_PARAM_STR, action,
                                            "cur_vid",          TL_MSG_PARAM_STR, cur_vid,
                                            "Drive",            TL_MSG_PARAM_STR, cur_unm,
                                            "acsstatus (rbuf)", TL_MSG_PARAM_STR, acsstatus( dr->mount_status ) );
                        c = acserr2act (0, dr->mount_status);
                        RETURN (c);
                }

	} else if (*action == 'd') {

                ACS_DISMOUNT_RESPONSE *dr;                        
                dr = (ACS_DISMOUNT_RESPONSE *)rbuf;
                
                if (dr->dismount_status) {
			char msg[OPRMSGSZ];
                        snprintf (msg, sizeof(msg), TP041, action, cur_vid, cur_unm, acsstatus (dr->dismount_status));
			msg[sizeof(msg) - 1] = '\0';
                        usrmsg (func, "%s\n", msg);
                        tl_tpdaemon.tl_log( &tl_tpdaemon, 41, 5,
                                            "func",             TL_MSG_PARAM_STR, func,
                                            "action",           TL_MSG_PARAM_STR, action,
                                            "cur_vid",          TL_MSG_PARAM_STR, cur_vid,
                                            "Drive",            TL_MSG_PARAM_STR, cur_unm,
                                            "acsstatus (rbuf)", TL_MSG_PARAM_STR, acsstatus( dr->dismount_status ) );
                        c = acserr2act (1, dr->dismount_status);
                        RETURN (c);
                }
        }

	return (0);
}

#endif

static int drvord;
static const char *rmc_host = NULL;
static char rmc_errbuf[256];
static char smc_ldr[CA_MAXRBTNAMELEN+6];
static struct robot_info robot_info;

static int parseloader(const char *const loader) {
	char *dp;
	char func[16];
	char *p;

	ENTRY (parseloader);
	snprintf (smc_ldr, sizeof(smc_ldr), "/dev/%s", loader);
	smc_ldr[sizeof(smc_ldr) - 1] = '\0';
	if ((p = strchr (smc_ldr, ',')) == 0) {
		usrmsg (func, "TP041 - %s of %s on %s failed : %s\n", action,
		    cur_vid, cur_unm, "invalid loader");
                tl_tpdaemon.tl_log( &tl_tpdaemon, 41, 5,
                                    "func",    TL_MSG_PARAM_STR, func,
                                    "action",  TL_MSG_PARAM_STR, action,
                                    "cur_vid", TL_MSG_PARAM_STR, cur_vid,
                                    "Drive",   TL_MSG_PARAM_STR, cur_unm,
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
                                    "Drive",   TL_MSG_PARAM_STR, cur_unm,
                                    "Message", TL_MSG_PARAM_STR, "invalid loader" );
		RETURN (RBT_NORETRY);
	}
	if ((p = strchr (smc_ldr, '@'))) {
		*p = '\0';
		rmc_host = p + 1;
	}
	if (!rmc_host) {
		usrmsg (func, "TP041 - %s of %s on %s failed : %s\n", action,
                    cur_vid, cur_unm, "rmc host must be specified");
                tl_tpdaemon.tl_log( &tl_tpdaemon, 41, 5,
                  "func",    TL_MSG_PARAM_STR, func,
                  "action",  TL_MSG_PARAM_STR, action,
                  "cur_vid", TL_MSG_PARAM_STR, cur_vid,
                  "Drive",   TL_MSG_PARAM_STR, cur_unm,
                  "Message", TL_MSG_PARAM_STR, "rmc host must be specified" );
                RETURN (RBT_NORETRY);
        }

	RETURN (0);
}

static int smcmount(char *vid,
             int side,
             char *loader)
{
	struct smc_element_info element_info;
	char func[16];

	ENTRY (smcmount);

	memset(&element_info, '\0', sizeof(element_info));
        
	{
		const int parseloader_rc = parseloader(loader);
		if(0 != parseloader_rc) {
			RETURN (parseloader_rc);
		}
	}
	(void) rmc_seterrbuf (rmc_errbuf, sizeof(rmc_errbuf));

	{

		const int rmc_mount_rc = rmc_mount (rmc_host, vid, side, drvord);
		const int rmc_mount_serrno = serrno;

		if (0 == rmc_mount_rc) {
			RETURN (0);
                } else if (EBUSY == rmc_mount_rc) {

                        /* this will never happen, since c == -1 on error     */
                        RETURN(RBT_FAST_RETRY);

                } else if ((-1 == rmc_mount_rc) && ((rmc_mount_serrno - ERMCRBTERR) == EBUSY)) {

                        /* 
                           EBUSY: tape may be mounted none the less, so check 
                           that and repeat mount only if appropriate          
                        */

                        const int RETRIES = 3;
			int retryCtr = 0;
			{
				char msg[OPRMSGSZ];
                        	const char *const p = strrchr (rmc_errbuf, ':');
				snprintf (msg, sizeof(msg), TP041, "mount", vid, cur_unm,
                                 	p ? p + 2 : rmc_errbuf);
				msg[sizeof(msg) - 1] = '\0';
                        	tplogit (func, "%s", msg);
                        	tl_tpdaemon.tl_log( &tl_tpdaemon, 41, 5,
                                            "func",    TL_MSG_PARAM_STR, func,
                                            "action",  TL_MSG_PARAM_STR, "mount",
                                            "cur_vid", TL_MSG_PARAM_STR, vid,
                                            "Drive",   TL_MSG_PARAM_STR, cur_unm,
                                            "Message", TL_MSG_PARAM_STR, p ? p + 2 : rmc_errbuf );
			}
                        
                        while (retryCtr < RETRIES) {
                                
                                /* check if the tape is mounted or not */
                                const int mounted = istapemounted( vid, drvord, smc_ldr );
                                if (0 == mounted) {

                                        tplogit (func, "Encountered EBUSY. Tape not mounted. Retry Mount.\n" );
                                        tl_tpdaemon.tl_log( &tl_tpdaemon, 104, 2,
                                                            "func"   , TL_MSG_PARAM_STR, func,
                                                            "Message", TL_MSG_PARAM_STR, "Encountered EBUSY. Tape not mounted. Retry mount." );
                                        RETURN (RBT_FAST_RETRY);

                                } else if (1 == mounted) {
                                
                                        tplogit (func, "Encountered EBUSY. Tape mounted. Continue.\n" );
                                        tl_tpdaemon.tl_log( &tl_tpdaemon, 104, 2,
                                                            "func"   , TL_MSG_PARAM_STR, func,
                                                            "Message", TL_MSG_PARAM_STR, "Encountered EBUSY. Tape mounted. Continue." );
                                        RETURN (0);

                                } else {
                                        
                                        tplogit (func, "Encountered EBUSY. Error in istapemounted. Retry to retrieve info.\n" );
                                        tl_tpdaemon.tl_log( &tl_tpdaemon, 103, 2,
                                                            "func"   , TL_MSG_PARAM_STR, func,
                                                            "Message", TL_MSG_PARAM_STR, "Encountered EBUSY. Error in istapemounted. Retry to retrieve info." );
                                        sleep( 60 );
                                }                                
                                retryCtr++;
                        }

                        /* no info available, return the original problem */
			if (SECOMERR == rmc_mount_serrno) {
				RETURN (RBT_FAST_RETRY);
			} else {
				RETURN (rmc_mount_serrno - ERMCRBTERR);
			}

                } else if ((-1 == rmc_mount_rc) && ((rmc_mount_serrno - ERMCRBTERR) == 7)) {
                        
                        /* 
                           'Volume in use': this may happen when we try to 
                           mount a tape that's in our drive already (after a 
                           spoiled EBUSY check), so check again and return 
                           the error only if appropriate.

                           NB: Error code 7 is not exactly 'Volume in use', it 
                           is rather a request for a slow retry (RBT_OMSG_SLOW_R). 
                           In order to be sure rmc_errbuf should be checked. OTHO,
                           if the tape is in the drive there should be no need 
                           for a retry.                                           
                        */
                        
                        const int RETRIES = 3;
			int retryCtr = 0;

			{
				char msg[OPRMSGSZ];
                        	const char *const p = strrchr (rmc_errbuf, ':');
				snprintf (msg, sizeof(msg), TP041, "mount", vid, cur_unm,
                                 	p ? p + 2 : rmc_errbuf);
				msg[sizeof(msg) -1] = '\0';
                        	tplogit (func, "%s", msg);
                        	tl_tpdaemon.tl_log( &tl_tpdaemon, 41, 5,
                                            "func",    TL_MSG_PARAM_STR, func,
                                            "action",  TL_MSG_PARAM_STR, "mount",
                                            "cur_vid", TL_MSG_PARAM_STR, vid,
                                            "Drive",   TL_MSG_PARAM_STR, cur_unm,
                                            "Message", TL_MSG_PARAM_STR, p ? p + 2 : rmc_errbuf );
			}
                        
                        while (retryCtr < RETRIES) {
                                
                                /* check if the tape is mounted or not */
                                const int mounted = istapemounted( vid, drvord, smc_ldr );
                                if (0 == mounted) {

                                        tplogit (func, "Encountered 'Volume in Use'. Tape not mounted locally. Return Error.\n" );
                                        tl_tpdaemon.tl_log( &tl_tpdaemon, 104, 2,
                                                            "func"   , TL_MSG_PARAM_STR, func,
                                                            "Message", TL_MSG_PARAM_STR, "Encountered EBUSY. Tape not mounted locally. Return Error." );

					if (SECOMERR == rmc_mount_serrno) {
						RETURN (RBT_FAST_RETRY);
					} else {
						RETURN (rmc_mount_serrno - ERMCRBTERR);
					}

                                } else if (1 == mounted) {
                                
                                        tplogit (func, "Encountered 'Volume in Use'. Tape mounted locally. Continue.\n" );
                                        tl_tpdaemon.tl_log( &tl_tpdaemon, 104, 2,
                                                            "func"   , TL_MSG_PARAM_STR, func,
                                                            "Message", TL_MSG_PARAM_STR, "Encountered EBUSY. Tape mounted locally. Continue." );
                                        RETURN (0);

                                } else {
                                        
                                        tplogit (func, "Encountered 'Volume in Use'. Error in istapemounted. Retry to retrieve info.\n" );
                                        tl_tpdaemon.tl_log( &tl_tpdaemon, 103, 2,
                                                            "func"   , TL_MSG_PARAM_STR, func,
                                                            "Message", TL_MSG_PARAM_STR, "Encountered EBUSY. Error in istapemounted. Retry to retrieve info." );
                                        sleep( 60 );
                                }                                
                                retryCtr++;
                        }

                        /* no info available, return the original problem */
			if(SECOMERR == rmc_mount_serrno) {
				RETURN (RBT_FAST_RETRY);
			} else {
				RETURN (rmc_mount_serrno - ERMCRBTERR);
			}

                } else {

                        /* log information about the error condition         */
			if (SECOMERR != rmc_mount_serrno) {
                                tplogit (func, "Error in smcmount: c=%d, serrno=%d, (serrno-ERMCRBTERR)=%d\n", 
                                         rmc_mount_rc, rmc_mount_serrno, (rmc_mount_serrno - ERMCRBTERR) );                        
                                tl_tpdaemon.tl_log( &tl_tpdaemon, 41, 5,
                                                    "func"               , TL_MSG_PARAM_STR, func,
                                                    "Message"            , TL_MSG_PARAM_STR, "Error in smcmount",
                                                    "c"                  , TL_MSG_PARAM_INT, rmc_mount_rc, 
                                                    "serrno"             , TL_MSG_PARAM_INT, rmc_mount_serrno, 
                                                    "(serrno-ERMCRBTERR)", TL_MSG_PARAM_INT, (rmc_mount_serrno - ERMCRBTERR) );
                        }                        
			{
				char msg[OPRMSGSZ];
                        	const char *const p = strrchr (rmc_errbuf, ':');
				snprintf (msg, sizeof(msg), TP041, "mount", vid, cur_unm,
                                 	p ? p + 2 : rmc_errbuf);
				msg[sizeof(msg) - 1] = '\0';
                        	/* Just send message to operator after two retries*/ 
                        	tplogit (func, "%s", msg);
                        	tl_tpdaemon.tl_log( &tl_tpdaemon, 41, 5,
                                            "func",    TL_MSG_PARAM_STR, func,
                                            "action",  TL_MSG_PARAM_STR, "mount",
                                            "cur_vid", TL_MSG_PARAM_STR, vid,
                                            "Drive",   TL_MSG_PARAM_STR, cur_unm,
                                            "Message", TL_MSG_PARAM_STR, p ? p + 2 : rmc_errbuf );
			}

			if (SECOMERR == rmc_mount_serrno) {
				RETURN (RBT_FAST_RETRY);
			} else {
				RETURN (rmc_mount_serrno - ERMCRBTERR);
			}
                }
	}
}

static int smcdismount(char *vid,
        char *loader,
        int force,
        int vsnretry)
{
	struct smc_element_info element_info;
	char func[16];
	
	ENTRY (smcdismount);

	memset(&element_info, '\0', sizeof(element_info));

	{
		const int parseloader_rc = parseloader(loader);
		if(0 != parseloader_rc) {
			RETURN (parseloader_rc);
		}
	}
	(void) rmc_seterrbuf (rmc_errbuf, sizeof(rmc_errbuf));

	{
		const int rmc_dismount_rc = rmc_dismount (rmc_host, vid, drvord, force);
		const int rmc_dismount_serrno = serrno;
		if (0 == rmc_dismount_rc) {
			RETURN (0);
		} else if (EBUSY == rmc_dismount_rc) {
                        /* this will never happen, since rmc_dismount_rc == -1 on error    */
                        RETURN (RBT_FAST_RETRY);

		} else if ((-1 == rmc_dismount_rc) && ((rmc_dismount_serrno - ERMCRBTERR) == EBUSY)) {

                        /* this should trigger a retry on a busy error       */ 

			char msg[OPRMSGSZ];
                        const char *const p = strrchr (rmc_errbuf, ':');
                        snprintf (msg, sizeof(msg), TP041, "demount", vid, cur_unm,
                                 p ? p + 2 : rmc_errbuf);
			msg[sizeof(msg) - 1] = '\0';
                        tplogit (func, "%s", msg);
                        tl_tpdaemon.tl_log( &tl_tpdaemon, 41, 5,
                                            "func",    TL_MSG_PARAM_STR, func,
                                            "action",  TL_MSG_PARAM_STR, "demount",
                                            "cur_vid", TL_MSG_PARAM_STR, vid,
                                            "Drive",   TL_MSG_PARAM_STR, cur_unm,
                                            "Message", TL_MSG_PARAM_STR, p ? p + 2 : rmc_errbuf );

                        tplogit (func, "Encountered EBUSY. Will retry.\n" );
                        tl_tpdaemon.tl_log( &tl_tpdaemon, 41, 2,
                                            "func"   , TL_MSG_PARAM_STR, func,
                                            "Message", TL_MSG_PARAM_STR, "Encountered EBUSY. Will retry." );

                        RETURN (RBT_FAST_RETRY);

                } else {
                        /* log information about the error condition         */
                        if (rmc_dismount_serrno != SECOMERR) {
                                tplogit (func, "Error in smcdismount: c=%d, serrno=%d, (serrno-ERMCRBTERR)=%d\n", 
                                         rmc_dismount_rc, rmc_dismount_serrno, (rmc_dismount_serrno - ERMCRBTERR) );                        
                                tl_tpdaemon.tl_log( &tl_tpdaemon, 41, 5,
                                                    "func"               , TL_MSG_PARAM_STR, func,
                                                    "Message"            , TL_MSG_PARAM_STR, "Error in smcdismount",
                                                    "c"                  , TL_MSG_PARAM_INT, rmc_dismount_rc, 
                                                    "serrno"             , TL_MSG_PARAM_INT, rmc_dismount_serrno, 
                                                    "(serrno-ERMCRBTERR)", TL_MSG_PARAM_INT, (rmc_dismount_serrno - ERMCRBTERR) );
                        }
			{
				char msg[OPRMSGSZ];
                        	const char *const p = strrchr (rmc_errbuf, ':');
                        	snprintf (msg, sizeof(msg), TP041, "demount", vid, cur_unm,
                                 	p ? p + 2 : rmc_errbuf);
				msg[sizeof(msg) - 1] = '\0';
                        	/* Just send message to operator after two retries on connection reset*/ 
                        	if (vsnretry > 3 && (rmc_dismount_serrno == SECOMERR)) {
                                	usrmsg (func, "%s\n", msg);
                                	tl_tpdaemon.tl_log( &tl_tpdaemon, 41, 5,
                                                    	"func",    TL_MSG_PARAM_STR, func,
                                                    	"action",  TL_MSG_PARAM_STR, "demount",
                                                    	"cur_vid", TL_MSG_PARAM_STR, vid,
                                                    	"Drive",   TL_MSG_PARAM_STR, cur_unm,
                                                    	"Message", TL_MSG_PARAM_STR, p ? p + 2 : rmc_errbuf );
                        	} else if (vsnretry <= 3 && (rmc_dismount_serrno == SECOMERR)){
                                	tplogit (func, "%s", msg);
                                	tl_tpdaemon.tl_log( &tl_tpdaemon, 41, 5,
                                                    	"func",    TL_MSG_PARAM_STR, func,
                                                    	"action",  TL_MSG_PARAM_STR, "demount",
                                                    	"cur_vid", TL_MSG_PARAM_STR, vid,
                                                    	"Drive",   TL_MSG_PARAM_STR, cur_unm,
                                                    	"Message", TL_MSG_PARAM_STR, p ? p + 2 : rmc_errbuf );
                        	} else {
                                	usrmsg (func, "%s\n", msg);
                                	tl_tpdaemon.tl_log( &tl_tpdaemon, 41, 5,
                                                    	"func",    TL_MSG_PARAM_STR, func,
                                                    	"action",  TL_MSG_PARAM_STR, "demount",
                                                    	"cur_vid", TL_MSG_PARAM_STR, vid,
                                                    	"Drive",   TL_MSG_PARAM_STR, cur_unm,
                                                    	"Message", TL_MSG_PARAM_STR, p ? p + 2 : rmc_errbuf );
                        	}
			}

			if(SECOMERR == rmc_dismount_serrno) {
				RETURN (RBT_FAST_RETRY);
			} else {
                        	RETURN (rmc_dismount_serrno - ERMCRBTERR);
			}
                }
	}
}


/*
** istapemounted: check if the requested tape is already mounted
**
** returns 1 if yes
**         0 if no
**        -1 on error
*/
static int istapemounted( char *vid, int drvord, char *smc_ldr ) {
        
        char func[16];
        int  rc = 0;
        struct smc_element_info element_info;

        ENTRY (istapemounted);        
        
        /* get robot geometry */
        tplogit (func, "get robot geometry\n", vid );
        tl_tpdaemon.tl_log( &tl_tpdaemon, 111, 2,
                            "func"   , TL_MSG_PARAM_STR, func,
                            "Message", TL_MSG_PARAM_STR, "get robot geometry" );

        if (rmc_get_geometry( rmc_host, &robot_info) < 0) {
                
                tplogit( func, "could not get robot geometry\n" );
                tl_tpdaemon.tl_log( &tl_tpdaemon, 104, 2,
                                    "func"   , TL_MSG_PARAM_STR, func,
                                    "Message", TL_MSG_PARAM_STR, "could not get robot geometry" );
                rc = -1;
                goto out;
        }
        
        /* request drive info */
        memset( &element_info, 0, sizeof( struct smc_element_info ) );
        tplogit (func, "read element status\n", vid );
        tl_tpdaemon.tl_log( &tl_tpdaemon, 111, 2,
                            "func"   , TL_MSG_PARAM_STR, func,
                            "Message", TL_MSG_PARAM_STR, "read element status" );

        if (rmc_read_elem_status (rmc_host, 4,
                                  robot_info.device_start+drvord, 1, &element_info) > 0) {

                if (0 == strcmp( vid, element_info.name)) {

                        tplogit (func, "%s is mounted\n", vid );
                        tl_tpdaemon.tl_log( &tl_tpdaemon, 110, 4,
                                            "func"   , TL_MSG_PARAM_STR  , func,
                                            "Message", TL_MSG_PARAM_STR  , "is mounted",
                                            "VID"    , TL_MSG_PARAM_STR  , vid,
                                            "TPVID"  , TL_MSG_PARAM_TPVID, vid );
                        rc = 1;
                        goto out;

                } else {

                        tplogit (func, "%s is not mounted\n", vid );
                        tl_tpdaemon.tl_log( &tl_tpdaemon, 110, 4,
                                            "func"   , TL_MSG_PARAM_STR  , func,
                                            "Message", TL_MSG_PARAM_STR  , "is not mounted",
                                            "VID"    , TL_MSG_PARAM_STR  , vid,
                                            "TPVID"  , TL_MSG_PARAM_TPVID, vid );                        
                        show_element_info( &element_info );
                        rc = 0;
                        goto out;
                }
                
        } else {
                
                tplogit (func, "could not retrieve drive status\n");
                tl_tpdaemon.tl_log( &tl_tpdaemon, 104, 2,
                                    "func"   , TL_MSG_PARAM_STR  , func,
                                    "Message", TL_MSG_PARAM_STR  , "could not retrieve drive status" );
                rc = -1;
                goto out;
        }
        
 out:
        RETURN (rc);
}


/*
** show_element_info: dump the contents of a struct smc_element_info
**
** return value: 0 on success
**              -1 in case a NULL ptr is passed
*/
static int show_element_info( struct smc_element_info *element_info ) {

        char func[16];
        int  rc = 0;

        ENTRY (show_element_info);

        if (NULL != element_info ) {

                tplogit (func,\
                         "element_address: %d, "\
                         "element_type: %d, "\
                         "state: %d, "\
                         "asc: %d, "\
                         "ascq: %d, "\
                         "flags: %d, "\
                         "source_address: %d, "\
                         "name: %s\n", element_info->element_address, element_info->element_type,
                         element_info->state, element_info->asc, 
                         element_info->ascq, element_info->flags,
                         element_info->source_address, element_info->name );
                {
                        char __asc[32], __ascq[32];
                        sprintf( __asc, "%d (0x%x)", element_info->asc, element_info->asc );
                        sprintf( __ascq, "%d (0x%x)", element_info->ascq, element_info->ascq );
                        tl_tpdaemon.tl_log( &tl_tpdaemon, 111, 9,
                                            "func"           , TL_MSG_PARAM_STR, func,
                                            "element_address", TL_MSG_PARAM_INT, element_info->element_address,
                                            "element_type"   , TL_MSG_PARAM_INT, element_info->element_type,
                                            "state"          , TL_MSG_PARAM_INT, element_info->state,
                                            "asc"            , TL_MSG_PARAM_STR, __asc,
                                            "ascq"           , TL_MSG_PARAM_STR, __ascq,
                                            "flags"          , TL_MSG_PARAM_INT, element_info->flags,
                                            "source_address" , TL_MSG_PARAM_INT, element_info->source_address,
                                            "name"           , TL_MSG_PARAM_STR, element_info->name );
                }

        } else {

                tplogit (func, "element_info is NULL\n" );
                tl_tpdaemon.tl_log( &tl_tpdaemon, 103, 2,
                                    "func"   , TL_MSG_PARAM_STR  , func,
                                    "Message", TL_MSG_PARAM_STR  , "element_info is NULL" );
                rc = -1;
        }

        RETURN (rc);
}
