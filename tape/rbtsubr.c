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

int smcmount( char*, int, char* );
int smcdismount( char*, char*, int, int );

int send2dmc( int*, DMCrequest_t * );
int fromdmc( int*, DMCreply_t * );

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

	if (*loader == 'r')
		return (0);
	strcpy (action, "mount");
	strcpy (cur_unm, unm);
	strcpy (cur_vid, vid);
#if defined(CDK)
	if (*loader == 'a')
		return (acsmount (vid, loader, ring));
#else
	(void)ring;
#endif

	if (*loader == 'd')
		return(dmcmount(vid,unm,loader));

	if (*loader == 'n'
	    || *loader == 'R'
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
                        tl_tpdaemon.tl_log( &tl_tpdaemon, 42, 4,
                                            "func",    TL_MSG_PARAM_STR, func,
                                            "Message", TL_MSG_PARAM_STR, "popen",
                                            "Error",   TL_MSG_PARAM_STR, strerror(errno),
                                            "Drive"  , TL_MSG_PARAM_STR, cur_unm );
			RETURN (-errno);
		}
		while (fgets (buf, sizeof(buf), f) != NULL) {
			usrmsg (func, "TP041 - %s of %s on %s failed : %s\n",
			    action, cur_vid, cur_unm, buf);
                        tl_tpdaemon.tl_log( &tl_tpdaemon, 41, 5,
                                            "func",    TL_MSG_PARAM_STR, func,
                                            "action",  TL_MSG_PARAM_STR, action,
                                            "cur_vid", TL_MSG_PARAM_STR, cur_vid,
                                            "Drive",   TL_MSG_PARAM_STR, cur_unm,
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

	if (*loader == 'r')
		return (0);
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

	if (*loader == 'd')
		return(dmcdismount(vid, unm, loader, force));

	if (*loader == 'n'
	    || *loader == 'R'
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
                        tl_tpdaemon.tl_log( &tl_tpdaemon, 42, 4,
                                            "func",    TL_MSG_PARAM_STR, func,
                                            "Message", TL_MSG_PARAM_STR, "popen",
                                            "Error",   TL_MSG_PARAM_STR, strerror(errno),
                                            "Drive",   TL_MSG_PARAM_STR, cur_unm );
			RETURN (-errno);
		}
		while (fgets (buf, sizeof(buf), f) != NULL) {
			usrmsg (func, "TP041 - %s of %s on %s failed : %s\n",
			    action, cur_vid, cur_unm, buf);
                        tl_tpdaemon.tl_log( &tl_tpdaemon, 41, 5,
                                            "func",    TL_MSG_PARAM_STR, func,
                                            "action",  TL_MSG_PARAM_STR, action,
                                            "cur_vid", TL_MSG_PARAM_STR, cur_vid,
                                            "Drive",   TL_MSG_PARAM_STR, cur_unm,
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
                            "Drive",   TL_MSG_PARAM_STR, cur_unm,
                            "Message", TL_MSG_PARAM_STR, "invalid loader type" );
	RETURN (RBT_CONF_DRV_DN);
}

#if defined(CDK)

/*	acssubr - I/O control routines for Storage Tek silos */
/*	loader should be of the form "acs"acs_id,lsm,panel,drive */

char acsloader[14];
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
        }
        return changed;
}


int acserr2act(int req_type,	/* 0 --> mount, 1 --> dismount */
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

int acsmount(char *vid,
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
                        sprintf (msg, TP041, action, cur_vid, cur_unm, acsstatus (status));
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

int acsdismount(char *vid,
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
		sprintf (msg, TP041, action, cur_vid, cur_unm, acsstatus (status));
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
	        sprintf (msg, TP041, action, cur_vid, cur_unm, acsstatus (status));
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
	    sprintf (msg, TP041, action, cur_vid, cur_unm, acsstatus (dr->dismount_status));
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
	    sprintf (msg, TP041, action, cur_vid, cur_unm, acsstatus (dr->mount_status));
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
                        sprintf (msg, TP041, action, cur_vid, cur_unm, acsstatus (dr->mount_status));
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
                        sprintf (msg, TP041, action, cur_vid, cur_unm, acsstatus (dr->dismount_status));
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
/*
 * Code for DEC Media Changer (TL820) robots. Works together with
 * the dmcserv server. Depends on the "dmc.h" include file.
 */
extern char *getconfent();
int dmcmount(char *vid,
             char *unm,
             char *loader)
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
                                            "Drive",   TL_MSG_PARAM_STR, cur_unm,
                                            "last",    TL_MSG_PARAM_STR, last );
		}
	}
	if ( rep.log_info != NULL && rep.log_info_l ) free(rep.log_info);
	RETURN(rep.status);
}

int dmcdismount(char *vid,
                char *unm,
                char *loader,
                unsigned int force)
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
                            "vol_id",   TL_MSG_PARAM_STR, req.vid,
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
                                            "Drive",   TL_MSG_PARAM_STR, cur_unm,
                                            "last",    TL_MSG_PARAM_STR, last );
		}
	}
	if ( rep.log_info != NULL && rep.log_info_l ) free(rep.log_info);
	RETURN(rep.status);
}
int send2dmc(int *sock,
             DMCrequest_t *req)
{
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

int fromdmc(int *sock,
            DMCreply_t *rep)
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

int opensmc(char *loader)
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
	if (rmc_host) {
		(void) rmc_seterrbuf (rmc_errbuf, sizeof(rmc_errbuf));
		RETURN (0);
	}

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
                                tl_tpdaemon.tl_log( &tl_tpdaemon, 42, 5,
                                                    "func",    TL_MSG_PARAM_STR, func,
                                                    "smc_ldr", TL_MSG_PARAM_STR, smc_ldr,
                                                    "Message", TL_MSG_PARAM_STR, "get_geometry", 
                                                    "Error",   TL_MSG_PARAM_STR, strrchr (msgaddr, ':') + 2,
                                                    "Drive",   TL_MSG_PARAM_STR, cur_unm );
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
                                    "Drive",   TL_MSG_PARAM_STR, cur_unm,
                                    "Message", TL_MSG_PARAM_STR, "invalid loader" );
		RETURN (RBT_NORETRY);
	}
	RETURN (0);
}

int smcmount(char *vid,
             int side,
             char *loader)
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

                        /* this will never happen, since c == -1 on error     */
                        RETURN(RBT_FAST_RETRY);

                } else if ((-1 == c) && ((serrno - ERMCRBTERR) == EBUSY)) {

                        /* 
                           EBUSY: tape may be mounted none the less, so check 
                           that and repeat mount only if appropriate          
                        */

                        int mounted, RETRIES = 3, retryCtr = 0;
                        int save_serrno = serrno; 
 
                        p = strrchr (rmc_errbuf, ':');
			sprintf (msg, TP041, "mount", vid, cur_unm,
                                 p ? p + 2 : rmc_errbuf);
                        tplogit (func, "%s", msg);
                        tl_tpdaemon.tl_log( &tl_tpdaemon, 41, 5,
                                            "func",    TL_MSG_PARAM_STR, func,
                                            "action",  TL_MSG_PARAM_STR, "mount",
                                            "cur_vid", TL_MSG_PARAM_STR, vid,
                                            "Drive",   TL_MSG_PARAM_STR, cur_unm,
                                            "Message", TL_MSG_PARAM_STR, p ? p + 2 : rmc_errbuf );
                        
                        while (retryCtr < RETRIES) {
                                
                                /* check if the tape is mounted or not */
                                mounted = istapemounted( vid, drvord, smc_ldr );
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
                        c = (save_serrno == SECOMERR) ? RBT_FAST_RETRY : save_serrno - ERMCRBTERR;

                } else if ((-1 == c) && ((serrno - ERMCRBTERR) == 7)) {
                        
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
                        
                        int mounted, RETRIES = 3, retryCtr = 0;
                        int save_serrno = serrno; 

                        p = strrchr (rmc_errbuf, ':');
			sprintf (msg, TP041, "mount", vid, cur_unm,
                                 p ? p + 2 : rmc_errbuf);
                        tplogit (func, "%s", msg);
                        tl_tpdaemon.tl_log( &tl_tpdaemon, 41, 5,
                                            "func",    TL_MSG_PARAM_STR, func,
                                            "action",  TL_MSG_PARAM_STR, "mount",
                                            "cur_vid", TL_MSG_PARAM_STR, vid,
                                            "Drive",   TL_MSG_PARAM_STR, cur_unm,
                                            "Message", TL_MSG_PARAM_STR, p ? p + 2 : rmc_errbuf );
                        
                        while (retryCtr < RETRIES) {
                                
                                /* check if the tape is mounted or not */
                                mounted = istapemounted( vid, drvord, smc_ldr );
                                if (0 == mounted) {

                                        tplogit (func, "Encountered 'Volume in Use'. Tape not mounted locally. Return Error.\n" );
                                        tl_tpdaemon.tl_log( &tl_tpdaemon, 104, 2,
                                                            "func"   , TL_MSG_PARAM_STR, func,
                                                            "Message", TL_MSG_PARAM_STR, "Encountered EBUSY. Tape not mounted locally. Return Error." );

                                        c = (save_serrno == SECOMERR) ? RBT_FAST_RETRY : save_serrno - ERMCRBTERR;
                                        RETURN (c);

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
                        c = (save_serrno == SECOMERR) ? RBT_FAST_RETRY : save_serrno - ERMCRBTERR;

                } else if (c) {

                        /* log information about the error condition         */
                        if (serrno != SECOMERR) {
                                tplogit (func, "Error in smcmount: c=%d, serrno=%d, (serrno-ERMCRBTERR)=%d\n", 
                                         c, serrno, (serrno - ERMCRBTERR) );                        
                                tl_tpdaemon.tl_log( &tl_tpdaemon, 41, 5,
                                                    "func"               , TL_MSG_PARAM_STR, func,
                                                    "Message"            , TL_MSG_PARAM_STR, "Error in smcmount",
                                                    "c"                  , TL_MSG_PARAM_INT, c, 
                                                    "serrno"             , TL_MSG_PARAM_INT, serrno, 
                                                    "(serrno-ERMCRBTERR)", TL_MSG_PARAM_INT, (serrno - ERMCRBTERR) );
                        }                        
                        p = strrchr (rmc_errbuf, ':');
			sprintf (msg, TP041, "mount", vid, cur_unm,
                                 p ? p + 2 : rmc_errbuf);
                        /* Just send message to operator after two retries*/ 
                        tplogit (func, "%s", msg);
                        tl_tpdaemon.tl_log( &tl_tpdaemon, 41, 5,
                                            "func",    TL_MSG_PARAM_STR, func,
                                            "action",  TL_MSG_PARAM_STR, "mount",
                                            "cur_vid", TL_MSG_PARAM_STR, vid,
                                            "Drive",   TL_MSG_PARAM_STR, cur_unm,
                                            "Message", TL_MSG_PARAM_STR, p ? p + 2 : rmc_errbuf );
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
                        tl_tpdaemon.tl_log( &tl_tpdaemon, 42, 5,
                                            "func",    TL_MSG_PARAM_STR, func,
                                            "smc_ldr", TL_MSG_PARAM_STR, smc_ldr,
                                            "Message", TL_MSG_PARAM_STR, "find_cartridge", 
                                            "Error",   TL_MSG_PARAM_STR, p ? p + 2 : msgaddr,
                                            "Drive",   TL_MSG_PARAM_STR, cur_unm );
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
                                    "Drive",   TL_MSG_PARAM_STR, cur_unm,
                                    "Message", TL_MSG_PARAM_STR, "volume not in library" );
		RETURN (RBT_NORETRY);
	}
	if (element_info.element_type != 2) {
		sprintf (msg, TP041, "mount", vid, cur_unm, "volume in use");
		usrmsg (func, "%s\n", msg);
                tl_tpdaemon.tl_log( &tl_tpdaemon, 41, 5,
                                    "func",    TL_MSG_PARAM_STR, func,
                                    "action",  TL_MSG_PARAM_STR, "mount",
                                    "cur_vid", TL_MSG_PARAM_STR, vid,
                                    "Drive",   TL_MSG_PARAM_STR, cur_unm,
                                    "Message", TL_MSG_PARAM_STR, "volume in use" );
		RETURN (RBT_SLOW_RETRY);
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
                                            "Drive",   TL_MSG_PARAM_STR, cur_unm,
                                            "Message", TL_MSG_PARAM_STR, p ? p + 2 : msgaddr );
                }
		RETURN (c);
	}
	RETURN (0);
}

int smcdismount(char *vid,
                char *loader,
                int force,
                int vsnretry)
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
                        /* this will never happen, since c == -1 on error    */
                        RETURN (RBT_FAST_RETRY);

                } else if ((-1 == c) && ((serrno - ERMCRBTERR) == EBUSY)) {

                        /* this should trigger a retry on a busy error       */ 

                        p = strrchr (rmc_errbuf, ':');
                        sprintf (msg, TP041, "demount", vid, cur_unm,
                                 p ? p + 2 : rmc_errbuf);
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

                } else if (c) {
                        /* log information about the error condition         */
                        if (serrno != SECOMERR) {
                                tplogit (func, "Error in smcdismount: c=%d, serrno=%d, (serrno-ERMCRBTERR)=%d\n", 
                                         c, serrno, (serrno - ERMCRBTERR) );                        
                                tl_tpdaemon.tl_log( &tl_tpdaemon, 41, 5,
                                                    "func"               , TL_MSG_PARAM_STR, func,
                                                    "Message"            , TL_MSG_PARAM_STR, "Error in smcdismount",
                                                    "c"                  , TL_MSG_PARAM_INT, c, 
                                                    "serrno"             , TL_MSG_PARAM_INT, serrno, 
                                                    "(serrno-ERMCRBTERR)", TL_MSG_PARAM_INT, (serrno - ERMCRBTERR) );
                        }
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
                                                    "Drive",   TL_MSG_PARAM_STR, cur_unm,
                                                    "Message", TL_MSG_PARAM_STR, p ? p + 2 : rmc_errbuf );
                        } else if (vsnretry <= 3 && (serrno == SECOMERR)){
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
                        tl_tpdaemon.tl_log( &tl_tpdaemon, 42, 5,
                                            "func",    TL_MSG_PARAM_STR, func,
                                            "smc_ldr", TL_MSG_PARAM_STR, smc_ldr,
                                            "Message", TL_MSG_PARAM_STR, "read_elem_status", 
                                            "Error",   TL_MSG_PARAM_STR, p ? p + 2 : msgaddr,
                                            "Drive",   TL_MSG_PARAM_STR, cur_unm );
		}
		RETURN (c);
	}
	if ((element_info.state & 0x1) == 0) {
		usrmsg (func, TP041, "demount", vid, cur_unm, "Medium Not Present");
                tl_tpdaemon.tl_log( &tl_tpdaemon, 41, 5,
                                    "func",    TL_MSG_PARAM_STR, func,
                                    "action",  TL_MSG_PARAM_STR, "demount",
                                    "cur_vid", TL_MSG_PARAM_STR, vid,
                                    "Drive",   TL_MSG_PARAM_STR, cur_unm,
                                    "Message", TL_MSG_PARAM_STR, "Medium Not Present" );
		RETURN (RBT_OK);
	}
	if ((element_info.state & 0x8) == 0) {
		usrmsg (func, TP041, "demount", vid, cur_unm, "Drive Not Unloaded");
                tl_tpdaemon.tl_log( &tl_tpdaemon, 41, 5,
                                    "func",    TL_MSG_PARAM_STR, func,
                                    "action",  TL_MSG_PARAM_STR, "demount",
                                    "cur_vid", TL_MSG_PARAM_STR, vid,
                                    "Drive",   TL_MSG_PARAM_STR, cur_unm,
                                    "Message", TL_MSG_PARAM_STR, "Drive Not Unloaded" );
		RETURN (RBT_UNLD_DMNT);
	}
	if (*vid && !force && strcmp (element_info.name, vid)) {
		usrmsg (func, TP050, vid, element_info.name);
                tl_tpdaemon.tl_log( &tl_tpdaemon, 50, 3,
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
                                            "Drive",   TL_MSG_PARAM_STR, cur_unm,
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

        if (rmc_get_geometry( rmc_host, smc_ldr, &robot_info) < 0) {
                
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

        if (rmc_read_elem_status (rmc_host, smc_ldr, 4,
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
