/*
 * Copyright (C) 1990-2003 by CERN/IT/PDP/DM
 * All rights reserved
 */

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <pwd.h>
#include <string.h>
#include <sys/time.h>
#include "Ctape.h"
#include "Ctape_api.h"
#include "marshall.h"
#include "sacct.h"
#include "serrno.h"
#include "net.h"
#include "vdqm_api.h"
#include "tplogger_api.h"
#include <time.h>

char *devtype;
char *dvrname;
char errbuf[512];
char func[16];
char hostname[CA_MAXHOSTNAMELEN+1];
int jid;
int maxfds;
char msg[OPRMSGSZ];
char orepbuf[OPRMSGSZ];
fd_set readmask;
char repbuf[133];
int rpfd;

/*
** Prototypes
*/
void configdown( char* );
int rbtdmntchk( int*, char*, unsigned int* );

int main(int	argc,
         char	**argv)
{
	int c;
	unsigned int demountforce;
	char *dgn;
	char *drive;
	char *dvn;
	char *getconfent();
	gid_t gid;
	char *loader;
	int msglen;
	int n;
	char *p;
	char *q;
	int rlsflags;
	char *sbp;
	char sendbuf[REQBUFSZ];
	int tapefd;
	uid_t uid;
	int ux;
	int vdqm_rc;
	int vdqm_status;
	char *vid;
        int tapealerts;
        int harderror;
        int mediaerror;
        int readfailure;
        int writefailure;
        int vsnretry;

        time_t TStartUnmount, TEndUnmount, TUnmount;

	ENTRY (rlstape);

        TStartUnmount = (int)time(NULL);        

        p = getconfent ("TAPE", "TPLOGGER", 0);
        if (p && (0 == strcasecmp(p, "SYSLOG"))) {
                tl_init_handle( &tl_tpdaemon, "syslog" ); 
        } else {
                tl_init_handle( &tl_tpdaemon, "dlf" );  
        }
        tl_tpdaemon.tl_init( &tl_tpdaemon, 0 );
 
  if (18 != argc) {
    printf("Wrong number of arguments\n");
    exit(-1);
  }

	drive = argv[1];
	vid = argv[2];
	dvn = argv[3];
	rpfd = atoi (argv[4]);
	uid = atoi (argv[5]);
	gid = atoi (argv[6]);
	jid = atoi (argv[9]);
	ux = atoi (argv[10]);
	rlsflags = atoi (argv[11]);
	dgn = argv[12];
	devtype = argv[13];
	dvrname = argv[14];
	loader = argv[15];
        tapealerts = 0;
        harderror = 0;
        mediaerror = 0;
        readfailure = 0;
        writefailure = 0;  
        vsnretry = 0;

	tplogit (func, "rls dvn=<%s>, vid=<%s>, rlsflags=%d\n", dvn, vid, rlsflags);
        tl_tpdaemon.tl_log( &tl_tpdaemon, 111, 6,
                            "func"    , TL_MSG_PARAM_STR  , func,
                            "Message" , TL_MSG_PARAM_STR  , "rls",
                            "dvn"     , TL_MSG_PARAM_STR  , dvn,
                            "vid"     , TL_MSG_PARAM_STR  , vid,
                            "rlsflags", TL_MSG_PARAM_INT  , rlsflags,
                            "TPVID"   , TL_MSG_PARAM_TPVID, vid );
        
        if (rlsflags & TPRLS_DELAY) {
                int slp = 300;
                if ((p = getconfent ("TAPE", "CRASHED_RLS_HANDLING_RETRY_DELAY", 0))) {
                        slp = atoi(p)>0?atoi(p):300;
                }                
                tplogit (func, "release delayed for %d seconds\n", slp);
                tl_tpdaemon.tl_log( &tl_tpdaemon, 111, 5,
                                    "func"    , TL_MSG_PARAM_STR  , func,
                                    "Message" , TL_MSG_PARAM_STR  , "release delayed",
                                    "seconds" , TL_MSG_PARAM_INT  , slp,
                                    "vid"     , TL_MSG_PARAM_STR  , vid,
                                    "TPVID"   , TL_MSG_PARAM_TPVID, vid );
                sleep(slp);
        }        

	(void) Ctape_seterrbuf (errbuf, sizeof(errbuf));

	while ((tapefd = open (dvn, O_RDONLY|O_NDELAY)) < 0 && errno == EBUSY)
		sleep (UCHECKI);
	if (tapefd >= 0) {
	
          tapealerts = get_tape_alerts(tapefd, dvn, devtype);
    	  if (tapealerts >= 0) {
       	      harderror = tapealerts & 0x0001;
       	      mediaerror = (tapealerts >> 2) & 0x0001;
       	      readfailure = (tapealerts >> 4) & 0x0001;
       	      writefailure = (tapealerts >> 8) & 0x0001;
              tplogit (func, "tape alerts: hardware error %d, media error %d, read failure %d, write failure %d\n", 
                       harderror,
                       mediaerror, 
                       readfailure,
                       writefailure);
              tl_tpdaemon.tl_log( &tl_tpdaemon, (harderror || mediaerror || readfailure || writefailure) ? 80 : 82, 8,
                                  "func"          , TL_MSG_PARAM_STR,   func,
                                  "Message"       , TL_MSG_PARAM_STR,   "tape alerts",
                                  "hardware error", TL_MSG_PARAM_INT,   harderror,
                                  "media error"   , TL_MSG_PARAM_INT,   mediaerror,
                                  "read failure"  , TL_MSG_PARAM_INT,   readfailure,
                                  "write failure" , TL_MSG_PARAM_INT,   writefailure,
                                  "JobID"         , TL_MSG_PARAM_INT,   jid,
                                  "TPVID"         , TL_MSG_PARAM_TPVID, vid );
              
              if (tapealerts > 0) {
                      
                      char *p = NULL;
                                            
                      p = getconfent( "TAPE", "DOWN_ON_TPALERT", 0 );
                      if (NULL != p) {
                              /* found a config entry */
                              if ( (0 == strcmp( p, "yes")) || (0 == strcmp( p, "YES"))) {                               
                                      tplogit( func, "Configure the drive down (config)\n" );
                                      tl_tpdaemon.tl_log( &tl_tpdaemon, 103, 5,
                                                          "func"   , TL_MSG_PARAM_STR  , func,
                                                          "Message", TL_MSG_PARAM_STR  , "Configure the drive down (config)",
                                                          "JobID"  , TL_MSG_PARAM_INT,   jid,
                                                          "vid"    , TL_MSG_PARAM_STR  , vid,
                                                          "TPVID"  , TL_MSG_PARAM_TPVID, vid );
                                      configdown (drive);
                              } else {
                                      tplogit( func, "Leave the drive up (config)\n" );
                                      tl_tpdaemon.tl_log( &tl_tpdaemon, 104, 5,
                                                          "func"   , TL_MSG_PARAM_STR  , func,
                                                          "Message", TL_MSG_PARAM_STR  , "Leave the drive up (config)",
                                                          "JobID"  , TL_MSG_PARAM_INT,   jid,
                                                          "vid"    , TL_MSG_PARAM_STR  , vid,
                                                          "TPVID"  , TL_MSG_PARAM_TPVID, vid );
                              }
                      } else {
                              /* default: configure the drive down */
                              tplogit( func, "Leave the drive up (default)\n" );
                              tl_tpdaemon.tl_log( &tl_tpdaemon, 103, 5,
                                                  "func"   , TL_MSG_PARAM_STR  , func,
                                                  "Message", TL_MSG_PARAM_STR  , "Leave the drive up (default)",
                                                  "JobID"  , TL_MSG_PARAM_INT,   jid,
                                                  "vid"    , TL_MSG_PARAM_STR  , vid,
                                                  "TPVID"  , TL_MSG_PARAM_TPVID, vid );
                      }
              }

    	  } else {

	    	tplogit (func, "tape alerts: no information available\n");
                tl_tpdaemon.tl_log( &tl_tpdaemon, 81, 3,
                                    "func"   , TL_MSG_PARAM_STR  , func,
                                    "Message", TL_MSG_PARAM_STR  , "tape alerts: no information available",
                                    "TPVID"  , TL_MSG_PARAM_TPVID, vid );
    	  }

	  close (tapefd);
 	} else {
                        {
                                tplogit (func, TP042, dvn, "open", strerror(errno));
                                tl_tpdaemon.tl_log( &tl_tpdaemon, 42, 7,
                                                    "func"   , TL_MSG_PARAM_STR  , func,
                                                    "dvn"    , TL_MSG_PARAM_STR  , dvn,
                                                    "Message", TL_MSG_PARAM_STR  , "open",
                                                    "Error"  , TL_MSG_PARAM_STR  , strerror(errno),
                                                    "JobID"  , TL_MSG_PARAM_INT  , jid,
                                                    "Drive"  , TL_MSG_PARAM_STR  , drive, 
                                                    "TPVID"  , TL_MSG_PARAM_TPVID, vid );
                        }
 	}

	/* delay VDQM_UNIT_RELEASE so that a new request for the same volume
	   has a chance to keep the volume mounted */

	if ((p = getconfent ("TAPE", "RLSDELAY", 0)))
		sleep (atoi (p));

	vdqm_status = VDQM_UNIT_RELEASE;
	if (rlsflags & TPRLS_UNLOAD) {
		tplogit (func, "The TPRLS_UNLOAD bit of rlsflags has been set\n");
		vdqm_status |= VDQM_FORCE_UNMOUNT;
	}

	tplogit (func, "calling vdqm_UnitStatus(VDQM_UNIT_RELEASE)\n");
        tl_tpdaemon.tl_log( &tl_tpdaemon, 111, 3,
                            "func"   , TL_MSG_PARAM_STR  , func,
                            "Message", TL_MSG_PARAM_STR  , "calling vdqm_UnitStatus(VDQM_UNIT_RELEASE)",
                            "TPVID"  , TL_MSG_PARAM_TPVID, vid );
        while ((vdqm_rc = vdqm_UnitStatus (NULL, vid, dgn, NULL, drive,
		&vdqm_status, NULL, jid)) &&
		(serrno == SECOMERR || serrno == EVQHOLD))
			sleep (60);
	tplogit (func, "vdqm_UnitStatus returned %s\n",
		vdqm_rc ? sstrerror(serrno) : "ok");
        tl_tpdaemon.tl_log( &tl_tpdaemon, vdqm_rc ? 103 : 111, 6,
                            "func"   , TL_MSG_PARAM_STR,   func,
                            "Message", TL_MSG_PARAM_STR,   "vdqm_UnitStatus returned",
                            "Error"  , TL_MSG_PARAM_STR,   vdqm_rc ? sstrerror(serrno) : "ok",
                            "JobID"  , TL_MSG_PARAM_INT,   jid,
                            "vid"    , TL_MSG_PARAM_STR,   vid,
                            "TPVID"  , TL_MSG_PARAM_TPVID, vid );  
	if (vdqm_rc == 0 && (vdqm_status & VDQM_VOL_UNMOUNT) == 0 &&
	    (rlsflags & TPRLS_UNLOAD) == 0) {
		rlsflags |= TPRLS_NOUNLOAD;
		goto freevol;
	}
	if (*vid == '\0')	/* mount req failed or killed */
		goto vol_unmount;	/* before volume was requested on drive */
	if (rlsflags & TPRLS_NOUNLOAD)
		goto freevol;

	gethostname (hostname, CA_MAXHOSTNAMELEN+1);

        /* initialize for select */

#if defined(linux)
        maxfds = getdtablesize();
#else
        maxfds = _NFILE;
#endif
        FD_ZERO (&readmask);

unload_loop:
	while ((tapefd = open (dvn, O_RDONLY|O_NDELAY)) < 0 && errno == EBUSY)
		sleep (UCHECKI);
	if (tapefd >= 0) {
		if ((c = chkdriveready (tapefd)) < 0) {
			configdown (drive);
		} else if (c > 0) {
			if (*loader != 'n') {
				if (unldtape (tapefd, dvn) < 0) {                                        
                                        char *p = NULL;                                        
                                        p = getconfent( "TAPE", "DOWN_ON_UNLOAD_FAILURE", 0 );
                                        if ((NULL == p) || (0 == strcasecmp(p, "NO"))) {
                                                char msg[] = "Leave drive up after unload failure";
                                                tplogit(func, "%s\n", msg);
                                                tl_tpdaemon.tl_log(&tl_tpdaemon, 104, 2,
                                                                   "func"   , TL_MSG_PARAM_STR, func,
                                                                   "Message", TL_MSG_PARAM_STR, msg);
                                        } else {
                                                char msg[] = "Put drive down after unload failure";
                                                tplogit(func, "%s\n", msg);
                                                tl_tpdaemon.tl_log(&tl_tpdaemon, 103, 2,
                                                                   "func"   , TL_MSG_PARAM_STR, func,
                                                                   "Message", TL_MSG_PARAM_STR, msg);
                                                configdown (drive);
                                        }
                                }
                        }
			tapeacct (TPUNLOAD, uid, gid, jid, dgn, drive, vid, 0, 0);
		}
		close (tapefd);
	} else {
                        {
                                tplogit (func, TP042, dvn, "open", strerror(errno));
                                tl_tpdaemon.tl_log( &tl_tpdaemon, 42, 7,
                                                    "func"   , TL_MSG_PARAM_STR  , func,
                                                    "dvn"    , TL_MSG_PARAM_STR  , dvn,
                                                    "Message", TL_MSG_PARAM_STR  , "open",
                                                    "Error"  , TL_MSG_PARAM_STR  , strerror(errno),
                                                    "JobID"  , TL_MSG_PARAM_INT  , jid,
                                                    "Drive"  , TL_MSG_PARAM_STR  , drive, 
                                                    "TPVID"  , TL_MSG_PARAM_TPVID, vid );
                        }
	}
	c = 0;

	if (*loader != 'm') {
		demountforce = 0;
		do {
                        vsnretry++;
                        tl_tpdaemon.tl_log( &tl_tpdaemon, 79, 7,
                                            "func"    , TL_MSG_PARAM_STR  , func,
                                            "VID"     , TL_MSG_PARAM_STR  , vid,
                                            "Drive"   , TL_MSG_PARAM_STR  , drive, 
                                            "DGN"     , TL_MSG_PARAM_STR  , dgn,
                                            "Hostname", TL_MSG_PARAM_STR  , hostname,
                                            "JobID"   , TL_MSG_PARAM_INT  , jid,
                                            "TPVID"   , TL_MSG_PARAM_TPVID, vid );
			c = rbtdemount (vid, drive, dvn, loader, demountforce,vsnretry);
			if ((n = rbtdmntchk (&c, drive, &demountforce)) < 0)
				goto freevol;
		} while (n == 1);
		if (n == 2) goto unload_loop;
	}
vol_unmount:
	vdqm_status = VDQM_VOL_UNMOUNT;
	tplogit (func, "calling vdqm_UnitStatus(VDQM_VOL_UNMOUNT)\n");
        tl_tpdaemon.tl_log( &tl_tpdaemon, 111, 3,
                            "func"   , TL_MSG_PARAM_STR,   func,
                            "Message", TL_MSG_PARAM_STR,   "calling vdqm_UnitStatus(VDQM_VOL_UNMOUNT)",
                            "TPVID"  , TL_MSG_PARAM_TPVID, vid );
	while ((vdqm_rc = vdqm_UnitStatus (NULL, vid, dgn, NULL, drive,
		&vdqm_status, NULL, 0)) &&
		(serrno == SECOMERR || serrno == EVQHOLD))
			sleep (60);
	tplogit (func, "vdqm_UnitStatus returned %s\n",
		vdqm_rc ? sstrerror(serrno) : "ok");
        tl_tpdaemon.tl_log( &tl_tpdaemon, vdqm_rc ? 103 : 111, 6,
                            "func"   , TL_MSG_PARAM_STR  , func,
                            "Message", TL_MSG_PARAM_STR  , "vdqm_UnitStatus returned",
                            "Error"  , TL_MSG_PARAM_STR  , vdqm_rc ? sstrerror(serrno) : "ok",
                            "JobID"  , TL_MSG_PARAM_INT  , jid,
                            "vid"    , TL_MSG_PARAM_STR  , vid,
                            "TPVID"  , TL_MSG_PARAM_TPVID, vid );        
	goto freedrv;

freevol:

freedrv:

	/* Build FREEDRV request header */

	sbp = sendbuf;
	marshall_LONG (sbp, TPMAGIC);
	marshall_LONG (sbp, FREEDRV);
	q = sbp;	/* save pointer. The next field will be updated */
	msglen = 3 * LONGSIZE;
	marshall_LONG (sbp, msglen);

	/* Build request body */

	marshall_LONG (sbp, uid);
	marshall_LONG (sbp, gid);
	marshall_LONG (sbp, jid);
	marshall_WORD (sbp, rlsflags);
	marshall_WORD (sbp, rpfd);
	marshall_WORD (sbp, ux);
 
	msglen = sbp - sendbuf;
	marshall_LONG (q, msglen);	/* update length field */
 
	if ((c = send2tpd (NULL, sendbuf, msglen, NULL, 0))) {
		usrmsg (func, "%s", errbuf);
                tl_tpdaemon.tl_log( &tl_tpdaemon, 111, 3,
                                    "func"   , TL_MSG_PARAM_STR  , func,
                                    "Message", TL_MSG_PARAM_STR  , errbuf,
                                    "TPVID"  , TL_MSG_PARAM_TPVID, vid );
        }
	if (c < 0) c = serrno;

        tl_tpdaemon.tl_log( &tl_tpdaemon, 75, 3,
                            "func" , TL_MSG_PARAM_STR  , func,
                            "vid"  , TL_MSG_PARAM_STR  , vid,
                            "TPVID", TL_MSG_PARAM_TPVID, vid );

        /* get the time for unmount */
        TEndUnmount = (int)time(NULL); 
        TUnmount = ((time_t)TEndUnmount - (time_t)TStartUnmount);
        
        tl_tpdaemon.tl_log( &tl_tpdaemon, 92, 8,
                            "func"       , TL_MSG_PARAM_STR  , func,
                            "unmounttime", TL_MSG_PARAM_INT  , TUnmount,
                            "tapemoved"  , TL_MSG_PARAM_STR  , (rlsflags & TPRLS_NOUNLOAD)?"no":"yes",
                            "Drive"      , TL_MSG_PARAM_STR  , drive, 
                            "DGN"        , TL_MSG_PARAM_STR  , dgn,
                            "JobID"      , TL_MSG_PARAM_INT  , jid,
                            "vid"        , TL_MSG_PARAM_STR  , vid,
                            "TPVID"      , TL_MSG_PARAM_TPVID, vid );

        tl_tpdaemon.tl_exit( &tl_tpdaemon, 0 );
	exit (c);
}

void configdown(char *drive)
{
	sprintf (msg, TP033, drive, hostname); /* ops msg */
	usrmsg ("rlstape", "%s\n", msg);
        tl_tpdaemon.tl_log( &tl_tpdaemon, 33, 4,
                            "func",     TL_MSG_PARAM_STR, "rlstape",
                            "Message",  TL_MSG_PARAM_STR, msg,
                            "Drive",    TL_MSG_PARAM_STR, drive, 
                            "Hostname", TL_MSG_PARAM_STR, hostname );

	(void) Ctape_config (drive, CONF_DOWN, TPCD_SYS);
}

int rbtdmntchk(int *c,
               char *drive,
               unsigned int *demountforce)
{
	fd_set readfds;
	struct timeval rbttimeval;

	switch (*c) {
	case 0:
		return (0);
	case RBT_FAST_RETRY:
                tplogit (func, "RBT_FAST_RETRY %s\n", msg);
                tl_tpdaemon.tl_log( &tl_tpdaemon, 111, 2,
                                    "func",    TL_MSG_PARAM_STR, func,
                                    "Message", TL_MSG_PARAM_STR, msg );        

		rbttimeval.tv_sec = RBTFASTRI;
		rbttimeval.tv_usec = 0;
		memcpy (&readfds, &readmask, sizeof(readmask));
                if (select (maxfds, &readfds, (fd_set *)0,
                            (fd_set *)0, &rbttimeval) > 0) {
                        /* usually the operator's reply was 
                           checked here; should not happen,
                           retry none the less ... */
                        tplogit (func, "select returned >0\n");
                        tl_tpdaemon.tl_log( &tl_tpdaemon, 111, 2,
                                            "func",    TL_MSG_PARAM_STR, func,
                                            "Message", TL_MSG_PARAM_STR, "select returned >0" );
                        return (1);
                }
                /* retry after timeout */
                return (1);	
	case RBT_DMNT_FORCE:
		if (*demountforce) {
			configdown (drive);
			*c = EIO;
			return (-1);
		} else {
			*demountforce = 1;
			return (1);	/* retry */
		}
	case RBT_CONF_DRV_DN:
		configdown (drive);
		*c = EIO;
		return (-1);
	case RBT_UNLD_DMNT:
                /* add a delay of  RBTUNLDDMNTI */
                tplogit (func, "RBT_UNLD_DMNT %s\n", msg);
                tl_tpdaemon.tl_log( &tl_tpdaemon, 111, 2,
                                    "func",    TL_MSG_PARAM_STR, func,
                                    "Message", TL_MSG_PARAM_STR, msg );        

		rbttimeval.tv_sec = RBTUNLDDMNTI;
		rbttimeval.tv_usec = 0;
		memcpy (&readfds, &readmask, sizeof(readmask));
                if (select (maxfds, &readfds, (fd_set *)0,
                            (fd_set *)0, &rbttimeval) > 0) {
                        tplogit (func, "select returned >0\n");
                        tl_tpdaemon.tl_log( &tl_tpdaemon, 111, 2,
                                            "func",    TL_MSG_PARAM_STR, func,
                                            "Message", TL_MSG_PARAM_STR, "select returned >0" );
                        return (2);
                }
                /* unload and retry after timeout */
		return (2);	
	default:
		configdown (drive);
		return (-1);	/* unrecoverable error */
	}
}
