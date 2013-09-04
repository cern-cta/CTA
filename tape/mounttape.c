/*
 * Copyright (C) 1990-2003 by CERN/IT/PDP/DM
 * All rights reserved
 */

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <pwd.h>
#include <signal.h>
#include <fcntl.h>
#if linux
#include <sys/mtio.h>
#endif
#include <netinet/in.h>
#include <string.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/param.h>
#include "Ctape.h"
#include "Ctape_api.h"
#include "marshall.h"
#include "sacct.h"
#include "serrno.h"
#include "net.h"
#include "vdqm_api.h"
#include "vmgr_api.h"
#include "tplogger_api.h"
#include <sys/ioctl.h>
#include <sys/mtio.h>
#include <time.h>
#include <Ctape_api.h>

static char *acctname;
char *devtype;
static char *drive;
static char errbuf[512];
static char func[16];
static gid_t gid;
static char hostname[CA_MAXHOSTNAMELEN+1];
int jid;
static char *loader;
static int mode;
static int msg_num;
static char *name;
static char *path;
int rpfd;
static int tapefd;
static uid_t uid;
static int updvsn_done = -1;
static char *vid;
static int do_cleanup_after_mount = 0;
static int mount_ongoing = 0;

static void configdown( char* );
static int Ctape_updvsn( uid_t, gid_t, int, int, char*, char*, int, int, int );
static int rbtmountchk( int*, char*, char*, char*, char* );
static int rbtdmntchk( int*, char*, unsigned int* );
static int repairbadmir( int tapefd, char* path );
static void mountkilled();
static void cleanup();

int main(int	argc,
         char	**argv)
{
	int c;
	char *clienthost;
	unsigned int demountforce;
	int den;
	char density [CA_MAXDENLEN+1];
	struct devinfo *devinfo;
	char *dgn;
	char *dvn;
	char hdr1[81];
	char hdr2[81];
	static char labels[6][4] = {"", "al", "nl", "sl", "blp", "aul"};
	int lblcode;
#if linux
	struct mtop mtop;
#endif
	int n;
	int needrbtmnt;
	char *p;
	int prelabel;
	char repbuf[REPBUFSZ];
	char rings[9];
	char *sbp;
	int scsi;
	int side;
	int Tflag = 0;
        int Fflag = 0; /* force flag */
	int tplbl;
	int tpmode;
	int tpmounted;
	char tpvsn[CA_MAXVSNLEN+1];
	int ux;
	int vdqm_rc;
	int vdqm_reqid;
	int vdqm_status;
	char *vsn;
	char vol1[LBLBUFSZ];
	int vsnretry = 0;
	char *why;
	int why4a;
    
        time_t TStartMount, TEndMount, TMount;

        char *getconfent();

	ENTRY (mounttape);

        TStartMount = (int)time(NULL);      

        p = getconfent ("TAPE", "TPLOGGER", 0);
        if (p && (0 == strcasecmp(p, "SYSLOG"))) {
                tl_init_handle( &tl_tpdaemon, "syslog" ); 
        } else {
                tl_init_handle( &tl_tpdaemon, "dlf" );  
        }
        tl_tpdaemon.tl_init( &tl_tpdaemon, 0 );

  if (25 != argc) {
    printf("Wrong number of arguments\n");
    exit(-1);
  }

	drive = argv[1];
	vid = argv[2];
	dvn = argv[3];
	rpfd = atoi(argv[4]);
	uid = atoi (argv[5]);
	gid = atoi (argv[6]);
	name = argv[7];
	acctname = argv[8];
	jid = atoi (argv[9]);
	ux = atoi(argv[10]);
	dgn = argv[11];
	devtype = argv[12];
	/* The driver name in argv[13] is no longer used */
	loader = argv[14];
	mode = atoi (argv[15]);
	lblcode = atoi (argv[16]);
	vsn = argv[17];
	path = argv[18];
	den = atoi (argv[19]);
	prelabel = atoi (argv[20]);
	vdqm_reqid = atoi (argv[21]);
	tpmounted = atoi (argv[22]);
	side = atoi (argv[23]);
	clienthost = argv[24];

	if ((prelabel > 2) && (prelabel & DOUBLETM)) {
                tplogit (func, "DOUBLETM option set\n");
		prelabel -= DOUBLETM;
		Tflag = 1;
	}
	if ((prelabel > 2) && (prelabel & FORCEPRELBL)) {
                tplogit (func, "FORCEPRELBL option set\n");
		prelabel -= FORCEPRELBL;
                Fflag = 1;
	}
	scsi = 1;

	c = 0;
	(void) Ctape_seterrbuf (errbuf, sizeof(errbuf));
	devinfo = Ctape_devinfo (devtype);
	gethostname (hostname, CA_MAXHOSTNAMELEN+1);

        /* signal (SIGINT, mountkilled); */
        {
                int rc = 0;
                struct sigaction sa;
                sigset_t sigset;

                memset( &sa,'\0',sizeof( sa ) );

                sigemptyset( &sigset );
                sigaddset( &sigset, SIGINT );

                sa.sa_handler = (void (*)(int))mountkilled;
                sa.sa_mask    = sigset;
                sa.sa_flags   = SA_RESTART;

                rc = sigaction( SIGINT, &sa, NULL );
                if( -1 == rc ) {
                        tplogit (func, "Error in sigaction\n");
                        tl_tpdaemon.tl_log( &tl_tpdaemon, 103, 5,
                                            "func",    TL_MSG_PARAM_STR  , func,
                                            "Message", TL_MSG_PARAM_STR  , "Error in sigaction",
                                            "JobID"  , TL_MSG_PARAM_INT  , jid,
                                            "vid"    , TL_MSG_PARAM_STR  , vid,
                                            "TPVID"  , TL_MSG_PARAM_TPVID, vid );
                }
        }

	vdqm_status = VDQM_UNIT_ASSIGN;
	tplogit (func, "calling vdqm_UnitStatus(VDQM_UNIT_ASSIGN)\n");
        tl_tpdaemon.tl_log( &tl_tpdaemon, 111, 3,
                            "func"   , TL_MSG_PARAM_STR  , func,
                            "Message", TL_MSG_PARAM_STR  , "calling vdqm_UnitStatus(VDQM_UNIT_ASSIGN)",
                            "TPVID"  , TL_MSG_PARAM_TPVID, vid);
	while ((vdqm_rc = vdqm_UnitStatus (NULL, NULL, dgn, NULL, drive,
		&vdqm_status, &vdqm_reqid, jid)) &&
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

	updvsn_done = 0;
	density[0] = '\0';
	if ((c = vmgrchecki (vid, vsn, dgn, density, labels[lblcode], mode, uid, gid, clienthost)))
		goto reply;

	if (tpmounted) {	/* tape already mounted */
		if (!scsi)
			tapefd = open (path, O_RDONLY);
		else
			tapefd = open (path, O_RDONLY|O_NDELAY);
		if (tapefd >= 0) {
			if (chkdriveready (tapefd) > 0) goto mounted;
			else close (tapefd);
		}
	}

	/* build mount message */

	if (mode == WRITE_DISABLE)
		strcpy (rings, "read");
	else
		strcpy (rings, "write");
	why = "";
	why4a = 0;
	msg_num = 0;

	/* send vid, vsn and flag "to be mounted" to the tape daemon */

	if ((c = Ctape_updvsn (uid, gid, jid, ux, vid, vsn, 1, lblcode, mode)))
		goto reply;
	updvsn_done = 1;

	tapeacct (TP2MOUNT, uid, gid, jid, dgn, drive, vid, 0, why4a);

	if (*loader != 'm')
		needrbtmnt = 1;

	while (1) {
		{
			char msg[OPRMSGSZ];
                	snprintf (msg, sizeof(msg), TP020, vid, labels[lblcode], rings, drive, hostname, 	 
                         name, jid, why);
			msg[sizeof(msg) - 1] = '\0';
                
                	tplogit (func, "%s\n", msg);
                	tl_tpdaemon.tl_log( &tl_tpdaemon, 78, 12,
                                    "func"    , TL_MSG_PARAM_STR  , func,
                                    "Message" , TL_MSG_PARAM_STR  , msg,
                                    "VID"     , TL_MSG_PARAM_STR  , vid,
                                    "Label"   , TL_MSG_PARAM_STR  , labels[lblcode],
                                    "Rings"   , TL_MSG_PARAM_STR  , rings, 
                                    "Drive"   , TL_MSG_PARAM_STR  , drive, 
                                    "DGN"     , TL_MSG_PARAM_STR  , dgn,
                                    "Hostname", TL_MSG_PARAM_STR  , hostname, 
                                    "Name"    , TL_MSG_PARAM_STR  , name,
                                    "JobID"   , TL_MSG_PARAM_INT  , jid,
                                    "Reason"  , TL_MSG_PARAM_STR  , why,
                                    "TPVID"   , TL_MSG_PARAM_TPVID, vid );
		}

		if (*loader != 'm' && needrbtmnt) {
			do {
                                mount_ongoing = 1;
				c = rbtmount (vid, side, drive, dvn, mode, loader);
				if ( do_cleanup_after_mount == 1 ) {
					(void)cleanup();
					exit(2);
				}
				mount_ongoing = 0;
				if ((n = rbtmountchk (&c, drive, vid, dvn, loader)) < 0)
					goto reply;
			} while (n == 1);
		}

		while (1) {	/* wait for drive ready or operator cancel */
				if ((tapefd = open (path, O_RDONLY|O_NDELAY)) >= 0) {
					if (chkdriveready (tapefd) > 0) {
						tpmode = chkwriteprot (tapefd);
						break;
					} else close (tapefd);
				} else c = errno;
			if (c) {
				if (errno == ENXIO)	/* drive not operational */
					configdown (drive);
				else {
					usrmsg (func, TP042, path, "open",
						strerror(errno));
                                        tl_tpdaemon.tl_log( &tl_tpdaemon, 42, 7,
                                                            "func"   , TL_MSG_PARAM_STR  , func,
                                                            "path"   , TL_MSG_PARAM_STR  , path,
                                                            "Message", TL_MSG_PARAM_STR  , "open",
                                                            "Error"  , TL_MSG_PARAM_STR  , strerror(errno),
                                                            "JobID"  , TL_MSG_PARAM_INT  , jid,
                                                            "Drive"  , TL_MSG_PARAM_STR  , drive, 
                                                            "TPVID"  , TL_MSG_PARAM_TPVID, vid);
	                                if (strcmp (devtype, "3592") == 0) {
	                                  vdqm_status = VDQM_VOL_MOUNT;
	                                  tplogit (func, "calling vdqm_UnitStatus(VDQM_VOL_MOUNT)\n");
                                          tl_tpdaemon.tl_log( &tl_tpdaemon, 111, 3,
                                                              "func"   , TL_MSG_PARAM_STR  , func,
                                                              "Message", TL_MSG_PARAM_STR  , "calling vdqm_UnitStatus(VDQM_VOL_MOUNT)",
                                                              "TPVID"  , TL_MSG_PARAM_TPVID, vid);
	                                  while ((vdqm_rc = vdqm_UnitStatus (NULL, vid, dgn, NULL, drive,
		                                  &vdqm_status, NULL, jid)) &&
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
                                                                      "TPVID"  , TL_MSG_PARAM_TPVID, vid); 
                                        }
				}

				goto reply;
			}
#if defined(CDK)
			if (*loader == 'a') {
				c = acsmountresp();
			}
			if (*loader == 'l' || *loader == 'a') {
				if ((n = rbtmountchk (&c, drive, vid, dvn, loader)) < 0) {
					goto reply;
				}
				/* Raise EIO if RBT_FAST_RETRY or RBT_DMNT_FORCE */
				if (n == 1) {
					usrmsg (func, "Raising EIO from rbtmountchk return value of 1\n");
					serrno = EIO;
					c = EIO;
					goto reply;
				}
			}
#endif
			sleep(UCHECKI);
		}

		/* tape is ready */
                
		/* check if the volume is write protected */

		if (tpmode != mode && tpmode == WRITE_DISABLE && *loader != 'm') {
			char msg[OPRMSGSZ];
			snprintf (msg, sizeof(msg), TP041, "mount", vid, drive, "write protected");
			msg[sizeof(msg) - 1] = '\0';
			usrmsg (func, "%s\n", msg);
                        tl_tpdaemon.tl_log( &tl_tpdaemon, 41, 7,
                                            "func"   , TL_MSG_PARAM_STR  , func,
                                            "Message", TL_MSG_PARAM_STR  , msg,
                                            "Command", TL_MSG_PARAM_STR  , "mount",
                                            "VID"    , TL_MSG_PARAM_STR  , vid,
                                            "Drive"  , TL_MSG_PARAM_STR  , drive,
                                            "JobID"  , TL_MSG_PARAM_INT  , jid,
                                            "TPVID"  , TL_MSG_PARAM_TPVID, vid);
			c = ETWPROT;
			goto reply;
		}
		if (tpmode != mode && *loader == 'm') {
			if ((c = unldtape (tapefd, path)))
				goto reply;

			close (tapefd);
			why = "wrong ring status";
			why4a = TPU_WNGR;
			tapeacct (TPUNLOAD, uid, gid, jid, dgn, drive, vid, 0, TPU_WNGR);
			continue;
		}
#if linux
		mtop.mt_op = MTSETBLK;
		mtop.mt_count = 0;
		ioctl (tapefd, MTIOCTOP, &mtop);	/* set variable block size */
#endif

		/* position tape at BOT */

		if ((c = rwndtape (tapefd, path))) goto reply;

		/* set density and compression mode */

#if linux
		(void) setdens (tapefd, path, devtype, den);
#endif

		/* check VOL1 label if not blp */

		if (lblcode == BLP) break;

		if ((c = readlbl (tapefd, path, vol1)) < 0) goto reply;
		if (prelabel >= 0 && c == 3) break;	/* tape is new. ok to prelabel */
		if (prelabel >= 0 && c == 2 && readlbl (tapefd, path, vol1) > 1)
			break;	/* tape has only a single or a double tapemark */
		if (c) {	/* record read is not 80 bytes long */
			c = 0;
			tplbl = NL;
		} else if (strncmp (vol1, "VOL1", 4) == 0) tplbl = AL;
		else {
			ebc2asc (vol1, 80);
			if (strncmp (vol1, "VOL1", 4) == 0) tplbl = SL;
			else tplbl = NL;
		}
		if (tplbl != NL) {
			strncpy (tpvsn, vol1 + 4, 6);
			tpvsn[6] = '\0';
			if ((p = strchr (tpvsn, ' ')) != NULL) *p = '\0';
		}
		if (prelabel >= 0) {
			char msg[OPRMSGSZ];
			if (tplbl == NL) {
				snprintf (msg, sizeof(msg), TP062, vid, "is an NL tape", "");
			} else {
				snprintf (msg, sizeof(msg), TP062, vid, "has vsn ", tpvsn);
			}
			msg[sizeof(msg) - 1] = '\0';
			usrmsg (func, "%s\n", msg);
                        tl_tpdaemon.tl_log( &tl_tpdaemon, 62, 5,
                                            "func"   , TL_MSG_PARAM_STR  , func,
                                            "Message", TL_MSG_PARAM_STR  , msg,
                                            "VID"    , TL_MSG_PARAM_STR  , vid,
                                            "VSN"    , TL_MSG_PARAM_STR  , tpvsn,
                                            "TPVID"  , TL_MSG_PARAM_TPVID, vid );
                        if (!Fflag) {
                                /* default: do not label a prelabelled tape
                                   unless force flag is set */
                                usrmsg (func, "Tape is already labelled. Use 'tplabel -f' to override.\n", msg);
                                c = ETOPAB;
                                goto reply;                                
                        } else {        
                                /* 'operator' gave ok */
                                break;	
                        }
		}
		if (tplbl == NL && lblcode == NL) break;
		if (tplbl != NL) {
			tplogit (func, "vol1 = %s\n", vol1);
                        tl_tpdaemon.tl_log( &tl_tpdaemon, 111, 3,
                                            "func" , TL_MSG_PARAM_STR  , func,
                                            "vol1" , TL_MSG_PARAM_STR  , vol1,
                                            "TPVID", TL_MSG_PARAM_TPVID, vid);
                }
		if (tplbl == AL && lblcode == AUL) tplbl = AUL;
		if (lblcode != tplbl && vsnretry) {	/* wrong label type */
			usrmsg (func, TP021, labels[lblcode], labels[tplbl]);
                        tl_tpdaemon.tl_log( &tl_tpdaemon, 21, 4,
                                            "func"   , TL_MSG_PARAM_STR  , func,
                                            "Request", TL_MSG_PARAM_STR  , labels[lblcode],
                                            "Tape"   , TL_MSG_PARAM_STR  , labels[tplbl],
                                            "TPVID"  , TL_MSG_PARAM_TPVID, vid );			
			c = ETWLBL;
			goto reply;
		}
		if (tplbl != NL) {
			if (lblcode == tplbl && strcmp (tpvsn, vsn) == 0) break;
		}
		if (*loader != 'm' && vsnretry++) {
			usrmsg (func, TP039, vsn, tpvsn);
                        tl_tpdaemon.tl_log( &tl_tpdaemon, 39, 4,
                                            "func"   , TL_MSG_PARAM_STR  , func,
                                            "Request", TL_MSG_PARAM_STR  , vsn,
                                            "Tape"   , TL_MSG_PARAM_STR  , tpvsn,
                                            "TPVID"  , TL_MSG_PARAM_TPVID, vid );			
			c = ETWVSN;
			goto reply;
		}
		if (*loader != 'n')
			if ((n = unldtape (tapefd, path))) {
				c = n;
				goto reply;
			}
		close (tapefd);
		why = "wrong vsn";
		why4a = TPM_WNGV;
		tapeacct (TPUNLOAD, uid, gid, jid, dgn, drive, vid, 0, TPU_WNGV);
		if (*loader == 'm')
			continue;
		demountforce = 1;
		do {
            vsnretry++;  
			c = rbtdemount (vid, drive, dvn, loader, demountforce, vsnretry);
			if ((n = rbtdmntchk (&c, drive, &demountforce)) < 0)
				goto reply;
		} while (n == 1);
		if (n == 2)	/* tape not unloaded */
			goto reply;
		needrbtmnt = 1;
		continue;
	}

	/* tape is mounted */
                
    if (strcmp (devtype, "9840") == 0 ||
        strcmp (devtype, "9940") == 0 ||
        strcmp (devtype, "994B") == 0 || 
        strcmp (devtype, "T10000") == 0 ||
        strcmp (devtype, "LTO") == 0 ||
        strcmp (devtype, "3592") == 0) {
            
            /* Now checking the MIR */
            if (is_mir_invalid_load(tapefd, path, devtype) == 1) {

                    /* MIR is bad */
                    char *p = NULL;

                    tplogit(func, "Bad MIR detected vid=%s\n", vid );
                    tl_tpdaemon.tl_log( &tl_tpdaemon, 86, 4,
                                        "func"   , TL_MSG_PARAM_STR  , func,
                                        "Message", TL_MSG_PARAM_STR  , "Bad MIR detected",
                                        "Drive"  , TL_MSG_PARAM_STR  , drive, 
                                        "TPVID"  , TL_MSG_PARAM_TPVID, vid );                    

                    p = getconfent( "TAPE", "BADMIR_HANDLING", 0 );
                    if (NULL != p) {
                            
                            if (0 == strcasecmp( p, "REPAIR" )) { 
                            
                                    /* try to repair a bad MIR, cancel the request in case of a failure */
                                    int MAX_REPAIR_RETRIES = 3;
                                    int retry_counter      = 0;
                                    int mir_repaired       = 0;

                                    tplogit(func, "Bad MIR config=REPAIR vid=%s\n", vid );
                                    tl_tpdaemon.tl_log( &tl_tpdaemon, 85, 3,
                                                        "func"   , TL_MSG_PARAM_STR  , func,
                                                        "Message", TL_MSG_PARAM_STR  , "Bad MIR config=REPAIR",
                                                        "TPVID"  , TL_MSG_PARAM_TPVID, vid );

                                    while (retry_counter < MAX_REPAIR_RETRIES ) {
                                    
                                            if ( repairbadmir( tapefd, path ) < 0) {
                                                    tplogit(func, "Bad MIR repair attempt=%d vid=%s\n", retry_counter, vid);
                                                    tl_tpdaemon.tl_log( &tl_tpdaemon, 85, 4,
                                                                        "func"   , TL_MSG_PARAM_STR  , func,
                                                                        "Message", TL_MSG_PARAM_STR  , "Bad MIR repair attempt",
                                                                        "Attempt", TL_MSG_PARAM_INT  , retry_counter,
                                                                        "TPVID"  , TL_MSG_PARAM_TPVID, vid );
                                            } else {
                                                    tplogit(func, "Bad MIR repair=finished vid=%s\n", vid);
                                                    tl_tpdaemon.tl_log( &tl_tpdaemon, 90, 4,
                                                                        "func"   , TL_MSG_PARAM_STR  , func,
                                                                        "Message", TL_MSG_PARAM_STR  , "Bad MIR repair=finished",
                                                                        "VID"    , TL_MSG_PARAM_STR  , vid, 
                                                                        "TPVID"  , TL_MSG_PARAM_TPVID, vid );
                                                    tplogit(func, "Bad MIR request=continued vid=%s\n", vid);
                                                    tl_tpdaemon.tl_log( &tl_tpdaemon, 88, 4,
                                                                        "func"   , TL_MSG_PARAM_STR  , func,
                                                                        "Message", TL_MSG_PARAM_STR  , "Bad MIR request=continued",
                                                                        "VID"    , TL_MSG_PARAM_STR  , vid, 
                                                                        "TPVID"  , TL_MSG_PARAM_TPVID, vid);                                    
                                                    mir_repaired = 1;
                                                    break;
                                            }
                                            retry_counter++;
                                    }
                                    if (!mir_repaired) {
                                            
                                            /* Cancel after failed repair */
                                            tplogit(func, "Bad MIR repair=failed vid=%s\n", vid);
                                            tl_tpdaemon.tl_log( &tl_tpdaemon, 89, 4,
                                                                "func"   , TL_MSG_PARAM_STR  , func,
                                                                "Message", TL_MSG_PARAM_STR  , "Bad MIR repair=failed",
                                                                "VID"    , TL_MSG_PARAM_STR  , vid, 
                                                                "TPVID"  , TL_MSG_PARAM_TPVID, vid);

                                            tplogit(func, "Bad MIR request=canceled vid=%s\n", vid);
                                            tl_tpdaemon.tl_log( &tl_tpdaemon, 87, 5,
                                                                "func"   , TL_MSG_PARAM_STR  , func,
                                                                "Message", TL_MSG_PARAM_STR  , "Bad MIR request=canceled",
                                                                "JobID"  , TL_MSG_PARAM_INT,   jid,
                                                                "VID"    , TL_MSG_PARAM_STR  , vid, 
                                                                "TPVID"  , TL_MSG_PARAM_TPVID, vid);
                                            cleanup();
                                            sendrep (rpfd, TAPERC, ETBADMIR);
                                            exit(0);                 

                                    } else {

                                            /* check the result of the repair */
                                            if (is_mir_invalid_load(tapefd, path, devtype) == 1) {
                                                    
                                                    /* MIR still invalid */
                                                    tplogit(func, "Bad MIR recheck=invalid vid=%s\n", vid);
                                                    tl_tpdaemon.tl_log( &tl_tpdaemon, 85, 4,
                                                                        "func"   , TL_MSG_PARAM_STR  , func,
                                                                        "Message", TL_MSG_PARAM_STR  , "Bad MIR recheck=invalid",
                                                                        "VID"    , TL_MSG_PARAM_STR  , vid, 
                                                                        "TPVID"  , TL_MSG_PARAM_TPVID, vid);                                    
                                            } else {

                                                    /* MIR now valid */
                                                    tplogit(func, "Bad MIR recheck=valid vid=%s\n", vid);
                                                    tl_tpdaemon.tl_log( &tl_tpdaemon, 85, 4,
                                                                        "func"   , TL_MSG_PARAM_STR  , func,
                                                                        "Message", TL_MSG_PARAM_STR  , "Bad MIR recheck=valid",
                                                                        "VID"    , TL_MSG_PARAM_STR  , vid, 
                                                                        "TPVID"  , TL_MSG_PARAM_TPVID, vid);                                    
                                            }
                                    }

                            } else if (0 == strcasecmp( p, "IGNORE" )) {
                                    
                                     /* ignore the bad MIR and rely on the drive to repair it */
                                    tplogit(func, "Bad MIR config=IGNORE vid=%s\n", vid );
                                    tl_tpdaemon.tl_log( &tl_tpdaemon, 85, 3,
                                                        "func"   , TL_MSG_PARAM_STR  , func,
                                                        "Message", TL_MSG_PARAM_STR  , "Bad MIR config=IGNORE",
                                                        "TPVID"  , TL_MSG_PARAM_TPVID, vid );
                                    tplogit(func, "Bad MIR request=continued vid=%s\n", vid);
                                    tl_tpdaemon.tl_log( &tl_tpdaemon, 88, 4,
                                                        "func"   , TL_MSG_PARAM_STR  , func,
                                                        "Message", TL_MSG_PARAM_STR  , "Bad MIR request=continued",
                                                        "VID"    , TL_MSG_PARAM_STR  , vid, 
                                                        "TPVID"  , TL_MSG_PARAM_TPVID, vid);                                    
                                    
                            } else if (0 == strcasecmp( p, "CANCEL" )) {
                                    
                                    /* cancel the request without operator intervention */
                                    tplogit(func, "Bad MIR config=CANCEL vid=%s\n", vid );
                                    tl_tpdaemon.tl_log( &tl_tpdaemon, 85, 3,
                                                        "func"   , TL_MSG_PARAM_STR  , func,
                                                        "Message", TL_MSG_PARAM_STR  , "Bad MIR config=CANCEL",
                                                        "TPVID"  , TL_MSG_PARAM_TPVID, vid);
                                    tplogit(func, "Bad MIR request=canceled vid=%s\n", vid);
                                    tl_tpdaemon.tl_log( &tl_tpdaemon, 87, 5,
                                                        "func"   , TL_MSG_PARAM_STR  , func,
                                                        "Message", TL_MSG_PARAM_STR  , "Bad MIR request=canceled",
                                                        "JobID"  , TL_MSG_PARAM_INT  , jid,
                                                        "VID"    , TL_MSG_PARAM_STR  , vid, 
                                                        "TPVID"  , TL_MSG_PARAM_TPVID, vid);                                    
                                    cleanup();
                                    sendrep (rpfd, TAPERC, ETBADMIR);
                                    exit(0);                                    

                            } else {
                                    
                                    /* unknown option, regard as CANCEL */
                                    tplogit(func, "Bad MIR config=%s(unknown) vid=%s\n", p, vid );
                                    tl_tpdaemon.tl_log( &tl_tpdaemon, 85, 4,
                                                        "func"   , TL_MSG_PARAM_STR  , func,
                                                        "Message", TL_MSG_PARAM_STR  , "Bad MIR config=unknown",
                                                        "Option" , TL_MSG_PARAM_STR  , p, 
                                                        "TPVID"  , TL_MSG_PARAM_TPVID, vid);
                                    tplogit(func, "Bad MIR request=canceled vid=%s\n", vid);
                                    tl_tpdaemon.tl_log( &tl_tpdaemon, 87, 5,
                                                        "func"   , TL_MSG_PARAM_STR  , func,
                                                        "Message", TL_MSG_PARAM_STR  , "Bad MIR request=canceled",
                                                        "JobID"  , TL_MSG_PARAM_INT  , jid,
                                                        "VID"    , TL_MSG_PARAM_STR  , vid, 
                                                        "TPVID"  , TL_MSG_PARAM_TPVID, vid);                                    
                                    cleanup();
                                    sendrep (rpfd, TAPERC, ETBADMIR);
                                    exit(0);                                    
                            }
                            
                    } else {

                            tplogit(func, "Bad MIR config=unspecified vid=%s\n", vid );
                            tl_tpdaemon.tl_log( &tl_tpdaemon, 85, 3,
                                                "func"   , TL_MSG_PARAM_STR  , func,
                                                "Message", TL_MSG_PARAM_STR  , "Bad MIR config=unspecified",
                                                "TPVID"  , TL_MSG_PARAM_TPVID, vid);
                            
                            tplogit(func, "Bad MIR request=canceled vid=%s\n", vid);
                            tl_tpdaemon.tl_log( &tl_tpdaemon, 87, 5,
                                                "func"   , TL_MSG_PARAM_STR  , func,
                                                "Message", TL_MSG_PARAM_STR  , "Bad MIR request=canceled",
                                                "JobID"  , TL_MSG_PARAM_INT,   jid,
                                                "VID"    , TL_MSG_PARAM_STR  , vid, 
                                                "TPVID"  , TL_MSG_PARAM_TPVID, vid);
                            cleanup();
                            sendrep (rpfd, TAPERC, ETBADMIR);
                            exit(0);                 
                    } 
                    
            } else {
                    
                    /* MIR is ok */
                    tplogit(func, "MIR is valid\n");
                    tl_tpdaemon.tl_log( &tl_tpdaemon, 111, 3,
                                        "func"   , TL_MSG_PARAM_STR  , func,
                                        "Message", TL_MSG_PARAM_STR  , "MIR is valid",
                                        "TPVID"  , TL_MSG_PARAM_TPVID, vid );
            }
    }

	tapeacct (TPMOUNTED, uid, gid, jid, dgn, drive, vid, 0, 0);
	if ((c = Ctape_updvsn (uid, gid, jid, ux, vid, vsn, 0, lblcode, mode)))
		goto reply;
	(void) vmgr_seterrbuf (errbuf, sizeof(errbuf));
	errbuf[0] = '\0';
	if (vmgr_tpmounted (vid, mode, jid) && serrno != ENOENT) {
		if (*errbuf) {
			usrmsg (func, "%s", errbuf);
                        tl_tpdaemon.tl_log( &tl_tpdaemon, 103, 5,
                                            "func"   , TL_MSG_PARAM_STR  , func,
                                            "Message", TL_MSG_PARAM_STR  , errbuf,
                                            "JobID"  , TL_MSG_PARAM_INT  , jid,
                                            "vid"    , TL_MSG_PARAM_STR  , vid,
                                            "TPVID"  , TL_MSG_PARAM_TPVID, vid );                       
                }
		usrmsg (func, "vmgr_tpmounted returned %s\n", sstrerror (serrno));
                tl_tpdaemon.tl_log( &tl_tpdaemon, 103, 6,
                                    "func"   , TL_MSG_PARAM_STR  , func,
                                    "Message", TL_MSG_PARAM_STR  , "vmgr_tpmounted returned",
                                    "Error"  , TL_MSG_PARAM_STR  , sstrerror (serrno),
                                    "JobID"  , TL_MSG_PARAM_INT  , jid,
                                    "vid"    , TL_MSG_PARAM_STR  , vid,
                                    "TPVID"  , TL_MSG_PARAM_TPVID, vid );
		c = -1;
		goto reply;
	}
mounted:
	vdqm_status = VDQM_VOL_MOUNT;
	tplogit (func, "calling vdqm_UnitStatus(VDQM_VOL_MOUNT)\n");
        tl_tpdaemon.tl_log( &tl_tpdaemon, 111, 3,
                            "func"   , TL_MSG_PARAM_STR  , func,
                            "Message", TL_MSG_PARAM_STR  , "calling vdqm_UnitStatus(VDQM_VOL_MOUNT)",
                            "TPVID"  , TL_MSG_PARAM_TPVID, vid );
	while ((vdqm_rc = vdqm_UnitStatus (NULL, vid, dgn, NULL, drive,
		&vdqm_status, NULL, jid)) &&
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

	/* do the prelabel if flag is set */

	if (prelabel >= 0) {
		if ((c = rwndtape (tapefd, path))) goto reply;
		close (tapefd);
		if ((tapefd = open (path, O_WRONLY)) < 0) {
			usrmsg (func, TP042, path, "reopen", strerror(errno));
                        tl_tpdaemon.tl_log( &tl_tpdaemon, 42, 7,
                                            "func"   , TL_MSG_PARAM_STR  , func,
                                            "path"   , TL_MSG_PARAM_STR  , path,
                                            "Message", TL_MSG_PARAM_STR  , "reopen",
                                            "Error"  , TL_MSG_PARAM_STR  , strerror(errno),
                                            "JobID"  , TL_MSG_PARAM_INT  , jid,
                                            "Drive"  , TL_MSG_PARAM_STR  , drive, 
                                            "TPVID"  , TL_MSG_PARAM_TPVID, vid);
			c = errno;
			goto reply;
		}
		if (lblcode != NL) {
			buildvollbl (vol1, vsn, lblcode, name);
			if (lblcode == SL) asc2ebc (vol1, 80);
			if ((c = writelbl (tapefd, path, vol1)) < 0) goto reply;
			if (prelabel > 0) {
				int blksize;
				if (strcmp (devtype, "SD3"))
					blksize = 32760;
				else
					blksize = 262144;
				buildhdrlbl(hdr1, hdr2,
					"PRELABEL", vsn, 1, 1, 0,
					"U", blksize, 0, den, lblcode);
				if (lblcode == SL) asc2ebc (hdr1, 80);
				if ((c = writelbl (tapefd, path, hdr1)) < 0)
					goto reply;
				if (prelabel > 1) {
					if (lblcode == SL) asc2ebc (hdr2, 80);
					if ((c = writelbl (tapefd, path, hdr2)) < 0)
						goto reply;
				}
			}
		}
		if  ((c = wrttpmrk (tapefd, path, 1, 0)) < 0) goto reply;
		if (devinfo->eoitpmrks == 2 || Tflag)
			if  ((c = wrttpmrk (tapefd, path, 1, 0)) < 0) goto reply;
		if (strcmp (devtype, "SD3") == 0 && ! Tflag)	/* flush buffer */
			if  ((c = wrttpmrk (tapefd, path, 0, 0)) < 0) goto reply;
		goto reply;
	}

	if (c == 0) {
		sbp = repbuf;
		marshall_STRING (sbp, devtype);
		marshall_WORD (sbp, den);
		marshall_WORD (sbp, lblcode);
		sendrep (rpfd, MSG_DATA, sbp - repbuf, repbuf);
	}
reply:
	if (c < 0) c = serrno;
	if (c) {
		cleanup();
	} else {
		close (tapefd);
#if defined(CDK)
		if (*loader == 'a')
			wait4acsfinalresp();
#endif
		if (*loader == 's')
			closesmc ();
	}
	sendrep (rpfd, TAPERC, c);

        tl_tpdaemon.tl_log( &tl_tpdaemon, 73, 3,
                            "func" , TL_MSG_PARAM_STR  , func,
                            "vid"  , TL_MSG_PARAM_STR  , vid,
                            "TPVID", TL_MSG_PARAM_TPVID, vid );

        /* get the time for unmount */
        TEndMount = (int)time(NULL); 
        TMount = ((time_t)TEndMount - (time_t)TStartMount);
        
        tl_tpdaemon.tl_log( &tl_tpdaemon, 91, 4,
                            "func"     , TL_MSG_PARAM_STR  , func,
                            "mounttime", TL_MSG_PARAM_INT  , TMount,
                            "vid"      , TL_MSG_PARAM_STR  , vid,
                            "TPVID"    , TL_MSG_PARAM_TPVID, vid );

        if (!c) {
                /* tl_exit has been called in clenaup() already,
                   double free leads to crash */
                tl_tpdaemon.tl_exit( &tl_tpdaemon, 0 );        
        }
	exit (0);
}

int Ctape_updvsn(uid_t uid,
                 gid_t gid,
                 int jid,
                 int ux,
                 char *vid,
                 char *vsn,
                 int tobemounted,
                 int lblcode,
                 int mode)
{
	int c;
	int msglen;
	char *q;
	char *sbp;
	char sendbuf[REQBUFSZ];

	/* Build request header */

	sbp = sendbuf;
	marshall_LONG (sbp, TPMAGIC);
	marshall_LONG (sbp, UPDVSN);
	q = sbp;        /* save pointer. The next field will be updated */
	msglen = 3 * LONGSIZE;
	marshall_LONG (sbp, msglen);

	/* Build request body */
 
	marshall_LONG (sbp, uid);
	marshall_LONG (sbp, gid);
	marshall_LONG (sbp, jid);
	marshall_WORD (sbp, ux);
	marshall_STRING (sbp, vid);
	marshall_STRING (sbp, vsn);
	marshall_WORD (sbp, tobemounted);
	marshall_WORD (sbp, lblcode);
	marshall_WORD (sbp, mode);

	msglen = sbp - sendbuf;
	marshall_LONG (q, msglen);      /* update length field */
 
	c = send2tpd (NULL, sendbuf, msglen, NULL, 0);
	if (c < 0) {
		usrmsg (func, "%s", errbuf);
                tl_tpdaemon.tl_log( &tl_tpdaemon, 103, 5,
                                    "func",    TL_MSG_PARAM_STR  , func,
                                    "Message", TL_MSG_PARAM_STR  , errbuf,
                                    "JobID"  , TL_MSG_PARAM_INT  , jid,
                                    "vid"    , TL_MSG_PARAM_STR  , vid,
                                    "TPVID"  , TL_MSG_PARAM_TPVID, vid );
        }
	return (c);
}

static void cleanup()
{
	int flags;
	int msglen;
	char *q;
	char repbuf[1];
	char *sbp;
	char sendbuf[REQBUFSZ];

	tplogit (func, "cleanup started\n");
        tl_tpdaemon.tl_log( &tl_tpdaemon, 104, 2,
                            "func",    TL_MSG_PARAM_STR, func,
                            "Message", TL_MSG_PARAM_STR, "cleanup started" );
	if (tapefd >= 0)
		close (tapefd);
#if defined(CDK)
	if (*loader == 'a')
		wait4acsfinalresp();
#endif
	if ((*loader == 's') && (do_cleanup_after_mount == 0)) {
                do_cleanup_after_mount = 1;
		closesmc();
                /* 
                   check if the SIGINT hit us in a mount;
                   in that case we need to do the cleanup
                   after the mount has finished in order 
                   to prevent the dismount to overtake the 
                   mount 
                */                
		if ( mount_ongoing == 1 ) {
                        tplogit (func, "Cleanup triggered in a pending mount. Cleanup deferred.\n");
                        tl_tpdaemon.tl_log( &tl_tpdaemon, 103, 5,
                                            "func",    TL_MSG_PARAM_STR, func,
                                            "Message", TL_MSG_PARAM_STR, "Cleanup triggered in a pending mount. Cleanup deferred.",
                                            "JobID"  , TL_MSG_PARAM_INT  , jid,
                                            "vid"    , TL_MSG_PARAM_STR  , vid,
                                            "TPVID"  , TL_MSG_PARAM_TPVID, vid );
                        return;
                }
        }


	/* must unload and deassign */

	flags = TPRLS_KEEP_RSV|TPRLS_UNLOAD|TPRLS_NOWAIT;
 
	/* Build request header */

	sbp = sendbuf;
	marshall_LONG (sbp, TPMAGIC);
	marshall_LONG (sbp, TPRLS);
	q = sbp;        /* save pointer. The next field will be updated */
	msglen = 3 * LONGSIZE;
	marshall_LONG (sbp, msglen);
 
	/* Build request body */

	marshall_LONG (sbp, uid);
	marshall_LONG (sbp, gid);
	marshall_LONG (sbp, jid);
	marshall_WORD (sbp, flags);
	marshall_STRING (sbp, path);

	msglen = sbp - sendbuf;
	marshall_LONG (q, msglen);	/* update length field */

	(void) send2tpd (NULL, sendbuf, msglen, repbuf, sizeof(repbuf));

        /* called before each exit() */
        tl_tpdaemon.tl_exit( &tl_tpdaemon, 0 );        
}

static void configdown(char *drive)
{
	char msg[OPRMSGSZ];
	snprintf (msg, sizeof(msg), TP033, drive, hostname); /* ops msg */
	msg[sizeof(msg) - 1] = '\0';
	usrmsg ("mounttape", "%s\n", msg);
        tl_tpdaemon.tl_log( &tl_tpdaemon, 33, 4,
                            "func",     TL_MSG_PARAM_STR, "mounttape",
                            "Message",  TL_MSG_PARAM_STR, msg,
                            "Drive",    TL_MSG_PARAM_STR, drive, 
                            "Hostname", TL_MSG_PARAM_STR, hostname );

	(void) Ctape_config (drive, CONF_DOWN, TPCD_SYS);
}

static void mountkilled()
{
	cleanup();
	if ( do_cleanup_after_mount == 1 ) return;
	exit (2);
}

static int rbtdmntchk(int *c,
               char *drive,
               unsigned int *demountforce)
{
	switch (*c) {
	case 0:
		return (0);
	case RBT_NORETRY:
	case RBT_SLOW_RETRY:
		*c = EIO;
		return (-1);
	case RBT_FAST_RETRY:
                tplogit (func, "RBT_FAST_RETRY\n");
                tl_tpdaemon.tl_log( &tl_tpdaemon, 111, 2,
                                    "func",    TL_MSG_PARAM_STR, func,
                                    "Message", TL_MSG_PARAM_STR, "RBT_FAST_RETRY");        

		sleep(RBTFASTRI);
                /* retry after timeout */
                return (1);
	case RBT_DMNT_FORCE:
		if (*demountforce) {
			*c = EIO;
			return (-1);
		} else {
			*demountforce = 1;
			return (1);
		}
	case RBT_CONF_DRV_DN:
		configdown (drive);
		*c = EIO;
		return (-1);
	case RBT_UNLD_DMNT:
		*c = EIO;
		return (2);	/* may be unload and retry */
	default:
		return (-1);	/* unrecoverable error */
	}
}

static int rbtmountchk(int *c,
                char *drive,
                char *vid,
                char *dvn,
                char *loader)
{
	unsigned int demountforce;
	int n;
	int tapefd;
        int vsnretry=0;
    
	switch (*c) {
	case 0:
		return (0);
	case RBT_NORETRY:
		*c = EIO;
		return (-1);
	case RBT_SLOW_RETRY:
		*c = ETVBSY;	/* volume in use */
		return (-1);
	case RBT_FAST_RETRY:
                tplogit (func, "RBT_FAST_RETRY\n");
                tl_tpdaemon.tl_log( &tl_tpdaemon, 111, 2,
                                    "func",    TL_MSG_PARAM_STR, func,
                                    "Message", TL_MSG_PARAM_STR, "RBT_FAST_RETRY");        

		sleep(RBTFASTRI);
                return (1);  
	case RBT_DMNT_FORCE:
		if ((tapefd = open (dvn, O_RDONLY|O_NDELAY)) < 0) {
			*c = errno;
			return (-1);
		}
		if (chkdriveready (tapefd) > 0) {
			if ((*c = unldtape (tapefd, dvn)) < 0) {
				close (tapefd);
				return (-1);
			}
		}
		close (tapefd);
		demountforce = 1;
		do {
                        vsnretry++;
			*c = rbtdemount (vid, drive, dvn, loader, demountforce, vsnretry);
			if ((n = rbtdmntchk (c, drive, &demountforce)) < 0)
				return (-1);
		} while (n == 1);
		if (n == 2)	/* tape not unloaded */
			return (-1);
		return (1);	/* retry */
	case RBT_CONF_DRV_DN:
		configdown (drive);
		*c = EIO;
		return (-1);
	default:
		return (-1);	/* unrecoverable error */
	}
}


/*
** Repair a bad MIR (corrupted tape directory) by issuing
** a 'SPACE to EOD' and a 'RWND', blocking. 
*/
static int repairbadmir( int tapefd, char* path ) {

	char func[16];
	struct mtop mtop;
        
	ENTRY (repairbadmir);

        /* SPACE to EOD */
        tplogit( func, "Issue a SPACE to EOD\n" );
        tl_tpdaemon.tl_log( &tl_tpdaemon, 110, 2,
                            "func",     TL_MSG_PARAM_STR, func,
                            "Message",  TL_MSG_PARAM_STR, "Issue a SPACE to EOD" );        
        memset( &mtop, 0, sizeof(mtop) );
        mtop.mt_op      = MTSETDRVBUFFER;	
        mtop.mt_count   = MT_ST_CLEARBOOLEANS;
        mtop.mt_count  |= MT_ST_FAST_MTEOM;      /* set this to skip to EOD, 
                                                    otherwise MTEOM will only 
                                                    skip 32767 files */     
        if (ioctl( tapefd, MTIOCTOP, &mtop ) < 0) {
                
                tplogit( func, "Error during MT_ST_FAST_MTEOM\n" );
                tl_tpdaemon.tl_log( &tl_tpdaemon, 103, 2,
                                    "func",     TL_MSG_PARAM_STR, func,
                                    "Message",  TL_MSG_PARAM_STR, "Error during MT_ST_FAST_MTEOM" );
                serrno = rpttperror (func, tapefd, path, "ioctl"); 
		RETURN (-1);
        }

        memset( &mtop, 0, sizeof(mtop) );
	mtop.mt_op    = MTEOM;	
	mtop.mt_count = 1;
	if (ioctl( tapefd, MTIOCTOP, &mtop ) < 0) {
		
                tplogit( func, "Error during MTEOM\n" );
                tl_tpdaemon.tl_log( &tl_tpdaemon, 103, 2,
                                    "func",     TL_MSG_PARAM_STR, func,
                                    "Message",  TL_MSG_PARAM_STR, "Error during MTEOM" );
                serrno = rpttperror (func, tapefd, path, "ioctl"); 
		RETURN (-1);
	}
        

        /* REWIND */
        tplogit( func, "Issue a REWIND\n" );
        tl_tpdaemon.tl_log( &tl_tpdaemon, 110, 2,
                            "func",     TL_MSG_PARAM_STR, func,
                            "Message",  TL_MSG_PARAM_STR, "Issue a REWIND" );        
        if (rwndtape( tapefd, path ) < 0) {
                
                tplogit( func, "Error during REWIND\n" );
                tl_tpdaemon.tl_log( &tl_tpdaemon, 103, 2,
                                    "func",     TL_MSG_PARAM_STR, func,
                                    "Message",  TL_MSG_PARAM_STR, "Error during REWIND" );
                RETURN (-1);
        } 
        
        RETURN (0);
}
