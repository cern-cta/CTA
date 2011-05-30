/*
 * Copyright (C) 1990-2003 by CERN/IT/PDP/DM
 * All rights reserved
 */

#include <stdlib.h>
#include <errno.h>
#include <fcntl.h>
#include <string.h>
#include <sys/types.h>
#include "Ctape.h"
#include "Ctape_api.h"
#include "sacct.h"
#include "serrno.h"
#include "net.h"
#include "vdqm_api.h"
#include <unistd.h>
#include "tplogger_api.h"
#if defined(linux)
#include <sys/ioctl.h>
#include <linux/mtio.h>
#endif
int jid;
int main(int	argc,
         char	**argv)
{
	int c;
	char *dgn;
	char *drive;
	char *dvn;
	char *dvrname;
	char func[16];
	int reason;
	int rpfd;
	int status;
	int tapefd;
	int vdqm_rc;
	int vdqm_status;
#if defined(linux)
        struct mtop mt_cmd;
        int st_buffer_writes = 0;
        int st_read_ahead = 0;
        int st_async_writes = 0;
        char *p;
        char *getconfent();
        int st_timeout;
        int st_long_timeout;
#endif
	ENTRY (confdrive);

  if (11 != argc) {
    printf("Wrong number of arguments\n");
    exit(-1);
  }
  

        p = getconfent ("TAPE", "TPLOGGER", 0);
        if (p && (0 == strcasecmp(p, "SYSLOG"))) {
                tl_init_handle( &tl_tpdaemon, "syslog" ); 
        } else {
                tl_init_handle( &tl_tpdaemon, "dlf" );  
        }
        tl_tpdaemon.tl_init( &tl_tpdaemon, 0 );

	drive = argv[1];
	dvn = argv[2];
	rpfd = atoi (argv[3]);
	jid = atoi (argv[6]);
	dgn = argv[7];
	dvrname = argv[8];
	status = atoi (argv[9]);
	reason = atoi (argv[10]);

	c = 0;
	if (status == CONF_UP) {
		tapefd = open (dvn, O_RDONLY|O_NDELAY);
		if (tapefd < 0 &&
		    (errno == ENOENT || errno == ENXIO || errno == EBUSY ||
		     errno == ENODEV ||
		    (strcmp (dvrname, "Atape") == 0 && errno == EIO))) {
			c = errno;
		} else {
			if (tapefd >= 0) {
#if defined(linux)
                                /* set st parameters (moved here from kernel patches) */
                                mt_cmd.mt_op = MTSETDRVBUFFER;

                                /* ST_BUFFER_WRITES */
                                st_buffer_writes = 0;
                                if ((p = getconfent ("TAPE", "ST_BUFFER_WRITES", 0))) {
                                        st_buffer_writes = (int)atoi(p);
                                        if ((0 != st_buffer_writes) && (1 != st_buffer_writes)) {
                                                st_buffer_writes = 0;   
                                        }
                                }
                                if (!st_buffer_writes) {
                                        tplogit (func, "Clear ST_BUFFER_WRITES (0)\n");
                                        tl_tpdaemon.tl_log( &tl_tpdaemon, 110, 2,
                                                            "func",    TL_MSG_PARAM_STR, func,
                                                            "Message", TL_MSG_PARAM_STR, "Clear ST_BUFFER_WRITES (0)" );
                                        mt_cmd.mt_count = MT_ST_CLEARBOOLEANS |
                                                MT_ST_BUFFER_WRITES;
                                } else {
                                        tplogit (func, "Set ST_BUFFER_WRITES (1)\n");
                                        tl_tpdaemon.tl_log( &tl_tpdaemon, 110, 2,
                                                            "func",    TL_MSG_PARAM_STR, func,
                                                            "Message", TL_MSG_PARAM_STR, "Set ST_BUFFER_WRITES (1)" );
                                        mt_cmd.mt_count = MT_ST_SETBOOLEANS |
                                                MT_ST_BUFFER_WRITES;
                                }
                                if (ioctl(tapefd, MTIOCTOP, &mt_cmd) < 0) {
                                        tplogit (func, TP002, "ioctl (MT_ST_BUFFER_WRITES)", strerror(errno));
                                        tl_tpdaemon.tl_log( &tl_tpdaemon, 2, 3,
                                                            "func",    TL_MSG_PARAM_STR, func,
                                                            "Message", TL_MSG_PARAM_STR, "ioctl (MT_ST_BUFFER_WRITES)",
                                                            "Value",   TL_MSG_PARAM_STR, strerror(errno) );
                                        c = errno;
                                }

                                /* ST_ASYNC_WRITES */
                                st_async_writes = 0;
                                if ((p = getconfent ("TAPE", "ST_ASYNC_WRITES", 0))) {
                                        st_async_writes = (int)atoi(p);
                                        if ((0 != st_async_writes) && (1 != st_async_writes)) {
                                                st_async_writes = 0;   
                                        }
                                }
                                if (!st_async_writes) {
                                        tplogit (func, "Clear ST_ASYNC_WRITES (0)\n");
                                        tl_tpdaemon.tl_log( &tl_tpdaemon, 110, 2,
                                                            "func",    TL_MSG_PARAM_STR, func,
                                                            "Message", TL_MSG_PARAM_STR, "Clear ST_ASYNC_WRITES (0)" );
                                        mt_cmd.mt_count = MT_ST_CLEARBOOLEANS |
                                                MT_ST_ASYNC_WRITES;
                                } else {
                                        tplogit (func, "Set ST_ASYNC_WRITES (1)\n");
                                        tl_tpdaemon.tl_log( &tl_tpdaemon, 110, 2,
                                                            "func",    TL_MSG_PARAM_STR, func,
                                                            "Message", TL_MSG_PARAM_STR, "Set ST_ASYNC_WRITES (1)" );
                                        mt_cmd.mt_count = MT_ST_SETBOOLEANS |
                                                MT_ST_ASYNC_WRITES;
                                }
                                if (ioctl(tapefd, MTIOCTOP, &mt_cmd) < 0) {
                                        tplogit (func, TP002, "ioctl (MT_ST_ASYNC_WRITES)", strerror(errno));
                                        tl_tpdaemon.tl_log( &tl_tpdaemon, 2, 3,
                                                            "func",    TL_MSG_PARAM_STR, func,
                                                            "Message", TL_MSG_PARAM_STR, "ioctl (MT_ST_ASYNC_WRITES)",
                                                            "Value",   TL_MSG_PARAM_STR, strerror(errno) );
                                        c = errno;
                                }

                                /* ST_READ_AHEAD */
                                st_read_ahead = 0;
                                if ((p = getconfent ("TAPE", "ST_READ_AHEAD", 0))) {
                                        st_read_ahead = (int)atoi(p);
                                        if ((0 != st_read_ahead) && (1 != st_read_ahead)) {
                                                st_read_ahead = 0;   
                                        }
                                }
                                if (!st_read_ahead) {
                                        tplogit (func, "Clear ST_READ_AHEAD (0)\n");
                                        tl_tpdaemon.tl_log( &tl_tpdaemon, 110, 2,
                                                            "func",    TL_MSG_PARAM_STR, func,
                                                            "Message", TL_MSG_PARAM_STR, "Clear ST_READ_AHEAD (0)" );
                                        mt_cmd.mt_count = MT_ST_CLEARBOOLEANS |
                                                MT_ST_READ_AHEAD;
                                } else {
                                        tplogit (func, "Set ST_READ_AHEAD (1)\n");
                                        tl_tpdaemon.tl_log( &tl_tpdaemon, 110, 2,
                                                            "func",    TL_MSG_PARAM_STR, func,
                                                            "Message", TL_MSG_PARAM_STR, "Set ST_READ_AHEAD (1)" );
                                        mt_cmd.mt_count = MT_ST_SETBOOLEANS |
                                                MT_ST_READ_AHEAD;
                                }
                                if (ioctl(tapefd, MTIOCTOP, &mt_cmd) < 0) {
                                        tplogit (func, TP002, "ioctl (MT_ST_READ_AHEAD)", strerror(errno));
                                        tl_tpdaemon.tl_log( &tl_tpdaemon, 2, 3,
                                                            "func",    TL_MSG_PARAM_STR, func,
                                                            "Message", TL_MSG_PARAM_STR, "ioctl (MT_ST_READ_AHEAD)",
                                                            "Value",   TL_MSG_PARAM_STR, strerror(errno) );
                                        c = errno;
                                }

                                /* ST_TIMEOUT */
                                st_timeout = 900;
                                if ((p = getconfent ("TAPE", "ST_TIMEOUT", 0))) {
                                        st_timeout = (int)atoi(p);
                                        if (st_timeout < 0) {
                                                st_timeout = 900;   
                                        }
                                }
                                tplogit (func, "Set ST_TIMEOUT to %d seconds.\n", st_timeout);
                                tl_tpdaemon.tl_log( &tl_tpdaemon, 110, 3,
                                                    "func",    TL_MSG_PARAM_STR, func,
                                                    "Message", TL_MSG_PARAM_STR, "Set ST_TIMEOUT",
                                                    "Value",   TL_MSG_PARAM_INT, st_timeout );

                                mt_cmd.mt_count = MT_ST_SET_TIMEOUT | st_timeout;
                                if (ioctl(tapefd, MTIOCTOP, &mt_cmd) < 0) {
                                        tplogit (func, TP002, "ioctl (ST_SET_TIMEOUT)", strerror(errno));
                                        tl_tpdaemon.tl_log( &tl_tpdaemon, 2, 3,
                                                            "func",    TL_MSG_PARAM_STR, func,
                                                            "Message", TL_MSG_PARAM_STR, "ioctl (ST_SET_TIMEOUT)",
                                                            "Value",   TL_MSG_PARAM_STR, strerror(errno) );
                                        c = errno;
                                }

                                /* ST_LONG_TIMEOUT */
                                st_long_timeout = 3600;
                                if ((p = getconfent ("TAPE", "ST_LONG_TIMEOUT", 0))) {
                                        st_long_timeout = (int)atoi(p);
                                        if (st_timeout < 0) {
                                                st_long_timeout = 3600;   
                                        }
                                }
                                tplogit (func, "Set ST_LONG_TIMEOUT to %d seconds.\n", st_long_timeout);
                                tl_tpdaemon.tl_log( &tl_tpdaemon, 110, 3,
                                                    "func",    TL_MSG_PARAM_STR, func,
                                                    "Message", TL_MSG_PARAM_STR, "Set ST_LONG_TIMEOUT",
                                                    "Value",   TL_MSG_PARAM_INT, st_long_timeout );

                                mt_cmd.mt_count = MT_ST_SET_LONG_TIMEOUT | st_long_timeout;
                                if (ioctl(tapefd, MTIOCTOP, &mt_cmd) < 0) {
                                        tplogit (func, TP002, "ioctl (ST_SET_LONG_TIMEOUT)", strerror(errno));
                                        tl_tpdaemon.tl_log( &tl_tpdaemon, 2, 3,
                                                            "func",    TL_MSG_PARAM_STR, func,
                                                            "Message", TL_MSG_PARAM_STR, "ioctl (ST_SET_LONG_TIMEOUT)",
                                                            "Value",   TL_MSG_PARAM_STR, strerror(errno) );
                                        c = errno;
                                }
#endif /* linux */
				close (tapefd);
			}
		}
		if (c == 0)
			tapeacct (TPCONFUP, 0, 0, jid, dgn, drive, "", 0, reason);
	} else {
		tapeacct (TPCONFDN, 0, 0, jid, dgn, drive, "", 0, reason);
	}
	if (c == 0) {
		vdqm_status = (status == CONF_UP) ? VDQM_UNIT_UP : VDQM_UNIT_DOWN;
		tplogit (func, "calling vdqm_UnitStatus\n");
                tl_tpdaemon.tl_log( &tl_tpdaemon, 110, 2,
                                    "func",    TL_MSG_PARAM_STR, func,
                                    "Message", TL_MSG_PARAM_STR, "calling vdqm_UnitStatus" );
		while ((vdqm_rc = vdqm_UnitStatus (NULL, NULL, dgn, NULL, drive,
			&vdqm_status, NULL, 0)) &&
			(serrno == SECOMERR || serrno == EVQHOLD))
				sleep (60);
		tplogit (func, "vdqm_UnitStatus returned %s\n",
			vdqm_rc ? sstrerror(serrno) : "ok");
                tl_tpdaemon.tl_log( &tl_tpdaemon, 110, 3,
                                    "func",    TL_MSG_PARAM_STR, func, 
                                    "Message", TL_MSG_PARAM_STR, "vdqm_UnitStatus returned",
                                    "Error",   TL_MSG_PARAM_STR, vdqm_rc ? sstrerror(serrno) : "ok");
	}
	if (rpfd >= 0)
		sendrep (rpfd, TAPERC, c);

        tl_tpdaemon.tl_exit( &tl_tpdaemon, 0 );        
	exit (c);
}
