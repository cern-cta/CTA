/*
 * Copyright (C) 1999-2004 by CERN IT
 * All rights reserved
 */

/*
 * rtcpd_SelfMonitor.c - RTCOPY server self monitor
 */

#include <stdlib.h>
#include <time.h>
#if defined(_WIN32)
#include <winsock2.h>
#else  /* _WIN32 */
#include <sys/time.h>
#endif /* _WIN32 */

#include <errno.h>
#include <stdarg.h>
#include <stdio.h>
#include <string.h>
#include <signal.h>

#include <Castor_limits.h>
#include <Cglobals.h>
#include <log.h>
#include <osdep.h>
#include <net.h>
#include <Cthread_api.h>
#include <common.h>
#include <vdqm_api.h>
#include <Cuuid.h>
#include <rtcp_constants.h>
#include <rtcp.h>
#include <rtcp_server.h>
#include <Ctape_api.h>
#include <serrno.h>

extern processing_status_t proc_stat;

/*
 * Processing status check thread. The operations here
 * are inherently thread unsafe but this is not a problem:
 * as long as the content of the proc_stat is dynamic it
 * means that everything is well (processing is progressing).
 * When proc_stat is static, i.e. it doesn't change between two
 * checks, it means that something may have hung and we can
 * safely check the content of the structure.
 */
void *rtcpd_MonitorThread(void *arg) {
    processing_status_t ps_check;
    char proc_status_strings[][20] = PROC_STATUS_STRINGS;
    int i,nb_diskIO,current_activity, nbKB,time_hung,max_time, nbKillTries;

    /*
     * Just in case we don't want to monitor all threads in pool...
     */
    if ( arg != NULL )
        nb_diskIO = *(int *)arg;
    else
        nb_diskIO = proc_stat.nb_diskIO;

    ps_check.diskIOstatus = 
        (diskIOstatus_t *)calloc(nb_diskIO,sizeof(diskIOstatus_t));
    if ( ps_check.diskIOstatus == NULL ) {
        rtcp_log(LOG_ERR,"rtcpd_MonitorThread() calloc(%d,%d): %s\n",
            nb_diskIO,sizeof(diskIOstatus_t),sstrerror(errno));
        return(NULL);
    }

    ps_check.tapeIOstatus = proc_stat.tapeIOstatus;
    memcpy(ps_check.diskIOstatus,proc_stat.diskIOstatus,
        nb_diskIO*sizeof(diskIOstatus_t));
    ps_check.timestamp = time(NULL);
    nbKillTries = 0;

    for (;;) {
        sleep(30);
        if ( nbKillTries * 30 > RTCP_KILL_TIMEOUT ) {
            rtcp_log(LOG_ERR,"rtcpd_MonitorThread() exit from non-killable process after %d attempts\n",nbKillTries);
            exit(1);
        }
        if ( memcmp(&ps_check.tapeIOstatus,&proc_stat.tapeIOstatus,
             sizeof(tapeIOstatus_t)) == 0 ) {
            if ( memcmp(ps_check.diskIOstatus,proc_stat.diskIOstatus,
                nb_diskIO*sizeof(diskIOstatus_t)) == 0 ) {
                current_activity = ps_check.tapeIOstatus.current_activity;
                nbKB = (int)(ps_check.tapeIOstatus.nbbytes/1024);
                time_hung = (int)(time(NULL) - ps_check.timestamp);
                if ( current_activity == RTCP_PS_FINISHED ) {
                    rtcp_log(LOG_DEBUG,"rtcpd_MonitorThread() processing finished\n");
                    return(NULL);
                }
                rtcp_log(LOG_DEBUG,
                    "rtcpd_MonitorThread() proc hung since %d seconds\n",
                    time_hung);
                rtcp_log(LOG_DEBUG,"rtcpd_MonitorThread() tapeIO status 0x%x (%s), %ld KB\n",
                    current_activity,
                    proc_status_strings[current_activity],nbKB);
                switch (current_activity) {
                case RTCP_PS_MOUNT:
                    /*
                     * Check if mount time has exceeded
                     */
                    PS_LIMIT(RTCOPYD,MOUNT_TIME,max_time);
                    if ( time_hung > max_time ) {
                        rtcp_log(LOG_ERR,"rtcpd_MonitorThread() mount time exceeded\n");
                        nbKillTries++;
                        rtcpd_SetProcError(RTCP_FAILED);
                    }
                    break;
                case RTCP_PS_POSITION:
                    /*
                     * Check if position time has exceeded
                     */
                    PS_LIMIT(RTCOPYD,POSITION_TIME,max_time);
                    if ( time_hung > max_time ) {
                        rtcp_log(LOG_ERR,"rtcpd_MonitorThread() position time exceeded\n");
                        nbKillTries++;
                        rtcpd_SetProcError(RTCP_FAILED);
                    }
                    break;
                case RTCP_PS_OPEN:
                    /*
                     * Check if tape file open time has exceeded
                     */
                    PS_LIMIT(RTCOPYD,OPEN_TIME,max_time);
                    if ( time_hung > max_time ) {
                        rtcp_log(LOG_ERR,"rtcpd_MonitorThread() tape open time exceeded\n");
                        nbKillTries++;
                        rtcpd_SetProcError(RTCP_FAILED);
                    }
                    break;
                case RTCP_PS_READ:
                    /*
                     * Check if tape file read time has exceeded
                     */
                    PS_LIMIT(RTCOPYD,READ_TIME,max_time);
                    if ( time_hung > max_time ) {
                        rtcp_log(LOG_ERR,"rtcpd_MonitorThread() tape read time exceeded\n");
                        nbKillTries++;
                        rtcpd_SetProcError(RTCP_FAILED);
                    }
                    break;
                case RTCP_PS_WRITE:
                    /*
                     * Check if tape file write time has exceeded
                     */
                    PS_LIMIT(RTCOPYD,WRITE_TIME,max_time);
                    if ( time_hung > max_time ) {
                        rtcp_log(LOG_ERR,"rtcpd_MonitorThread() tape write time exceeded\n");
                        nbKillTries++;
                        rtcpd_SetProcError(RTCP_FAILED);
                    }
                    break;
                case RTCP_PS_CLOSERR:
                case RTCP_PS_CLOSE:
                    /*
                     * Check if tape file close time has exceeded
                     */
                    PS_LIMIT(RTCOPYD,CLOSE_TIME,max_time);
                    if ( time_hung > max_time ) {
                        rtcp_log(LOG_ERR,"rtcpd_MonitorThread() tape close time exceeded\n");
                        nbKillTries++;
                        rtcpd_SetProcError(RTCP_FAILED);
                    }
                    break;
                case RTCP_PS_STAGEUPDC:
                    /*
                     * Check if we're blocked too long in a stageupdc() call
                     */
                    PS_LIMIT(RTCOPYD,STAGEUPDC_TIME,max_time);
                    if ( time_hung > max_time ) {
                        rtcp_log(LOG_ERR,"rtcpd_MonitorThread() stageupdc() time exceeded\n");
                        nbKillTries++;
                        rtcpd_SetProcError(RTCP_FAILED);
                    }
                    break;
                default:
                    break;
                }
                for (i=0;i<nb_diskIO; i++) {
                    current_activity = ps_check.diskIOstatus[i].current_activity;
                    nbKB = (int)(ps_check.diskIOstatus[i].nbbytes/1024);
                    rtcp_log(LOG_DEBUG,"rtcpd_MonitorThread() diskIO thread %d, status 0x%x (%s), %ld KB\n",
                        i,current_activity,
                        proc_status_strings[current_activity],nbKB);
                    switch (current_activity) {
                    case RTCP_PS_OPEN:
                       /*
                        * Check if disk file open time has exceeded
                        */
                        PS_LIMIT(RTCOPYD,OPEN_TIME,max_time);
                        if ( time_hung > max_time ) {
                            rtcp_log(LOG_ERR,"rtcpd_MonitorThread() disk open time exceeded\n");
                            nbKillTries++;
                            rtcpd_SetProcError(RTCP_FAILED);
                        }
                        break;
                    case RTCP_PS_READ:
                       /*
                        * Check if disk file read time has exceeded
                        */
                        PS_LIMIT(RTCOPYD,READ_TIME,max_time);
                        if ( time_hung > max_time ) {
                            rtcp_log(LOG_ERR,"rtcpd_MonitorThread() disk read time exceeded\n");
                            nbKillTries++;
                            rtcpd_SetProcError(RTCP_FAILED);
                        }
                        break;
                    case RTCP_PS_WRITE:
                       /*
                        * Check if disk file write time has exceeded
                        */
                        PS_LIMIT(RTCOPYD,WRITE_TIME,max_time);
                        if ( time_hung > max_time ) {
                            rtcp_log(LOG_ERR,"rtcpd_MonitorThread() disk write time exceeded\n");
                            nbKillTries++;
                            rtcpd_SetProcError(RTCP_FAILED);
                        }
                        break;
                    case RTCP_PS_CLOSE:
                       /*
                        * Check if disk file close time has exceeded
                        */
                        PS_LIMIT(RTCOPYD,CLOSE_TIME,max_time);
                        if ( time_hung > max_time ) {
                            rtcp_log(LOG_ERR,"rtcpd_MonitorThread() disk close time exceeded\n");
                            nbKillTries++;
                            rtcpd_SetProcError(RTCP_FAILED);
                        }
                        break;
                    case RTCP_PS_STAGEUPDC:
                        /*
                         * Check if we're blocked too long in a stageupdc() call
                         */
                        PS_LIMIT(RTCOPYD,STAGEUPDC_TIME,max_time);
                        if ( time_hung > max_time ) {
                            rtcp_log(LOG_ERR,"rtcpd_MonitorThread() stageupdc() time exceeded\n");
                            nbKillTries++;
                            rtcpd_SetProcError(RTCP_FAILED);
                        }
                        break;
                    default:
                        break;
                    }

                }
                continue;
            }
        }
        ps_check.tapeIOstatus = proc_stat.tapeIOstatus;
        memcpy(ps_check.diskIOstatus,proc_stat.diskIOstatus,
            nb_diskIO*sizeof(diskIOstatus_t));
        ps_check.timestamp = time(NULL);
    }
}

int rtcpd_StartMonitor(int pool_size) {
    static int _pool_size;
    char *p;

    if ( ((p = getenv("RTCOPYD_SELF_MONITOR")) == NULL) &&
         ((p = getconfent("RTCOPYD","SELF_MONITOR",0)) == NULL) ) return(0);
    if ( strcmp(p,"NO") == 0 ) return(0);
    if ( pool_size > 0 ) _pool_size = pool_size;
    return(Cthread_create_detached(rtcpd_MonitorThread,(void *)&_pool_size));
} 

