/*
 * Copyright (C) 1999 by CERN IT-PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: rtcpd_SelfMonitor.c,v $ $Revision: 1.1 $ $Date: 1999/11/29 11:22:02 $ CERN IT-PDP/DM Olof Barring";
#endif /* not lint */

/*
 * rtcpd_SelfMonitor.c - RTCOPY server self monitor
 */


#if defined(_WIN32)
#include <winsock2.h>
#include <time.h>
#endif /* _WIN32 */

#include <stdlib.h>
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
#include <vdqm_api.h>
#include <rtcp_constants.h>
#include <rtcp.h>
#include <rtcp_server.h>
#include <Ctape_api.h>
#include <serrno.h>

extern char *getconfent(const char *, const char *, int);

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
    int i,nb_diskIO,current_activity, nbKB,time_hung,max_time;

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

    for (;;) {
        sleep(30);
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
    return(NULL);
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
