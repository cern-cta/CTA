/*
 * Copyright (C) 1999 by CERN IT-PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)vdqm_AdmOps.c,v 1.1 2000/02/29 07:51:10  CERN IT-PDP/DM Olof Barring";
#endif /* not lint */

/*
 * vdqm_AdmOps.c - Special routines for administrative operations.
 */


#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <regex.h>
#include <stdlib.h>
#include <pwd.h>
#include <time.h>
#include <osdep.h>
#include <net.h>
#include <log.h>
#include <serrno.h>
#include <Castor_limits.h>
#include <Cpwd.h>
#include <vdqm_constants.h>
#include <vdqm.h>

int vdqm_SetDedicate(vdqm_drvrec_t *drv) {
    vdqmDrvReq_t *DrvReq;
    int rc; 
    char errstr[256];

    if ( drv == NULL ) {
        serrno = EINVAL;
        return(-1);
    }

    DrvReq = &drv->drv;
    log(LOG_DEBUG,"vdqm_SetDedicate() compile regexp %s\n",DrvReq->dedicate);
    rc = regcomp(&drv->expbuf, DrvReq->dedicate, REG_EXTENDED|REG_ICASE);
    if ( rc != 0 ) {
        rc = regerror(rc,&drv->expbuf,errstr,sizeof(errstr));
        log(LOG_ERR,"vdqm_SetDedicate() regcomp(%s): %s \n",
            DrvReq->dedicate,errstr);
        return(-1); 
    }
    return(0);
}

int vdqm_ResetDedicate(vdqm_drvrec_t *drv) {
    vdqmDrvReq_t *DrvReq;

    if ( drv == NULL ) {
        serrno = EINVAL;
        return(-1);
    }

    DrvReq = &drv->drv;

    if ( *DrvReq->dedicate != '\0' ) {
        (void)regfree(&drv->expbuf);
        *DrvReq->dedicate = '\0';
    }
    return(0);
}

int vdqm_DrvMatch(vdqm_volrec_t *vol, vdqm_drvrec_t *drv) {
    vdqmVolReq_t *VolReq;
    char keywords[][20] = VDQM_DEDICATE_PREFIX;
    char formats[][20] = VDQM_DEDICATE_FORMAT;
    char match_item[256], format[256];
    struct tm *current_tm, tmstruc;
    time_t clock;
    struct passwd *pw;
    int uid,gid,mode,age;
    char datestr[11], timestr[9], *name, *group, *vid, *host; 
    char empty[1] = "";

    if ( vol == NULL || drv == NULL ) {
        serrno = EINVAL;
        return(-1);
    }

    /*
     * If not dedicated, we match all
     */
    if ( *drv->drv.dedicate == '\0' ) return(1);
    (void)time(&clock);
#if defined(_REENTRANT) || defined(_THREAD_SAFE)
    (void)localtime_r(&clock,&tmstruc);
    current_tm = &tmstruc;
#else
    current_tm = localtime(&clock);
#endif /* _REENTRANT || _THREAD_SAFE */
    VolReq = &vol->vol;
    (void) strftime(datestr,sizeof(datestr),VDQM_DEDICATE_DATEFORM,current_tm);
    (void) strftime(timestr,sizeof(timestr),VDQM_DEDICATE_TIMEFORM,current_tm);
    age = (int)(clock - (time_t)VolReq->recvtime);
    pw = Cgetpwuid(VolReq->clientUID);
    uid = VolReq->clientUID;
    gid = VolReq->clientGID;
    if ( pw == NULL ) name = empty;
    else name = pw->pw_name;
    host = VolReq->client_host;
    vid = VolReq->volid;
    mode = VolReq->mode;

    FILL_MATCHSTR(match_item,format,formats);

    log(LOG_DEBUG,"vdqm_DrvMatch(): match %s with %s\n",match_item,drv->drv.dedicate);
    if ((regexec(&drv->expbuf, match_item, (size_t)0, NULL, 0)) == 0) {
        log(LOG_DEBUG,"vdqm_DrvMatch() matched!\n");
        return(1);
    } else return(0);
}
