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
#include <time.h>
#include <osdep.h>
#include <net.h>
#include <log.h>
#include <serrno.h>
#include <Castor_limits.h>
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
    char match_item[256];

    if ( vol == NULL || drv == NULL ) {
        serrno = EINVAL;
        return(-1);
    }

    /*
     * If not dedicated, we match all
     */
    if ( *drv->drv.dedicate == '\0' ) return(1);
    VolReq = &vol->vol;
    sprintf(match_item,
           "clientUID=%d,clientGID=%d,mode=%d,client_host=%s,volid=%s",
           VolReq->clientUID,VolReq->clientGID,VolReq->mode,
           VolReq->client_host,VolReq->volid);

    log(LOG_DEBUG,"vdqm_DrvMatch(): match %s with %s\n",match_item,drv->drv.dedicate);
    if ((regexec(&drv->expbuf, match_item, (size_t)0, NULL, 0)) == 0) {
        log(LOG_DEBUG,"vdqm_DrvMatch() matched!\n");
        return(1);
    } else return(0);
}
