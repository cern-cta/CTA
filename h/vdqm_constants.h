/*
 * $Id: vdqm_constants.h,v 1.1 2004/07/30 12:56:49 motiakov Exp $
 */

/*
 * Copyright (C) 1999-2001 by CERN/IT/PDP/DM
 * All rights reserved
 */

/*
 * @(#)$RCSfile: vdqm_constants.h,v $ $Revision: 1.1 $ $Date: 2004/07/30 12:56:49 $ CERN IT-PDP/DM Olof Barring
 */

/*
 * vdqm_constants.h - definitions of all VDQM constants
 */

#ifndef _VDQM_CONSTANTS_H
#define _VDQM_CONSTANTS_H
/*
 * VDQM magic number
 */
#define VDQM_MAGIC (0x8537)

/*
 * Request types
 */
#define VDQM_BASE_REQTYPE (0x3100)
#define VDQM_REQ_MIN      (VDQM_BASE_REQTYPE)
#define VDQM_VOL_REQ      (VDQM_BASE_REQTYPE+0x01)
#define VDQM_DRV_REQ      (VDQM_BASE_REQTYPE+0x02)
#define VDQM_PING         (VDQM_BASE_REQTYPE+0x03)
#define VDQM_CLIENTINFO   (VDQM_BASE_REQTYPE+0x04)
#define VDQM_SHUTDOWN     (VDQM_BASE_REQTYPE+0x05)
#define VDQM_HOLD         (VDQM_BASE_REQTYPE+0x06)
#define VDQM_RELEASE      (VDQM_BASE_REQTYPE+0x07)
#define VDQM_REPLICA      (VDQM_BASE_REQTYPE+0x08)
#define VDQM_RESCHEDULE   (VDQM_BASE_REQTYPE+0x09)
#define VDQM_ROLLBACK     (VDQM_BASE_REQTYPE+0x0A)
#define VDQM_COMMIT       (VDQM_BASE_REQTYPE+0x0B)
#define VDQM_DEL_VOLREQ   (VDQM_BASE_REQTYPE+0x0C)
#define VDQM_DEL_DRVREQ   (VDQM_BASE_REQTYPE+0x0D)
#define VDQM_GET_VOLQUEUE (VDQM_BASE_REQTYPE+0x0E)
#define VDQM_GET_DRVQUEUE (VDQM_BASE_REQTYPE+0x0F)
#define VDQM_SET_PRIORITY (VDQM_BASE_REQTYPE+0x10)
#define VDQM_DEDICATE_DRV (VDQM_BASE_REQTYPE+0x11)
#define VDQM_HANGUP       (VDQM_BASE_REQTYPE+0x12)
#define VDQM_REQ_MAX      (VDQM_BASE_REQTYPE+0x13)
#define VDQM_VALID_REQTYPE(X) ((X)>VDQM_REQ_MIN && (X)<VDQM_REQ_MAX)

/*
 * Privileged requests. Note that VDQM_DEL_VOLREQ and VDQM_DEL_DRVREQ
 * are also privileged if client isn't the owner.
 */
#define VDQM_PRIV_REQ(X) ((X)==VDQM_SHUTDOWN || (X)==VDQM_HOLD || \
    (X)==VDQM_RELEASE || (X)==VDQM_REPLICA || (X)==VDQM_RESCHEDULE || \
    (X)==VDQM_SET_PRIORITY || (X)==VDQM_DEDICATE_DRV)

/*
 * Define string constants for logging purpose. Don't forget to update
 * if a new request value is added ! The constants should be in same order
 * as the request values.
 */
#define VDQM_REQ_VALUES {VDQM_VOL_REQ, VDQM_DRV_REQ, VDQM_PING, \
    VDQM_CLIENTINFO, VDQM_SHUTDOWN, VDQM_HOLD, VDQM_RELEASE, \
    VDQM_REPLICA, VDQM_RESCHEDULE, VDQM_ROLLBACK, VDQM_COMMIT, \
    VDQM_DEL_VOLREQ, VDQM_DEL_DRVREQ, VDQM_GET_VOLQUEUE, VDQM_GET_DRVQUEUE,\
    VDQM_SET_PRIORITY, VDQM_DEDICATE_DRV, -1}
#define VDQM_REQ_STRINGS {"VOL_REQ", "DRV_REQ", "PING", "CLIENTINFO", \
    "SHUTDOWN", "HOLD", "RELEASE", "REPLICA", "RESCHEDULE", "ROLLBACK", \
    "COMMIT", "DEL_VOLREQ", "DEL_DRVREQ", "GET_VOLQUEUE", "GET_DRVQUEUE", \
    "SET_PRIORITY", "DEDICATE_DRV",""}

/*
 * Unit status types
 */
#define VDQM_STATUS_MIN    (0x0000)
#define VDQM_UNIT_UP       (0x0001)
#define VDQM_UNIT_DOWN     (0x0002)
#define VDQM_UNIT_WAITDOWN (0x0004)
#define VDQM_UNIT_ASSIGN   (0x0008)
#define VDQM_UNIT_RELEASE  (0x0010)
#define VDQM_UNIT_BUSY     (0x0020)
#define VDQM_UNIT_FREE     (0x0040)
#define VDQM_VOL_MOUNT     (0x0080)
#define VDQM_VOL_UNMOUNT   (0x0100)
#define VDQM_UNIT_UNKNOWN  (0x0200)
#define VDQM_UNIT_ERROR    (0x0400)
#define VDQM_UNIT_MBCOUNT  (0x0800)
#define VDQM_UNIT_QUERY    (0x1000)
#define VDQM_FORCE_UNMOUNT (0x2000)
#define VDQM_TPD_STARTED   (0x4000)
#define VDQM_STATUS_MAX    (0x8000)
#define VDQM_VALID_STATUS(X) ((X)>VDQM_STATUS_MIN && (X)<VDQM_STATUS_MAX)
/*
 * Define string constants for logging purpose. Don't forget to update
 * if a new status value is added ! The constants should be in same order
 * as the status values.
 */
#define VDQM_STATUS_VALUES { VDQM_UNIT_UP , VDQM_UNIT_DOWN ,\
    VDQM_UNIT_WAITDOWN , VDQM_UNIT_ASSIGN , VDQM_UNIT_RELEASE , \
    VDQM_UNIT_BUSY , VDQM_UNIT_FREE , VDQM_VOL_MOUNT , VDQM_VOL_UNMOUNT , \
    VDQM_UNIT_UNKNOWN , VDQM_UNIT_ERROR , VDQM_UNIT_MBCOUNT, \
    VDQM_UNIT_QUERY, VDQM_FORCE_UNMOUNT, VDQM_TPD_STARTED, -1}
 
#define VDQM_STATUS_STRINGS {"VDQM_UNIT_UP","VDQM_UNIT_DOWN",\
   "VDQM_UNIT_WAITDOWN","VDQM_UNIT_ASSIGN","VDQM_UNIT_RELEASE", \
   "VDQM_UNIT_BUSY","VDQM_UNIT_FREE","VDQM_VOL_MOUNT","VDQM_VOL_UNMOUNT", \
   "VDQM_UNIT_UNKNOWN","VDQM_UNIT_ERROR","VDQM_UNIT_MBCOUNT", \
   "VDQM_UNIT_QUERY", "VDQM_FORCE_UNMOUNT", "VDQM_TPD_STARTED", ""}

/*
 * Volume request priority levels
 */
#define VDQM_PRIORITY_MIN     (0x0)
#define VDQM_PRIORITY_NORMAL  (0x1)
#define VDQM_PRIORITY_MAX     (0x2)
#define VDQM_PRIORITY_DEFAULT (VDQM_PRIORITY_NORMAL)
#define VALID_PRIORITY(X) ((X)>=VDQM_PRIORITY_MIN && (X)<=VDQM_PRIORITY_MAX)


#define VDQM_PRIORITY_VALUES { VDQM_PRIORITY_MIN , VDQM_PRIORITY_DEFAULT , \
    VDQM_PRIORITY_MAX , -1}

#define VDQM_PRIORITY_STRINGS {"VDQM_PRIORITY_MIN","VDQM_PRIORITY_DEFAULT", \
   "VDQM_PRIORITY_MAX",""}

/*
 * Dedication keywords. Define them all here so that nothing is forgotten
 * if format changes. Any new search criteria must be added to each one 
 * of the constants below.
 */
#define VDQM_DEDICATE_PREFIX   {"uid=","gid=","name=","host=","vid=", \
                                "mode=","datestr=","timestr=","age=",""}
#define VDQM_DEDICATE_DEFAULTS {"uid=.*","gid=.*","name=.*","host=.*", \
                                "vid=.*","mode=.*","datestr=.*", \
                                "timestr=.*","age=.*",""}
#define VDQM_DEDICATE_FORMAT   {"uid=%d","gid=%d","name=%s","host=%s", \
                                "vid=%s","mode=%d","datestr=%s", \
                                "timestr=%s","age=%d",""}
#define VDQM_DEDICATE_DATEFORM  "%d/%m/%C%y"
#define VDQM_DEDICATE_TIMEFORM  "%H:%M:%S"
#define FILL_MATCHSTR(STR,FORM,FORMS)   {int __i = 0; *(STR) = *(FORM) = '\0'; \
        while ( *(FORMS)[__i]!='\0') {strcat(FORM,FORMS[__i]);  \
            strcat(FORM,","); __i++;}; \
        if ( strlen(FORM) > 0) (FORM)[strlen(FORM)-1] = '\0'; \
        sprintf(STR,FORM, \
                uid,gid,name,host,vid,mode,datestr,timestr,age);}
/*
 * Defaults
 */
#ifndef VDQM_HOST
#define VDQM_HOST        "vdqm"
#endif /* VDQM_HOST */

#ifndef VDQM_PORT
#ifdef CSEC
#define VDQM_PORT        (5512)
#else
#define VDQM_PORT        (5012)
#endif /* CSEC */
#endif /* VDQM_PORT */

#ifndef VDQMBC_PORT
#ifdef CSEC
#define VDQMBC_PORT      (9390)
#else
#define VDQMBC_PORT      (8890)
#endif /* CSEC */
#endif /* VDQMBC_PORT */

#define VDQM_TIMEOUT     (300)
#define VDQM_THREADPOOL  (10)
#define VDQM_MAXTIMEDIFF (600)        /* Deferred unmount: max time diff to oldest request */
#define VDQM_RETRYDRVTIM (600)        /* Time between retries on drive */
#define VDQM_HDRBUFSIZ (3*LONGSIZE)
#define VDQM_MSGBUFSIZ (1024)

#ifndef VDQM_REPLICA_PIPELEN
#define VDQM_REPLICA_PIPELEN (10)
#endif /* VDQM_REPLICA_PIPELEN */

#ifndef VDQM_REPLICA_RETRIES
#define VDQM_REPLICA_RETRIES (3)
#endif /* VDQM_REPLICA_RETRIES */

#if defined(SOMAXCONN)
#define VDQM_BACKLOG (SOMAXCONN)
#else  /* VDQM_BACKLOG */
#define VDQM_BACKLOG (5)
#endif /* VDQM_BACKLOG */

#ifndef VDQM_LOG_FILE
#define VDQM_LOG_FILE "vdqm.log"
#endif /* VDQM_LOG_FILE */

#ifndef VDQM_DRIVES_FILE
#define VDQM_DRIVES_FILE "vdqm.drives"
#endif /* VDQM_DRIVES_FILE */



#ifndef VALID_VDQM_MSGLEN
#define VALID_VDQM_MSGLEN(X) ((X)>0 && (X)<(VDQM_MSGBUFSIZ))
#endif /* VALID_VDQM_MSGLEN */

#endif /* _VDQM_CONSTANTS_H */
