/*
 * Copyright (C) 1999-2004 by CERN IT
 * All rights reserved
 */

/*
 * rtcp_CallVMGR.c - Call VMGR and fill in missing info.
 */

#include <stdlib.h>
#if defined(_WIN32)
#include <winsock2.h>
#endif /* _WIN32 */
#include <errno.h>
#include <stdarg.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <ctype.h>
#include <unistd.h>
#include "net.h"
#include "Castor_limits.h"
#include "log.h"
#include <Cuuid.h>
#include "rtcp_constants.h"
#include "rtcp.h"
#if defined(RTCP_SERVER)
#include "rtcp_server.h"
#endif /* RTCP_SERVER */
#include "vmgr_api.h"
#include "serrno.h"

#if defined(_WIN32)
extern uid_t geteuid();
extern gid_t getegid();
#endif /* _WIN32 */

#define LOWERCASE(X) {char *__c; \
    for (__c=X; *__c != '\0'; __c++) *__c=tolower(*__c); } 

static char vmgr_error_buffer[512];        /* Vmgr error buffer */

int rtcp_CallVMGR(tape_list_t *tape, char *realVID) {
    int rc = 0;
#if defined(VMGR)
    tape_list_t *tl;
    rtcpTapeRequest_t *tapereq;
    char *firstVID;
    uid_t euid;
    gid_t egid;

    if ( tape == NULL ) {
        serrno = EINVAL;
        return(-1);
    }

    /* Make sure error buffer of VMGR do not go to stderr */
    vmgr_error_buffer[0] = '\0';
    if (vmgr_seterrbuf(vmgr_error_buffer,sizeof(vmgr_error_buffer)) != 0) {
		return(-1);
    }

    /* We need to know exact current euid/egid */
    euid = geteuid();
    egid = getegid();
#if defined(_WIN32)
    if (euid < 0 || egid < 0) {
		serrno = SEUSERUNKN;
		return(-1);
    }
#endif

    firstVID = tape->tapereq.vid;
    if ( *firstVID == '\0' ) firstVID = tape->tapereq.vsn;
    if ( realVID != NULL ) {
        strcpy(realVID,firstVID);
        UPPERCASE(realVID);
    }

    CLIST_ITERATE_BEGIN(tape,tl) {
        tapereq = &tl->tapereq;
        if ( *tapereq->vid == '\0' && *tapereq->vsn != '\0' )
            strcpy(tapereq->vid,tapereq->vsn);
        if ( ( *tapereq->density == '\0' && 
               strcmp(tapereq->dgn,"CART") != 0 ) ||
			 ( *tapereq->dgn == '\0' ) || 
			 ( *tapereq->label == '\0' ) || 
			 ( *tapereq->vsn == '\0' ) ) {
            UPPERCASE(tapereq->vid);
            UPPERCASE(tapereq->vsn);
			if ((rc = vmgrcheck(tapereq->vid,
								tapereq->vsn,
								tapereq->dgn,
								tapereq->density,
								tapereq->label,
								tapereq->mode,
								euid,
								egid)) != 0) {
	      
				if (vmgr_error_buffer[0] != '\0') {
#if defined(RTCP_SERVER)
					rtcpd_AppendClientMsg(tl, NULL, "%s\n",vmgr_error_buffer);
#else /* RTCP_SERVER */
					strcpy(tapereq->err.errmsgtxt,vmgr_error_buffer);
#endif /* RTCP_SERVER */
				}
				tapereq->err.severity = RTCP_USERR | RTCP_FAILED;
				if (vmgr_error_buffer[0] != '\0') {
#if defined(RTCP_SERVER)
					rtcp_log(LOG_ERR,"%s\n",vmgr_error_buffer);
#else /* RTCP_SERVER */
					rtcp_log(LOG_INFO,"! volume ID %s %s\n", tapereq->vid, vmgr_error_buffer);
#endif /* RTCP_SERVER */
				}
				serrno = tapereq->err.errorcode = rc;
				return(-1);
			}
            LOWERCASE(tapereq->label);
        }
    } CLIST_ITERATE_END(tape,tl);
#endif /* VMGR */
    return(rc);
}
