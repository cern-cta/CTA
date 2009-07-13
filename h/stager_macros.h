/*
 * $Id: stager_macros.h,v 1.26 2009/07/13 06:22:08 waldron Exp $
 */

#ifndef __stager_macros_h
#define __stager_macros_h

#include <stdio.h>
#include <stdlib.h>
#include "stager_util.h"
#include "Cns_api.h"
#include "stager_messages.h"
#include "serrno.h"
#include "Cuuid.h"
#include "stager_extern_globals.h"
#include "stager_uuid.h"

/* -------------------*/
/* Macros for logging */
/* -------------------*/

#define STAGER_LOG(what,fileid,message,value,message2,value2) stager_log(func,__FILE__,__LINE__,what,fileid,message,value,message2,value2);
#define STAGER_LOG_ERROR(fileid,string)            STAGER_LOG(STAGER_MSG_ERROR    ,fileid, "STRING", string, (const char *)NULL, (const char *)NULL)
#define STAGER_LOG_DB_ERROR(fileid,string,string2) STAGER_LOG(STAGER_MSG_ERROR    ,fileid, "STRING", string, "STRING", string2)
#define STAGER_LOG_ENTER() {}
#define STAGER_LOG_LEAVE() {return;}
#define STAGER_LOG_RETURN(value) {return(value);}
#define STAGER_NB_ELEMENTS(a) (sizeof(a)/sizeof((a)[0]))

#endif /* __stager_macros_h */
