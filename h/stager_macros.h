/*
 * $Id: stager_macros.h,v 1.8 2004/11/09 18:11:25 jdurand Exp $
 */

#ifndef __stager_macros_h
#define __stager_macros_h

#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>
#include "dlf_api.h"
#include "Cns_api.h"
#include "stager_messages.h"
#include "serrno.h"
#include "osdep.h"
#include "Cuuid.h"
#include "u64subr.h"
#include "stager_uuid.h"
#include "log.h"

/* --------------------------- */
/* Macros for logging with DLF */
/* --------------------------- */

/*
              DLF_LVL_EMERGENCY                     Not used
              DLF_LVL_ALERT                         Very important error
              DLF_LVL_ERROR                         Error message
              DLF_LVL_WARNING                       Self-monitoring warning
              DLF_LVL_AUTH                          Auth error
              DLF_LVL_SECURITY                      Csec error
              DLF_LVL_USAGE                         Trace of routines
              DLF_LVL_SYSTEM                        Say important information
              DLF_LVL_IMPORTANT                     Not used
              DLF_LVL_DEBUG                         Debug level
*/

/* Gcc does not like statement like: xxxx ? 1 : "" */
/* It will issue a warning about type mismatch... */
/* So I convert the integers to string */
#define STAGER_LOG(what,fileid,message,value,message2,value2) { \
  char tmpbuf1[21], tmpbuf2[21]; \
  int _save_serrno = serrno; \
  extern int optionStagerNoDlf; \
  extern int optionStagerLog; \
  extern Cuuid_t stagerUuid; \
  if (message != NULL) { \
    if (message2 != NULL) { \
      if (! optionStagerNoDlf) dlf_write( \
	        stagerUuid, \
	        stagerMessages[what].defaultSeverity, \
	        what, \
	        (struct Cns_fileid *)fileid, \
	        STAGER_NB_PARAMS+3, \
	        stagerMessages[what].what2Type,DLF_MSG_PARAM_STR,func, \
	        message,((strcmp(message,"STRING") == 0) || (strcmp(message,"SIGNAL NAME") == 0)) ? DLF_MSG_PARAM_STR : DLF_MSG_PARAM_INT,value, \
	        message2,((strcmp(message2,"STRING") == 0) || (strcmp(message2,"SIGNAL NAME") == 0)) ? DLF_MSG_PARAM_STR : DLF_MSG_PARAM_INT,value2, \
	        STAGER_LOG_WHERE \
	        ); \
      if (optionStagerLog) { \
        if ((strcmp(message,"STRING") == 0) || (strcmp(message,"SIGNAL NAME") == 0)) { \
	  if ((strcmp(message2,"STRING") == 0) || (strcmp(message2,"SIGNAL NAME") == 0)) { \
	    log(stagerMessages[what].severity2LogLevel, "%s : %s:%d : %s : %s : %s (errno=%d [%s], serrno=%d[%s])\n", func, __FILE__, __LINE__, stagerMessages[what].messageTxt, value, value2, errno, errno ? strerror(errno) : "", serrno, serrno ? sstrerror(serrno) : ""); \
	  } else { \
	    log(stagerMessages[what].severity2LogLevel, "%s : %s:%d : %s : %s : %d (errno=%d [%s], serrno=%d[%s])\n", func, __FILE__, __LINE__, stagerMessages[what].messageTxt, value, value2, errno, errno ? strerror(errno) : "", serrno, serrno ? sstrerror(serrno) : ""); \
	  } \
	} else { \
	  if ((strcmp(message2,"STRING") == 0) || (strcmp(message2,"SIGNAL NAME") == 0)) { \
	    log(stagerMessages[what].severity2LogLevel, "%s : %s:%d : %s : %d : %s (errno=%d [%s], serrno=%d[%s])\n", func, __FILE__, __LINE__, stagerMessages[what].messageTxt, value, value2, errno, errno ? strerror(errno) : "", serrno, serrno ? sstrerror(serrno) : ""); \
	  } else { \
	    log(stagerMessages[what].severity2LogLevel, "%s : %s:%d : %s : %d : %d (errno=%d [%s], serrno=%d[%s])\n", func, __FILE__, __LINE__, stagerMessages[what].messageTxt, value, value2, errno, errno ? strerror(errno) : "", serrno, serrno ? sstrerror(serrno) : ""); \
	  } \
	} \
      }	\
    } else { \
      if (! optionStagerNoDlf) dlf_write( \
	        stagerUuid, \
	        stagerMessages[what].defaultSeverity, \
	        what, \
	        (struct Cns_fileid *)fileid, \
	        STAGER_NB_PARAMS+2, \
	        stagerMessages[what].what2Type,DLF_MSG_PARAM_STR,func, \
	        message,((strcmp(message,"STRING") == 0) || (strcmp(message,"SIGNAL NAME") == 0)) ? DLF_MSG_PARAM_STR : DLF_MSG_PARAM_INT,value, \
	        STAGER_LOG_WHERE \
	        ); \
      if (optionStagerLog) { \
	if ((strcmp(message,"STRING") == 0) || (strcmp(message,"SIGNAL NAME") == 0)) { \
	  log(stagerMessages[what].severity2LogLevel, "%s : %s:%d : %s : %s (errno=%d [%s], serrno=%d[%s])\n", func, __FILE__, __LINE__, stagerMessages[what].messageTxt, value, errno, errno ? strerror(errno) : "", serrno, serrno ? sstrerror(serrno) : ""); \
	} else { \
	  log(stagerMessages[what].severity2LogLevel, "%s : %s:%d : %s : %d (errno=%d [%s], serrno=%d[%s])\n", func, __FILE__, __LINE__, stagerMessages[what].messageTxt, value, errno, errno ? strerror(errno) : "", serrno, serrno ? sstrerror(serrno) : ""); \
	} \
      } \
    } \
  } else { \
    if (message2 != NULL) { \
      if (! optionStagerNoDlf) dlf_write( \
		stagerUuid, \
		stagerMessages[what].defaultSeverity, \
		what, \
		(struct Cns_fileid *)fileid, \
		STAGER_NB_PARAMS+2, \
		stagerMessages[what].what2Type,DLF_MSG_PARAM_STR,func, \
	        message2,((strcmp(message2,"STRING") == 0) || (strcmp(message2,"SIGNAL NAME") == 0)) ? DLF_MSG_PARAM_STR : DLF_MSG_PARAM_INT,value2, \
		STAGER_LOG_WHERE \
		); \
      if (optionStagerLog) { \
	if ((strcmp(message2,"STRING") == 0) || (strcmp(message2,"SIGNAL NAME") == 0)) { \
	  log(stagerMessages[what].severity2LogLevel, "%s : %s:%d : %s : %s\n", func, __FILE__, __LINE__, stagerMessages[what].messageTxt, value2); \
	} else { \
	  log(stagerMessages[what].severity2LogLevel, "%s : %s:%d : %s : %d\n", func, __FILE__, __LINE__, stagerMessages[what].messageTxt, value2); \
	} \
      } \
    } else { \
      if (! optionStagerNoDlf) dlf_write( \
		stagerUuid, \
		stagerMessages[what].defaultSeverity, \
		what, \
		(struct Cns_fileid *)fileid, \
		STAGER_NB_PARAMS+1, \
		stagerMessages[what].what2Type,DLF_MSG_PARAM_STR,func, \
		STAGER_LOG_WHERE \
		); \
      if (optionStagerLog) { \
	log(stagerMessages[what].severity2LogLevel, "%s : %s:%d : %s\n", func, __FILE__, __LINE__, stagerMessages[what].messageTxt); \
      } \
    } \
  } \
  serrno = _save_serrno; \
}

#define STAGER_LOG_EMERGENCY(fileid,string)   STAGER_LOG(STAGER_MSG_EMERGENCY,fileid, "STRING", string, NULL, NULL)
#define STAGER_LOG_ALERT(fileid,string)       STAGER_LOG(STAGER_MSG_ALERT    ,fileid, "STRING", string, NULL, NULL)
#define STAGER_LOG_ERROR(fileid,string)       STAGER_LOG(STAGER_MSG_ERROR    ,fileid, "STRING", string, NULL, NULL)
#define STAGER_LOG_DB_ERROR(fileid,string,string2) STAGER_LOG(STAGER_MSG_ERROR    ,fileid, "STRING", string, "STRING", string2)
#define STAGER_LOG_SYSCALL(fileid,string)     STAGER_LOG(STAGER_MSG_SYSCALL  ,fileid, "STRING", string, NULL, NULL)
#define STAGER_LOG_WARNING(fileid,string)     STAGER_LOG(STAGER_MSG_WARNING  ,fileid, "STRING", string, NULL, NULL)
#define STAGER_LOG_AUTH(fileid,string)        STAGER_LOG(STAGER_MSG_AUTH     ,fileid, "STRING", string, NULL, NULL)
#define STAGER_LOG_SECURITY(fileid,string)    STAGER_LOG(STAGER_MSG_SECURITY ,fileid, "STRING", string, NULL, NULL)
#define STAGER_LOG_USAGE(fileid,string)       STAGER_LOG(STAGER_MSG_USAGE    ,fileid, "STRING", string, NULL, NULL)
#define STAGER_LOG_SIGNAL(fileid,value)       STAGER_LOG(STAGER_MSG_SYSTEM   ,fileid, "SIGNAL NUMBER" ,  value, NULL, NULL)
#define STAGER_LOG_SIGNAL_NAME(fileid,string) STAGER_LOG(STAGER_MSG_SYSTEM   ,fileid, "SIGNAL NAME", string, NULL, NULL)
#define STAGER_LOG_ENTER()                  { \
  extern int stagerTrace; if (stagerTrace) {STAGER_LOG(STAGER_MSG_ENTER ,NULL  ,NULL     ,NULL  , NULL, NULL);} \
}
#define STAGER_LOG_LEAVE()                  { \
  extern int stagerTrace; if (stagerTrace) {STAGER_LOG(STAGER_MSG_LEAVE ,NULL  ,NULL     ,NULL  , NULL, NULL); return; } \
}
#define STAGER_LOG_RETURN(value)            { \
  extern int stagerTrace; if (stagerTrace) {STAGER_LOG(STAGER_MSG_RETURN,NULL  ,"RC"     ,value , NULL, NULL); return(value);} \
}
#define STAGER_LOG_RETURN_NULL()  { \
  extern int stagerTrace; if (stagerTrace) {STAGER_LOG(STAGER_MSG_RETURN,NULL  ,"STRING" ,"NULL" , NULL, NULL); return(NULL);} \
}
#define STAGER_LOG_SYSTEM(fileid,string)    STAGER_LOG(STAGER_MSG_SYSTEM   ,fileid, "STRING", string, NULL, NULL)
#define STAGER_LOG_STARTUP() { \
  int _save_serrno = serrno; \
  dlf_write( \
	    stagerUuid, \
	    stagerMessages[STAGER_MSG_STARTUP].defaultSeverity, \
	    STAGER_MSG_STARTUP, \
	    (struct Cns_fileid *)NULL, \
	    4, \
	    stagerMessages[STAGER_MSG_STARTUP].what2Type,DLF_MSG_PARAM_STR,func, \
	    "GENERATED_DATE",DLF_MSG_PARAM_STR,__DATE__, \
	    "GENERATED_TIME",DLF_MSG_PARAM_STR,__TIME__, \
	    "ARGUMENTS",DLF_MSG_PARAM_STR,stagerConcatenatedArgv \
	    ); \
  serrno = _save_serrno; \
}
#define STAGER_LOG_EXIT(value) { \
  int _save_serrno = serrno; \
  dlf_write( \
	    stagerUuid, \
	    stagerMessages[STAGER_MSG_EXIT].defaultSeverity, \
	    STAGER_MSG_EXIT, \
	    (struct Cns_fileid *)NULL, \
	    2, \
	    stagerMessages[STAGER_MSG_EXIT].what2Type,DLF_MSG_PARAM_STR,func, \
	    "EXIT STATUS",DLF_MSG_PARAM_INT,value \
	    ); \
  serrno = _save_serrno; \
  exit(value); \
}
#define STAGER_LOG_IMPORTANT(fileid,string) STAGER_LOG(STAGER_MSG_IMPORTANT,fileid, "STRING", string, NULL, NULL)
#define STAGER_LOG_DEBUG(fileid,string)     { \
  extern int stagerDebug; if (stagerDebug) {STAGER_LOG(STAGER_MSG_DEBUG ,fileid,"STRING" ,string, NULL, NULL);} \
}

#define CALLIT(args) { \
  STAGER_LOG_USAGE(NULL,#args); \
  args; \
}

#define STAGER_NB_ELEMENTS(a) (sizeof(a)/sizeof((a)[0]))

/* Exit out of a thread if we received a signal trapped by the signal thread, using label for exit point */
/* ----------------------------------------------------------------------------------------------------- */
#define STAGER_THREAD_CHECK_SIGNAL(label) { \
  extern int stagerSignaled; \
  int stagerSignaledBackup; \
  extern void *stagerSignalCthreadStructure; \
  extern Cuuid_t stagerUuid; \
  if (Cthread_mutex_timedlock_ext(stagerSignalCthreadStructure,STAGER_MUTEX_TIMEOUT) != 0) { \
    STAGER_LOG_SYSCALL(NULL,"Cthread_mutex_timedlock_ext"); \
    goto label; \
  } \
  stagerSignaledBackup = stagerSignaled; \
  if (Cthread_mutex_unlock_ext(stagerSignalCthreadStructure) != 0) { \
    STAGER_LOG_SYSCALL(NULL,"Cthread_mutex_unlock_ext"); \
    goto label; \
  } \
  if (stagerSignaledBackup >= 0) { \
    STAGER_LOG_DEBUG(NULL,"Exiting because Signal Thread catched a signal"); \
    goto label; \
  } \
}

/* This macro avoid cut/paste of always the same code for methods not supposed to fail */
/* ----------------------------------------------------------------------------------- */
#define STAGER_DB_METHOD_THAT_SHOULD_NOT_FAIL(method,table,member,label) { \
  if (method (table,member) != 0) { \
    serrno = SEINTERNAL; \
    STAGER_LOG_DB_ERROR(fileid,#method,""); \
    rc = -1; \
    goto label; \
  } \
}

#endif /* __stager_macros_h */
