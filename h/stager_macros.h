/*
 * $Id: stager_macros.h,v 1.22 2005/06/16 15:51:32 jdurand Exp $
 */

#ifndef __stager_macros_h
#define __stager_macros_h

#include <stdio.h>
#include <stdlib.h>
#include "stager_util.h"
#include "dlf_api.h"
#include "Cns_api.h"
#include "stager_messages.h"
#include "serrno.h"
#include "Cuuid.h"
#include "stager_extern_globals.h"
#include "stager_uuid.h"

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

/* Possible messages saying this is a string are: "STRING", "SIGNAL NAME" - everything else is an integer */
#define STAGER_LOG(what,fileid,message,value,message2,value2) stager_log(func,__FILE__,__LINE__,what,fileid,message,value,message2,value2);
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
#define STAGER_LOG_ENTER() {}
#define STAGER_LOG_LEAVE() {return;}
#define STAGER_LOG_RETURN(value) {return(value);}
#define STAGER_LOG_RETURN_NULL() {return(NULL);}
#define STAGER_LOG_SYSTEM(fileid,string)    STAGER_LOG(STAGER_MSG_SYSTEM   ,fileid, "STRING", string, NULL, NULL)
#define STAGER_LOG_STARTUP() { \
  if (! stagerNoDlf) { \
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
  } \
}
#define STAGER_LOG_EXIT(value) { \
  if (! stagerNoDlf) { \
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
  } \
}
/* Special version of STAGER_LOG_STARTUP but with fileid, used in the job */
#define STAGER_LOG_STARTUP_FULL(fileid) { \
  if (! stagerNoDlf) { \
    int _save_serrno = serrno; \
    dlf_write( \
	      stager_request_uuid, \
	      stagerMessages[STAGER_MSG_STARTUP].defaultSeverity, \
	      STAGER_MSG_STARTUP, \
	      (struct Cns_fileid *)fileid, \
	      9, \
	      stagerMessages[STAGER_MSG_STARTUP].what2Type,DLF_MSG_PARAM_STR,func, \
	      "GENERATED_DATE",DLF_MSG_PARAM_STR,__DATE__, \
	      "GENERATED_TIME",DLF_MSG_PARAM_STR,__TIME__, \
	      "ARGUMENTS",DLF_MSG_PARAM_STR,stagerConcatenatedArgv, \
	      "SubRequestUuid",DLF_MSG_PARAM_UUID,stager_subrequest_uuid, \
	      "File",DLF_MSG_PARAM_STR,__FILE__, \
	      "Line",DLF_MSG_PARAM_INT,__LINE__, \
	      "errno",DLF_MSG_PARAM_INT,errno, \
	      "serrno",DLF_MSG_PARAM_INT,serrno \
	      ); \
    serrno = _save_serrno; \
  } \
}
/* Special version of STAGER_LOG_EXIT but with fileid, used in the job */
#define STAGER_LOG_EXIT_FULL(fileid,value) { \
  if (! stagerNoDlf) { \
    int _save_serrno = serrno; \
    dlf_write( \
	      stager_request_uuid, \
	      stagerMessages[STAGER_MSG_EXIT].defaultSeverity, \
	      STAGER_MSG_EXIT, \
	      (struct Cns_fileid *)fileid, \
	      7, \
	      stagerMessages[STAGER_MSG_EXIT].what2Type,DLF_MSG_PARAM_STR,func, \
	      "EXIT STATUS",DLF_MSG_PARAM_INT,value, \
	      "SubRequestUuid",DLF_MSG_PARAM_UUID,stager_subrequest_uuid, \
	      "File",DLF_MSG_PARAM_STR,__FILE__, \
	      "Line",DLF_MSG_PARAM_INT,__LINE__, \
	      "errno",DLF_MSG_PARAM_INT,errno, \
	      "serrno",DLF_MSG_PARAM_INT,serrno \
	      ); \
    serrno = _save_serrno; \
    exit(value); \
  } \
}
#define STAGER_LOG_IMPORTANT(fileid,string) STAGER_LOG(STAGER_MSG_IMPORTANT,fileid, "STRING", string, NULL, NULL)
#define STAGER_LOG_DEBUG(fileid,string)     { \
  if (stagerDebug) {STAGER_LOG(STAGER_MSG_DEBUG ,fileid,"STRING" ,string, NULL, NULL);} \
}
#define STAGER_LOG_VERBOSE(fileid,string)     { }

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
  if (Cthread_mutex_timedlock_ext(stagerSignalCthreadStructure,STAGER_MUTEX_TIMEOUT) != 0) { \
    STAGER_LOG_SYSCALL(NULL,"Cthread_mutex_timedlock_ext"); \
  } else { \
    stagerSignaledBackup = stagerSignaled; \
    if (Cthread_mutex_unlock_ext(stagerSignalCthreadStructure) != 0) { \
      STAGER_LOG_SYSCALL(NULL,"Cthread_mutex_unlock_ext"); \
    } \
    if (stagerSignaledBackup >= 0) { \
      STAGER_LOG_DEBUG(NULL,"Exiting because Signal Thread catched a signal"); \
      goto label; \
    } \
  } \
}

#define STAGER_THREAD_CHECK_SIGNAL_EXT(label,extracode) { \
  extern int stagerSignaled; \
  int stagerSignaledBackup; \
  extern void *stagerSignalCthreadStructure; \
  if (Cthread_mutex_timedlock_ext(stagerSignalCthreadStructure,STAGER_MUTEX_TIMEOUT) != 0) { \
    STAGER_LOG_SYSCALL(NULL,"Cthread_mutex_timedlock_ext"); \
  } else { \
    stagerSignaledBackup = stagerSignaled; \
    if (Cthread_mutex_unlock_ext(stagerSignalCthreadStructure) != 0) { \
      STAGER_LOG_SYSCALL(NULL,"Cthread_mutex_unlock_ext"); \
    } \
    if (stagerSignaledBackup >= 0) { \
      STAGER_LOG_DEBUG(NULL,"Exiting because Signal Thread catched a signal"); \
      extracode; \
      goto label; \
    } \
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
