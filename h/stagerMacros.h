/*
 * $Id: stagerMacros.h,v 1.2 2004/10/22 23:08:02 jdurand Exp $
 */

#ifndef __stagerMacros_h
#define __stagerMacros_h

#include "dlf_api.h"
#include "Cns_api.h"
#include "stagerMessages.h"
#include "serrno.h"
#include "osdep.h"
#include "Cthread_api.h"
#include "Cpool_api.h"

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
#define STAGER_LOG(what,fileid,message,value) { \
  extern int DLL_DECL _Cpool_self _PROTO(()); \
  char tmpbuf1[21], tmpbuf2[21]; \
  int _save_serrno = serrno; \
  if (message != NULL) { \
    dlf_write( \
	      stagerUuid, \
	      stagerMessages[what].defaultSeverity, \
	      what, \
	      (struct Cns_fileid *)fileid, \
	      STAGER_NB_PARAMS+4, \
	      stagerMessages[what].what2Type,DLF_MSG_PARAM_STR,func, \
	      "THREAD_ID",DLF_MSG_PARAM_STR,(Cthread_self() >= 0) ? u64tostr((u_signed64) Cthread_self(), tmpbuf1, 0) : "", \
	      "POOL_ID",  DLF_MSG_PARAM_STR,(_Cpool_self()  >= 0) ? u64tostr((u_signed64) _Cpool_self() , tmpbuf2, 0) : "", \
	      message,((strcmp(message,"STRING") == 0) || (strcmp(message,"SIGNAL NAME") == 0)) ? DLF_MSG_PARAM_STR : DLF_MSG_PARAM_INT,value, \
	      STAGER_LOG_WHERE \
	      ); \
  } else { \
    dlf_write( \
	      stagerUuid, \
	      stagerMessages[what].defaultSeverity, \
	      what, \
	      (struct Cns_fileid *)fileid, \
	      STAGER_NB_PARAMS+3, \
	      stagerMessages[what].what2Type,DLF_MSG_PARAM_STR,func, \
	      "THREAD_ID",DLF_MSG_PARAM_STR,(Cthread_self() >= 0) ? u64tostr((u_signed64) Cthread_self(), tmpbuf1, 0) : "", \
	      "POOL_ID",  DLF_MSG_PARAM_STR,(_Cpool_self()  >= 0) ? u64tostr((u_signed64) _Cpool_self() , tmpbuf2, 0) : "", \
	      STAGER_LOG_WHERE \
	      ); \
  } \
  serrno = _save_serrno; \
}

#define STAGER_LOG_EMERGENCY(fileid,string)   STAGER_LOG(STAGER_MSG_EMERGENCY,fileid, "STRING", string)
#define STAGER_LOG_ALERT(fileid,string)       STAGER_LOG(STAGER_MSG_ALERT    ,fileid, "STRING", string)
#define STAGER_LOG_ERROR(fileid,string)       STAGER_LOG(STAGER_MSG_ERROR    ,fileid, "STRING", string)
#define STAGER_LOG_SYSCALL(fileid,string)     STAGER_LOG(STAGER_MSG_SYSCALL  ,fileid, "STRING", string)
#define STAGER_LOG_WARNING(fileid,string)     STAGER_LOG(STAGER_MSG_WARNING  ,fileid, "STRING", string)
#define STAGER_LOG_AUTH(fileid,string)        STAGER_LOG(STAGER_MSG_AUTH     ,fileid, "STRING", string)
#define STAGER_LOG_SECURITY(fileid,string)    STAGER_LOG(STAGER_MSG_SECURITY ,fileid, "STRING", string)
#define STAGER_LOG_USAGE(fileid,string)       STAGER_LOG(STAGER_MSG_USAGE    ,fileid, "STRING", string)
#define STAGER_LOG_SIGNAL(fileid,value)       STAGER_LOG(STAGER_MSG_SYSTEM   ,fileid, "SIGNAL NUMBER" ,  value)
#define STAGER_LOG_SIGNAL_NAME(fileid,string) STAGER_LOG(STAGER_MSG_SYSTEM   ,fileid, "SIGNAL NAME", string)
#define STAGER_LOG_ENTER()                  { \
  extern stagerTrace; if (stagerTrace) {STAGER_LOG(STAGER_MSG_ENTER ,NULL  ,NULL     ,NULL  );} \
}
#define STAGER_LOG_LEAVE()                  { \
  extern stagerTrace; if (stagerTrace) {STAGER_LOG(STAGER_MSG_LEAVE ,NULL  ,NULL     ,NULL  ); return; } \
}
#define STAGER_LOG_RETURN(value)            { \
  extern stagerTrace; if (stagerTrace) {STAGER_LOG(STAGER_MSG_RETURN,NULL  ,"RC"     ,value ); return(value);} \
}
#define STAGER_LOG_RETURN_NULL()  { \
  extern stagerTrace; if (stagerTrace) {STAGER_LOG(STAGER_MSG_RETURN,NULL  ,"STRING" ,"NULL" ); return(NULL);} \
}
#define STAGER_LOG_SYSTEM(fileid,string)    STAGER_LOG(STAGER_MSG_SYSTEM   ,fileid, "STRING", string)
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
#define STAGER_LOG_IMPORTANT(fileid,string) STAGER_LOG(STAGER_MSG_IMPORTANT,fileid, "STRING", string)
#define STAGER_LOG_DEBUG(fileid,string)     { \
  extern stagerDebug; if (stagerDebug) {STAGER_LOG(STAGER_MSG_DEBUG ,fileid,"STRING" ,string);} \
}

#define CALLIT(args) { \
  STAGER_LOG_USAGE(NULL,#args); \
  args; \
}

#endif /* __stagerMacros_h */
