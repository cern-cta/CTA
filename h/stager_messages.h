/*
 * $Id: stager_messages.h,v 1.6 2007/08/27 16:00:24 sponcec3 Exp $
 */

#ifndef __stager_messages_h
#define __stager_messages_h

#include "dlf_api.h"

#define STAGER_FACILITY_NAME "stager"
#define STAGER_LOG_WHERE(file,line) "SubRequestUuid",DLF_MSG_PARAM_UUID,stager_subrequest_uuid,"File",DLF_MSG_PARAM_STR,file,"Line",DLF_MSG_PARAM_INT,line,"errno",DLF_MSG_PARAM_INT,errno,"serrno",DLF_MSG_PARAM_INT,serrno

#define STAGER_NB_PARAMS 5 /* This is the number of arguments in STAGER_LOG_WHERE */

enum stagerMessagesNo {
  STAGER_MSG_EMERGENCY,
  STAGER_MSG_ALERT    ,
  STAGER_MSG_ERROR    ,
  STAGER_MSG_SYSCALL  ,
  STAGER_MSG_WARNING  ,
  STAGER_MSG_AUTH     ,
  STAGER_MSG_SECURITY ,
  STAGER_MSG_USAGE    , 
  STAGER_MSG_ENTER    , 
  STAGER_MSG_LEAVE    ,
  STAGER_MSG_RETURN   ,
  STAGER_MSG_SYSTEM   ,
  STAGER_MSG_STARTUP  ,
  STAGER_MSG_IMPORTANT,
  STAGER_MSG_DEBUG    ,
  STAGER_MSG_EXIT     ,
  STAGER_MSG_RESTART  ,
  STAGER_MSG_FQUERY   ,
  STAGER_MSG_RQUERY   ,
  STAGER_MSG_IQUERY
};
struct stagerMessages {
  int msgNo;
  int defaultSeverity;
  int severity2LogLevel;
  char what2Type[CA_MAXLINELEN+1];
  char messageTxt[CA_MAXLINELEN+1];
};

extern struct stagerMessages stagerMessages[];

EXTERN_C int DLL_DECL stager_messagesNbElements _PROTO(());

#endif /* __stager_messages_h */
