/*
 * $Id: stager_messages.h,v 1.2 2004/11/08 10:56:04 jdurand Exp $
 */

#ifndef __stager_messages_h
#define __stager_messages_h

#include "dlf_api.h"

#define STAGER_FACILITY_NAME "stager"
#define STAGER_LOG_WHERE "RequestUuid",DLF_MSG_PARAM_UUID,stager_request_uuid,"SubRequestUuid",DLF_MSG_PARAM_UUID,stager_subrequest_uuid,"File",DLF_MSG_PARAM_STR,__FILE__,"Line",DLF_MSG_PARAM_INT,__LINE__,"errno",DLF_MSG_PARAM_INT,errno,"serrno",DLF_MSG_PARAM_INT,serrno
#define STAGER_NB_PARAMS 6 /* This is the number of arguments in STAGER_LOG_WHERE */

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
  STAGER_MSG_RESTART
};
struct stagerMessages {
  int msgNo;
  int defaultSeverity;
  char what2Type[CA_MAXLINELEN+1];
  char messageTxt[CA_MAXLINELEN+1];
};

extern struct stagerMessages stagerMessages[];

EXTERN_C int DLL_DECL stager_messagesNbElements _PROTO(());

#endif /* __stager_messages_h */
