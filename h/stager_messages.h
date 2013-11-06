/*
 * $Id: stager_messages.h,v 1.7 2009/08/18 09:42:59 waldron Exp $
 */

#ifndef __stager_messages_h
#define __stager_messages_h

#include "dlf_api.h"

#define STAGER_LOG_WHERE(file,line) "SubRequestUuid",DLF_MSG_PARAM_UUID,stager_subrequest_uuid,"File",DLF_MSG_PARAM_STR,file,"Line",DLF_MSG_PARAM_INT,line,"errno",DLF_MSG_PARAM_INT,errno,"serrno",DLF_MSG_PARAM_INT,serrno

#define STAGER_NB_PARAMS 5 /* This is the number of arguments in STAGER_LOG_WHERE */

enum stagerMessagesNo {
  STAGER_MSG_ERROR
};

struct stagerMessages {
  int msgNo;
  int defaultSeverity;
  int severity2LogLevel;
  char what2Type[CA_MAXLINELEN+1];
  char messageTxt[CA_MAXLINELEN+1];
};

extern struct stagerMessages stagerMessages[];

#endif /* __stager_messages_h */
