#ifndef __rm_messages_h

#include "osdep.h"
#include "Castor_limits.h"

#define RM02	"%s: RM02 - %s : %s error : %s\n"
#define RMXX	"%s: %s\n"
#define	RM99	"%s: RM99 - returns %d\n"

#define RMMASTER_FACILITY_NAME "rmmaster"
enum stagerMessagesNo {
  RM_MSG_STARTUP  ,
  RM_MSG_EXIT     ,
  RM_MSG_JOB_RECEIVED ,
  RM_MSG_JOB_STARTJOB ,
  RM_MSG_JOB_COMPLETED ,
  RM_MSG_ERROR    ,
  RM_MSG_SYSCALL  ,
  RM_MSG_WARNING  ,
  RM_MSG_USAGE    , 
  RM_MSG_SYSTEM
};
struct rmMessages {
  int msgNo;
  int defaultSeverity;
  char what2Type[CA_MAXLINELEN+1];
  char messageTxt[CA_MAXLINELEN+1];
};

extern struct rmMessages rmMessages[];

EXTERN_C int DLL_DECL rm_messagesNbElements _PROTO(());

#endif /* __rm_messages_h */
