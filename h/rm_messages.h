#ifndef __rm_messages_h

#include "osdep.h"
#include "Castor_limits.h"

#define RM02	"%s: RM02 - %s : %s error : %s\n"
#define RMXX	"%s: %s\n"
#define	RM99	"%s: RM99 - returns %d\n"

#define RMMASTER_FACILITY_NAME "rmmaster"

struct rmDlfMessages {
  int msgNumber;
  char msgText[CA_MAXLINELEN+1];
};

extern struct rmDlfMessages  rmDlfMsgs[];

EXTERN_C int DLL_DECL rm_msgsNbMessages _PROTO(());

/* DLF Messages numbers --- V.Motyakov 17.03.2006 */

#define RM_DLF_MSG_START                11
#define RM_DLF_MSG_EXIT                 12
#define RM_DLF_MSG_LSF_REOPEN           13
#define RM_DLF_MSG_SYSCALL              14
#define RM_DLF_MSG_WAIT_LSF_PROXY       15
#define RM_DLF_MSG_PING_LSF_PROXY       16
#define RM_DLF_MSG_RELOAD_JOBS_E        17
#define RM_DLF_MSG_RELOAD_NODES_E       18
#define RM_DLF_MSG_RELOAD_JOBS          19
#define RM_DLF_MSG_RELOAD_NODES         20
#define RM_DLF_MSG_THREAD_CREATE_E      21
#define RM_DLF_MSG_SIGNAL_RCVD          22
#define RM_DLF_MSG_LISTENING            23
#define RM_DLF_MSG_SOCKET_CR_E          24
#define RM_DLF_MSG_SOCKET_SO_E          25
#define RM_DLF_MSG_SOCKET_BN_E          26
#define RM_DLF_MSG_RCVD_LS_HDR_E        27
#define RM_DLF_MSG_COMMON_FUNC_E        28
#define RM_DLF_MSG_WRONG_MAGIC          29
#define RM_DLF_MSG_NFNDHOST             30
#define RM_DLF_MSG_RESTART              31
#define RM_DLF_MSG_UNKNOWN_MSG          32
#define RM_DLF_MSG_STRTOOLONG           33
#define RM_DLF_MSG_TESTING_RAM          34
#define RM_DLF_MSG_FS_EMPTY             35
#define RM_DLF_MSG_FS_NAME_START        36 
#define RM_DLF_MSG_FS_NAME_END          37 
#define RM_DLF_MSG_NICK_NAME_START      38 
#define RM_DLF_MSG_NICK_NAME_END        39
#define RM_DLF_MSG_FS_ST_INVALID        40
#define RM_DLF_MSG_ST_WRONG             41
#define RM_DLF_MSG_ST_FORCED            42
#define RM_DLF_MSG_FS_STATE_CHANGED     43
#define RM_DLF_MSG_FS_FSTATE_DISABLED   44
#define RM_DLF_MSG_FS_FORCED_STATE      45
#define RM_DLF_MSG_FS_INITIALIZED       46
#define RM_DLF_MSG_UNKNWN_FLAG          47
#define RM_DLF_MSG_NOT_UPDATED          48
#define RM_DLF_MSG_NCHANGE_STATE        49
#define RM_DLF_MSG_CHANGE_STATE         50
#define RM_DLF_MSG_NJOBS_CHANGE_STATE   51
#define RM_DLF_MSG_JOBS_CHANGE_STATE    52
#define RM_DLF_MSG_ENTERING             53
#define RM_DLF_MSG_LEAVING              54
#define RM_DLF_MSG_LSBINITFAILED        55
#define RM_DLF_MSG_LISTENING_USR        56
#define RM_DLF_MSG_REQUEST_RCVD         57
#define RM_DLF_MSG_JOBID_MISMATCH       58
#define RM_DLF_MSG_JOBSUBMITTED         59
#define RM_DLF_MSG_SLEEPING             60
#define RM_DLF_MSG_MSGPOSTED            61
#define RM_DLF_MSG_JOBRESUMED           62
#define RM_DLF_MSG_TRYDELJOB            63
#define RM_DLF_MSG_TRYCLEANCON          64
#define RM_DLF_MSG_JOB_RECEIVED         65
#define RM_DLF_MSG_JOBSFOUND            66
#define RM_DLF_MSG_LISTENING_LSF        67
#define RM_DLF_MSG_CHILDEXITED          68
#define RM_DLF_MSG_CHILDEXITED_UNCAUGHT 69
#define RM_DLF_MSG_CHILDSTOPPED         70
#define RM_DLF_MSG_CONNECT_E            71
#define RM_DLF_MSG_PROCESSING           72
#define RM_DLF_MSG_CN_FD_ZERO           73
#define RM_DLF_MSG_UNKNWNLSFERR         74
#define RM_DLF_MSG_LSFERR               75
#define RM_DLF_MSG_STARTED_PURGE        76
#define RM_DLF_MSG_JOBNOTFLUSHED        77
#define RM_DLF_MSG_REMOVEJOB            78
#define RM_DLF_MSG_NRMJBS_LT_ZERO       79
#define RM_DLF_MSG_RMJBS_NOT_NULL       80
#define RM_DLF_MSG_CPFN_N_TOOLONG       81
#define RM_DLF_MSG_CPFN_J_TOOLONG       82
#define RM_DLF_MSG_CPFN_J_EMPTY         83
#define RM_DLF_MSG_CPFN_N_EMPTY         84
#define RM_DLF_MSG_CP_DELETE            85
#define RM_DLF_MSG_CP_NEWER_TIME        86
#define RM_DLF_MSG_CP_FILE_TOO_OLD      87
#define RM_DLF_MSG_CP_NBJOBS_LT_ZERO    88
#define RM_DLF_MSG_CP_NBNODES_LT_ZERO   89
#define RM_DLF_MSG_UNKNOWN_HOST         90
#define RM_DLF_MSG_SKIP_MONITOR         91
#define RM_DLF_MSG_SKIP_SURVEY          92
#define RM_DLF_MSG_NODE_NOTUPD          93
#define RM_DLF_MSG_ST_CHANGING          94
#define RM_DLF_MSG_JP_NULL              95
#define RM_DLF_MSG_RFIO_COMMAND_ERROR   96
#define RM_DLF_MSG_RFIO_PCLOSE_ERROR    97
#define RM_DLF_MSG_CANT_FIND_JOB        98
#define RM_DLF_MSG_JOB_WAS_UPDATED      99
#define RM_DLF_MSG_NSURVEY_LT_ZERO     100
#define RM_DLF_MSG_STARTJOB            101

#endif /* __rm_messages_h */
