/*
 * $Id: stagerMessages.h,v 1.1 2004/10/19 18:16:43 jdurand Exp $
 */

#ifndef __stagerMessages_h
#define __stagerMessages_h

#include "dlf_api.h"

#define STAGER_FACILITY_NAME "stager"
#define STAGER_LOG_WHERE "File",DLF_MSG_PARAM_STR,__FILE__,"Line",DLF_MSG_PARAM_INT,__LINE__,"errno",DLF_MSG_PARAM_INT,errno,"serrno",DLF_MSG_PARAM_INT,serrno
#define STAGER_NB_PARAMS 4 /* This is the number of arguments in STAGER_LOG_WHERE */

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
struct stagerMessages stagerMessages[] = {
  { STAGER_MSG_EMERGENCY, DLF_LVL_EMERGENCY,  "EMERGENCY", "emergency"},

  { STAGER_MSG_ALERT    , DLF_LVL_ALERT    ,  "ALERT"    , "alert"},

  { STAGER_MSG_ERROR    , DLF_LVL_ERROR    ,  "ERROR"    , "error"},
  { STAGER_MSG_SYSCALL  , DLF_LVL_ERROR    ,  "ERROR"    , "system call error"},

  { STAGER_MSG_WARNING  , DLF_LVL_WARNING  ,  "WARNING"  , "warning"},

  { STAGER_MSG_AUTH     , DLF_LVL_AUTH     ,  "AUTH"     , "authentication"},

  { STAGER_MSG_SECURITY , DLF_LVL_SECURITY,   "SECURITY" , "security"},

  { STAGER_MSG_USAGE    , DLF_LVL_USAGE    ,  "USAGE"    , "usage"},
  { STAGER_MSG_ENTER    , DLF_LVL_USAGE    ,  "USAGE"    , "function enter"},
  { STAGER_MSG_LEAVE    , DLF_LVL_USAGE    ,  "USAGE"    , "function leave"},
  { STAGER_MSG_RETURN   , DLF_LVL_USAGE    ,  "USAGE"    , "function return"},

  { STAGER_MSG_SYSTEM   , DLF_LVL_SYSTEM   ,  "SYSTEM"   , "system"},
  { STAGER_MSG_STARTUP  , DLF_LVL_SYSTEM   ,  "SYSTEM"   , "stager server started"},

  { STAGER_MSG_IMPORTANT, DLF_LVL_IMPORTANT,  "IMPORTANT", "important"},

  { STAGER_MSG_DEBUG    , DLF_LVL_DEBUG    ,  "DEBUG"    , "debug"},

  { STAGER_MSG_EXIT     , DLF_LVL_SYSTEM   ,  "SYSTEM"   , "stager server exiting"},

  { STAGER_MSG_RESTART  , DLF_LVL_SYSTEM   ,  "SYSTEM"   , "stager server restart"}
};

#endif /* __stagerMessages_h */
