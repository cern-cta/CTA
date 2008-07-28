/*
 * $Id: stager_messages.c,v 1.1 2008/07/28 16:23:57 waldron Exp $
 */

/*
 * Copyright (C) 2004 by CERN/IT/ADC/CA
 * All rights reserved
 */

/* ============== */
/* System headers */
/* ============== */
#include <sys/types.h>

/* ============= */
/* Local headers */
/* ============= */
#include "stager_messages.h"
#include "stager_macros.h"          /* For STAGER_NB_ELEMENTS */
#include "log.h"

struct stagerMessages stagerMessages[] = {
  { STAGER_MSG_EMERGENCY, DLF_LVL_EMERGENCY,  LOG_EMERG, "EMERGENCY", "emergency"},

  { STAGER_MSG_ALERT    , DLF_LVL_ALERT    ,  LOG_ALERT, "ALERT"    , "alert"},

  { STAGER_MSG_ERROR    , DLF_LVL_ERROR    ,  LOG_ERR, "ERROR"    , "error"},
  { STAGER_MSG_SYSCALL  , DLF_LVL_ERROR    ,  LOG_ERR, "ERROR"    , "system call error"},

  { STAGER_MSG_WARNING  , DLF_LVL_WARNING  ,  LOG_WARNING, "WARNING"  , "warning"},

  { STAGER_MSG_AUTH     , DLF_LVL_AUTH     ,  LOG_ERR, "AUTH"     , "authentication"},

  { STAGER_MSG_SECURITY , DLF_LVL_SECURITY,   LOG_ERR, "SECURITY" , "security"},

  { STAGER_MSG_USAGE    , DLF_LVL_USAGE    ,  LOG_INFO, "USAGE"    , "usage"},
  { STAGER_MSG_ENTER    , DLF_LVL_USAGE    ,  LOG_INFO, "USAGE"    , "function enter"},
  { STAGER_MSG_LEAVE    , DLF_LVL_USAGE    ,  LOG_INFO, "USAGE"    , "function leave"},
  { STAGER_MSG_RETURN   , DLF_LVL_USAGE    ,  LOG_INFO, "USAGE"    , "function return"},

  { STAGER_MSG_SYSTEM   , DLF_LVL_SYSTEM   ,  LOG_INFO, "SYSTEM"   , "system"},
  { STAGER_MSG_STARTUP  , DLF_LVL_SYSTEM   ,  LOG_INFO, "SYSTEM"   , "stager server started"},

  { STAGER_MSG_IMPORTANT, DLF_LVL_IMPORTANT,  LOG_CRIT, "IMPORTANT", "important"},

  { STAGER_MSG_DEBUG    , DLF_LVL_DEBUG    ,  LOG_DEBUG, "DEBUG"    , "debug"},

  { STAGER_MSG_EXIT     , DLF_LVL_SYSTEM   ,  LOG_INFO, "SYSTEM"   , "stager server exiting"},

  { STAGER_MSG_RESTART  , DLF_LVL_SYSTEM   ,  LOG_INFO, "SYSTEM"   , "stager server restart"},

  { STAGER_MSG_FQUERY   , DLF_LVL_SYSTEM   ,  LOG_INFO, "SYSTEM"   , "Processing File Query by fileName"},

  { STAGER_MSG_RQUERY   , DLF_LVL_SYSTEM   ,  LOG_INFO, "SYSTEM"   , "Processing Request Query"},

  { STAGER_MSG_IQUERY   , DLF_LVL_SYSTEM   ,  LOG_INFO, "SYSTEM"   , "Processing File Query by fileId"}

};

int DLL_DECL stager_messagesNbElements() {
  return(STAGER_NB_ELEMENTS(stagerMessages));
}

