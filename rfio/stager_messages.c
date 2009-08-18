/*
 * $Id: stager_messages.c,v 1.3 2009/08/18 09:43:01 waldron Exp $
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
  { STAGER_MSG_ERROR, DLF_LVL_ERROR, LOG_ERR, "ERROR", "error" },
};
