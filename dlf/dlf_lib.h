/******************************************************************************
 *                      dlf/dlf_lib.h
 *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2003  CERN
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 *
 * @(#)$RCSfile: dlf_lib.h,v $ $Revision: 1.8 $ $Release$ $Date: 2009/08/18 09:42:58 $ $Author: waldron $
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#ifndef _DLF_LIB_H
#define _DLF_LIB_H 1

/* Include files */
#include <ctype.h>
#include <errno.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/syscall.h>
#include <syslog.h>
#include <unistd.h>

#include "Cuuid.h"
#include "Castor_limits.h"
#include "getconfent.h"

/* Defaults */
#define DEFAULT_SYSLOG_MSGLEN  1024 /* Default size of a syslog message  */
#define DEFAULT_RSYSLOG_MSGLEN 2000 /* Defautl size of a rsyslog message */


/*---------------------------------------------------------------------------
 * Structures
 *---------------------------------------------------------------------------*/
static struct {
  char *name;       /* Name of the priority */
  int  value;       /* The priority's numeric representation in syslog */
  char *text;       /* Textual representation of the priority */
} prioritylist[] = {
  { "LOG_EMERG",   LOG_EMERG,   "Emerg"  },
  { "LOG_ALERT",   LOG_ALERT,   "Alert"  },
  { "LOG_CRIT",    LOG_CRIT,    "Crit"   },
  { "LOG_ERR",     LOG_ERR,     "Error"  },
  { "LOG_WARNING", LOG_WARNING, "Warn"   },
  { "LOG_NOTICE",  LOG_NOTICE,  "Notice" },
  { "LOG_INFO",    LOG_INFO,    "Info"   },
  { "LOG_DEBUG",   LOG_DEBUG,   "Debug"  },
  { NULL,          0,           NULL     }
};


#endif /* _DLF_LIB_H */




