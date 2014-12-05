/******************************************************************************
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
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#pragma once

#include "h/Cuuid.h"
#include "h/Cns_api.h"
#include "h/osdep.h"

#include <syslog.h>

/*---------------------------------------------------------------------------
 * Definitions
 *---------------------------------------------------------------------------*/

/* Priority levels */
#define DLF_LVL_EMERGENCY     LOG_EMERG   /* System is unusable */
#define DLF_LVL_ALERT         LOG_ALERT   /* Action must be taken immediately */
#define DLF_LVL_CRIT          LOG_CRIT    /* Critical conditions */
#define DLF_LVL_ERROR         LOG_ERR     /* Error conditions */
#define DLF_LVL_WARNING       LOG_WARNING /* Warning conditions */
#define DLF_LVL_USER_ERROR    LOG_NOTICE  /* Normal but significant condition */
#define DLF_LVL_AUTH          LOG_NOTICE
#define DLF_LVL_SECURITY      LOG_NOTICE
#define DLF_LVL_SYSTEM        LOG_INFO    /* Informational */
#define DLF_LVL_DEBUG         LOG_DEBUG   /* Debug-level messages */

/* Parameter types */
#define DLF_MSG_PARAM_DOUBLE  1     /* Double precision floating point value */
#define DLF_MSG_PARAM_INT64   2     /* 64 bit integers */
#define DLF_MSG_PARAM_STR     3     /* General purpose string */
#define DLF_MSG_PARAM_TPVID   4     /* Tape visual identifier string */
#define DLF_MSG_PARAM_UUID    5     /* Subrequest identifier */
#define DLF_MSG_PARAM_FLOAT   6     /* Single precision floating point value */
#define DLF_MSG_PARAM_INT     7     /* Integer parameter */
#define DLF_MSG_PARAM_UID     8
#define DLF_MSG_PARAM_GID     9
#define DLF_MSG_PARAM_STYPE   10
#define DLF_MSG_PARAM_SNAME   11
#define DLF_MSG_PARAM_RAW     12    /* raw (set of) parameters, in key=value format */

/* Message number ranges (0-999) reserved for local messages */
#define DLF_BASE_SHAREDMEMORY 1000  /* Shared Memory related code */
#define DLF_BASE_STAGERLIB    1100  /* Stager library related code */
#define DLF_BASE_ORACLELIB    1200  /* Oracle library related code */
#define DLF_BASE_FRAMEWORK    1300  /* Framework related codes */

/* Limits */
#define DLF_MAX_PARAMNAMELEN  20    /* Maximum length of a parameter name */
#define DLF_MAX_PARAMSTRLEN   2048  /* Maximum length of a string value */
#define DLF_MAX_IDENTLEN      20    /* Maximum length of an ident/facility */
#define DLF_MAX_LINELEN       8192  /* Maximum length of a log message */
#define DLF_MAX_MSGTEXTS      8192  /* Maximum number of registered messages */


/*---------------------------------------------------------------------------
 * Structures
 *---------------------------------------------------------------------------*/

/* Provided for C++ users. Refer to: castor/dlf/Param.hpp */
typedef struct {
  char *name;                /* Name of the parameter */
  int  type;                 /* Parameter type, one of DLF_MSG_PARAM_* */
  union {
    char       *par_string;  /* Value for type DLF_PARAM_STRING */
    int        par_int;      /* Value for type DLF_PARAM_INT */
    u_signed64 par_u64;      /* Value for type DLF_PARAM_INT64 */
    double     par_double;   /* Value for type DLF_PARAM_DOUBLE */
    Cuuid_t    par_uuid;     /* Value for type DLF_PARAM_UUID */
  } value;
} dlf_write_param_t;


/*---------------------------------------------------------------------------
 * Prototypes
 *---------------------------------------------------------------------------*/

/* Initialize the DLF logging interface.
 *
 * @param ident   The ident argument is a string that is prepended to every
 *                log message and identifiers the source application.
 *
 * @return On success zero is returned, On error, -1 is returned, and errno is
 *         set appropriately.
 *
 *         Possible errors include:
 *          - EPERM  The interface is already initialized!
 *          - EINVAL Invalid argument (refers to errors in parsing the LogMask
 *                   configuration option in castor.conf for the daemon)
 *
 * @see openlog(), setlogmask()
 */
EXTERN_C int dlf_init (const char *ident);


/* Shutdown the DLF logging interface deallocating all resources.
 * @return On success, zero is returned, On error, -1 is returned, and errno is
 *         set appropriately.
 *
 *         Possible errors include:
 *          - ERERM  The interface not initialized.
 *
 * @see closelog()
 */
EXTERN_C int dlf_shutdown (void);


/* Register a message text with the logging interface. All messages logged by
 * the interface excluding free text parameters must be pre-registered. The
 * maximum number of registered messages is DLF_MAX_MSGTEXTS.
 *
 * The registered message text and number will also be logged to syslog using
 * the LOCAL2/LOG_INFO facility and priority. These messages must be forwarded
 * to the central DLF server so that they can be registered in the DLF
 * database.
 *
 * @param msgno   The number for the message
 * @param message The text associated to the msgno
 *
 * @return On success zero is returned, On error, -1 is returned, and errno is
 *         set appropriately.
 *
 *         Possible errors include:
 *          - ERERM  The interface not initialized.
 *          - EINVAL Invalid argument, one of:
 *                   - The message is NULL
 *                   - The length of the message exceeds DLF_MAX_PARAMSTRLEN
 *                     bytes
 *                   - The msgno is greater than DLF_MAX_MSGTEXTS
 *          - ENOMEM Out of memory
 */
EXTERN_C int dlf_regtext (unsigned int msgno,
			  const char *message);


/* This function writes a log message to syslog using the LOG_LOCAL3 facility.
 * it is the core of the DLF API
 *
 * @param reqid     The unique request identifier associated with the message
 * @param priority  The priority of the message. One of DLF_LVL_*
 * @param msg       The message itself
 * @param ns        A pointer to a structure containing the CASTOR name server
 *                  invariant associated with the message. This consists of the
 *                  name server hostname and file id. If no invariant is
 *                  associated with the message the value should be NULL.
 * @param numparams The number of log message parameters
 * @param params    An array of dlf_write_param_t structures
 * @param tv        The timestamp of the log message
 * @param source    The log source
 * @param sourcepid The log source's pid
 *
 * @return On success zero is returned, On error, -1 is returned, and error is
 *         set appropriately.
 *
 *         Possible errors include:
 *          - ERERM  The interface not initialized.
 *          - ENOMEM Out of memory
 *          - EINVAL Invalid argument, one of:
 *                   - The msgno is greater than DLF_MAX_MSGTEXTS
 *                   - The priority is invalid
 *                   - No message has been pre-registered with the given msgno
 *                   - A parameter of type DLF_MSG_PARAM_UUID is not a valid
 *                     uuid
 *
 * @note The location of where messages are logged to is defined by syslog
 *       configuration.
 * @note Parameter values of data type DLF_MSG_PARAM_STR which are longer than
 *       DLF_MAX_PARAMSTRLEN will be silently truncated.
 * @note Log messages with a length longer than DLF_MAX_LINELEN will be
 *       silently truncated.
 * @see syslog(), dlf_write()
 */
EXTERN_C int dlf_writept (Cuuid_t reqid,
                          unsigned int priority,
                          const char* msg,
                          struct Cns_fileid *ns,
                          unsigned int numparams,
                          dlf_write_param_t params[],
                          struct timeval *tv,
                          const char* source,
                          unsigned int sourcepid);

/* This is a wrapper around dlf_writept
 * The only difference is that this function takes no timestamp and no source.
 * It will log with the current time and use program name and process id of the logging thread
 * This is deprecated
 */
EXTERN_C int dlf_writepm (Cuuid_t reqid,
                          unsigned int priority,
                          char* msg,
                          struct Cns_fileid *ns,
                          unsigned int numparams,
                          dlf_write_param_t params[]);

/* This is a wrapper around dlf_writepm
 * The only difference is that this function takes a message number that will be translated
 * to a string using preregistered messages.
 * This is deprecated
 */
EXTERN_C int dlf_writep (Cuuid_t reqid,
			 unsigned int priority,
			 unsigned int msgno,
			 struct Cns_fileid *ns,
			 unsigned int numparams,
			 dlf_write_param_t params[]);

/* Check to see if the DLF interface has been initialized
 * @returns 1 if the interface is initialized, 0 if not
 * @see dlf_init()
 */
EXTERN_C int dlf_isinitialized (void);



