/******************************************************************************************************
 *                                                                                                    *
 * tplogger.h - Castor Tape Logger Interface                                                          *
 * Copyright (C) 2006 CERN IT/FIO (castor-dev@cern.ch)                                                *
 *                                                                                                    *
 * This program is free software; you can redistribute it and/or modify it under the terms of the GNU *
 * General Public License as published by the Free Software Foundation; either version 2 of the       *
 * License, or (at your option) any later version.                                                    *
 *                                                                                                    *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without  *
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU     *
 * General Public License for more details.                                                           *
 *                                                                                                    *
 * You should have received a copy of the GNU General Public License along with this program; if not, *
 * write to the Free Software Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307,  *
 * USA.                                                                                               *
 *                                                                                                    *
 ******************************************************************************************************/


/*
** $Id: tplogger_api.h,v 1.7 2009/08/18 09:43:00 waldron Exp $
*/


#ifndef TPLOGGER_H
#define TPLOGGER_H

#include <stdio.h>

#include "dlf_api.h"

/**
 * @file  tplogger_api.h
 * @brief tplogger interface
 */

/* Required by tplogger */
#define DLF_LEN_MSGTEXT       512


/*
** The tape logger message
*/
typedef struct tplogger_message_s {

        /* The message number                        */
        int  tm_no;

        /* The default log level of a message        */
        int  tm_lvl;

        /* The message text                          */
        char tm_txt[DLF_LEN_MSGTEXT+1];

} tplogger_message_t;


/*
** The tape logger interface
*/
typedef struct tplogger_s {

        /* Initalize a logging mechanism             */
        int (*tl_init)        ( struct tplogger_s *self, int init );

        /* Log a message with its default level      */
        int (*tl_log)         ( struct tplogger_s *self, unsigned short msg_no, int num_params, ... );

        /* Shutdown a logging mechanism              */
        int (*tl_exit)        ( struct tplogger_s *self, int exit );

        /* A tplogger identifier                     */
        char               *tl_name;

        /* Output file stream                        */
        FILE               *out_stream;

        /* Message table                             */
        tplogger_message_t *tl_msg;

        /* Message table entries                     */
        int                 tl_msg_entries;

} tplogger_t;


/* Severity levels, 'inspired' by DLF */
#define TL_LVL_EMERGENCY     1                 /**< system is unusable                       */
#define TL_LVL_ALERT         2                 /**< action must be taken immediately         */
#define TL_LVL_ERROR         3                 /**< error conditions                         */
#define TL_LVL_WARNING       4                 /**< warning conditions                       */
#define TL_LVL_AUTH          5                 /**< auth error                               */
#define TL_LVL_SECURITY      6                 /**< Csec error                               */
#define TL_LVL_USAGE         7                 /**< trace of routines                        */
#define TL_LVL_SYSTEM        8                 /**< system error condition                   */
#define TL_LVL_IMPORTANT     9                 /**< informative                              */
#define TL_LVL_MONITORING    10                /**< monitoring and statistics                */
#define TL_LVL_DEBUG         11                /**< debug-level messages                     */

#define TL_LVL_MIN           TL_LVL_EMERGENCY
#define TL_LVL_MAX           TL_LVL_DEBUG


/* Parameter types, 'inspired' by DLF */
#define TL_MSG_PARAM_DOUBLE   1                 /**< double precision floating point value    */
#define TL_MSG_PARAM_INT64    2                 /**< 64 bit integers                          */
#define TL_MSG_PARAM_STR      3                 /**< general purpose string                   */
#define TL_MSG_PARAM_TPVID    4                 /**< tape visual identifier string            */
#define TL_MSG_PARAM_UUID     5                 /**< subrequest identifier                    */
#define TL_MSG_PARAM_FLOAT    6                 /**< single precision floating point value    */
#define TL_MSG_PARAM_INT      7                 /**< integer parameter                        */
#define TL_MSG_PARAM_UID      8                 /**< user id                                  */
#define TL_MSG_PARAM_GID      9                 /**< group id                                 */
#define TL_MSG_PARAM_STYPE   10                 /**< e.g KRB5, GSI                            */
#define TL_MSG_PARAM_SNAME   11                 /**< e.g DN, Krb principal                    */


extern tplogger_message_t tplogger_messages[];

extern tplogger_message_t tplogger_messages_tpdaemon[];

extern tplogger_message_t tplogger_messages_rtcpd[];

extern tplogger_message_t tplogger_messages_rmcdaemon[];

extern tplogger_t tl_tpdaemon;

extern tplogger_t tl_rtcpd;

extern tplogger_t tl_rmcdaemon;

extern tplogger_t tl_gen;


/*
** Determine the number of elements in an array
*/
#define ARRAY_ENTRIES(arr) (sizeof(arr))/(sizeof((arr)[0]))

EXTERN_C int tplogger_nb_messages ( tplogger_t *self );


/*
** DLF prototypes of the tplogger interface
*/

/* #define MAX_SYSLOG_MSG_LEN 1024 */
#define MAX_SYSLOG_MSG_LEN 1048576

/* chunk size -1 passed to syslog  */
#define MAX_SYSLOG_CHUNK 767

EXTERN_C int tl_init_dlf         ( tplogger_t *self, int init );

EXTERN_C int tl_exit_dlf         ( tplogger_t *self, int exit );

EXTERN_C int tl_log_dlf          ( tplogger_t *self, unsigned short msg_no, int num_params, ... );


/*
** syslog prototypes of the tplogger interface
*/
EXTERN_C int tl_init_syslog         ( tplogger_t *self, int init );

EXTERN_C int tl_exit_syslog         ( tplogger_t *self, int exit );

EXTERN_C int tl_log_syslog          ( tplogger_t *self, unsigned short msg_no, int num_params, ... );


/*
** stdio prototypes of the tplogger interface
*/
EXTERN_C int tl_init_stdio         ( tplogger_t *self, int init );

EXTERN_C int tl_exit_stdio         ( tplogger_t *self, int exit );

EXTERN_C int tl_log_stdio          ( tplogger_t *self, unsigned short msg_no, int num_params, ... );


/*
** Initialize a tplogger.
**
** Currently supported types: "dlf", "stdio"
*/
EXTERN_C int tl_init_handle ( tplogger_t *self, const char *type );

#endif  /* TPLOGGER_H */
