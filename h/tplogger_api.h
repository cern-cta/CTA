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
** $Id: tplogger_api.h,v 1.3 2007/01/31 08:45:06 wiebalck Exp $
*/


#ifndef TPLOGGER_H
#define TPLOGGER_H

#include <stdio.h>

#include "dlf_api.h"

/**
 * @file  tplogger_api.h
 * @brief tplogger interface
 */

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

        /* Log a message with ist default level      */
        int (*tl_log)         ( struct tplogger_s *self, unsigned short msg_no, int num_params, ... );

        /* Log a message with a certain log level    */
        int (*tl_llog)        ( struct tplogger_s *self, int lvl, unsigned short msg_no, int num_params, ... ); 

        /* Get the log level of a predefined message */
        int (*tl_get_lvl)     ( struct tplogger_s *self, unsigned short msg_no );

        /* Set the log level of a predefined message */
        int (*tl_set_lvl)     ( struct tplogger_s *self, unsigned short msg_no, int lvl );
        
        /* Shutdown a logging mechanism              */
        int (*tl_exit)        ( struct tplogger_s *self, int exit );

        /* Prepare a call to fork                    */
        int (*tl_fork_prepare)( struct tplogger_s *self );

        /* Fork follow up for the child              */
        int (*tl_fork_child)  ( struct tplogger_s *self );

        /* Fork follow up for the parent             */
        int (*tl_fork_parent) ( struct tplogger_s *self );
        
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

extern tplogger_t tl_tpdaemon;

extern tplogger_t tl_rtcpd;

extern tplogger_t tl_gen;


/*
** Determine the number of elements in an array 
*/
#define ARRAY_ENTRIES(arr) (sizeof(arr))/(sizeof((arr)[0]))

EXTERN_C int DLL_DECL tplogger_nb_messages _PROTO(( tplogger_t *self ));


/*
** DLF prototypes of the tplogger interface
*/
EXTERN_C int DLL_DECL tl_init_dlf         _PROTO(( tplogger_t *self, int init ));

EXTERN_C int DLL_DECL tl_exit_dlf         _PROTO(( tplogger_t *self, int exit ));

EXTERN_C int DLL_DECL tl_log_dlf          _PROTO(( tplogger_t *self, unsigned short msg_no, int num_params, ... ));

EXTERN_C int DLL_DECL tl_llog_dlf         _PROTO(( tplogger_t *self, int sev, unsigned short msg_no, int num_params, ... ));

EXTERN_C int DLL_DECL tl_get_lvl_dlf      _PROTO(( tplogger_t *self, unsigned short msg_no ));

EXTERN_C int DLL_DECL tl_set_lvl_dlf      _PROTO(( tplogger_t *self, unsigned short msg_no, int lvl ));

EXTERN_C int DLL_DECL tl_fork_prepare_dlf _PROTO(( tplogger_t *self ));

EXTERN_C int DLL_DECL tl_fork_child_dlf   _PROTO(( tplogger_t *self ));

EXTERN_C int DLL_DECL tl_fork_parent_dlf  _PROTO(( tplogger_t *self ));


/*
** stdio prototypes of the tplogger interface
*/
EXTERN_C int DLL_DECL tl_init_stdio         _PROTO(( tplogger_t *self, int init ));

EXTERN_C int DLL_DECL tl_exit_stdio         _PROTO(( tplogger_t *self, int exit ));

EXTERN_C int DLL_DECL tl_log_stdio          _PROTO(( tplogger_t *self, unsigned short msg_no, int num_params, ... ));

EXTERN_C int DLL_DECL tl_llog_stdio         _PROTO(( tplogger_t *self, int sev, unsigned short msg_no, int num_params, ... ));

EXTERN_C int DLL_DECL tl_get_lvl_stdio      _PROTO(( tplogger_t *self, unsigned short msg_no ));

EXTERN_C int DLL_DECL tl_set_lvl_stdio      _PROTO(( tplogger_t *self, unsigned short msg_no, int lvl ));

EXTERN_C int DLL_DECL tl_fork_prepare_stdio _PROTO(( tplogger_t *self ));

EXTERN_C int DLL_DECL tl_fork_child_stdio   _PROTO(( tplogger_t *self ));

EXTERN_C int DLL_DECL tl_fork_parent_stdio  _PROTO(( tplogger_t *self ));


/*
** Initialize a tplogger.
**
** Currently supported types: "dlf", "stdio"
*/
EXTERN_C int DLL_DECL tl_init_handle _PROTO(( tplogger_t *self, const char *type ));

#endif  /* TPLOGGER_H */
