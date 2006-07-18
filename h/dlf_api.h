/******************************************************************************************************
 *                                                                                                    *
 * dlf_api.h - Castor Distribution Logging Facility                                                   *
 * Copyright (C) 2005 CERN IT/FIO (castor-dev@cern.ch)                                                *
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

/**
 * @file  dlf_api.h
 * @brief exposes the DLF client API (Application Programming Interface)
 */

#ifndef _DLF_H
#define _DLF_H

/* headers */
#include "Cuuid.h"
#include "Cns_api.h"
#include "osdep.h"

/* severity levels */
#define DLF_LVL_EMERGENCY     1             /**< system is unusable                       */
#define DLF_LVL_ALERT         2             /**< action must be taken immediately         */
#define DLF_LVL_ERROR         3             /**< error conditions                         */
#define DLF_LVL_WARNING       4             /**< warning conditions                       */
#define DLF_LVL_AUTH          5             /**< auth error                               */
#define DLF_LVL_SECURITY      6             /**< Csec error                               */
#define DLF_LVL_USAGE         7             /**< trace of routines                        */
#define DLF_LVL_SYSTEM        8             /**< system error condition                   */
#define DLF_LVL_IMPORTANT     9             /**< informative                              */
#define DLF_LVL_DEBUG         10            /**< debug-level messages                     */

/* parameter types */
#define DLF_MSG_PARAM_DOUBLE  1             /**< double precision floating point value    */
#define DLF_MSG_PARAM_INT64   2             /**< 64 bit integers                          */
#define DLF_MSG_PARAM_STR     3             /**< general purpose string                   */
#define DLF_MSG_PARAM_TPVID   4             /**< tape visual identifier string            */
#define DLF_MSG_PARAM_UUID    5             /**< subrequest identifier                    */
#define DLF_MSG_PARAM_FLOAT   6             /**< single precision floating point value    */
#define DLF_MSG_PARAM_INT     7             /**< integer parameter                        */

/* protocol
 *   - changes these could have significant effects on the database interface layer!!
 */
#define DLF_MAGIC             0x68767001    /**< protocol version                         */

#define DLF_LEN_HOSTNAME      63            /**< same as CA_MAXHOSTNAMELEN                */
#define DLF_LEN_FACNAME       20            /**< max length of a facility name            */
#define DLF_LEN_PARAMNAME     20            /**< max length of a msg parameter name       */
#define DLF_LEN_TIMESTAMP     14            /**< length of a time string                  */
#define DLF_LEN_STRINGVALUE   1024          /**< max length of a string value             */
#define DLF_LEN_NUMBERVALUE   38            /**< max length of a number value             */
#define DLF_LEN_MSGTEXT       512           /**< max length of a message text             */
#define DLF_LEN_TAPEID        20            /**< max length of a tape id                  */

/* limits */
#define DLF_MAX_MSGTEXTS      1024          /**< max number of message texts per facility */

/* errors code */
#define DLF_ERR_PROTO         1             /**< invalid dlf magic, protocol violation    */
#define DLF_ERR_INVALIDREQ    2             /**< invalid request type                     */
#define DLF_ERR_MARSHALL      3             /**< marhsall'ing failure                     */
#define DLF_ERR_DB            4             /**< database error                           */
#define DLF_ERR_SYSERR        5             /**< internal system error                    */
#define DLF_ERR_UNKNOWNFAC    6             /**< unknown facility name                    */
#define DLF_ERR_UNKNOWNTYPE   7             /**< unknown message parameter type           */
#define DLF_ERR_MAX           8             /**< maximum number of error messages         */

/* error messages */
#define DLF_ERR_01            "Invalid protocol version"
#define DLF_ERR_02            "Invalid request type"
#define DLF_ERR_03            "Marshalling error"
#define DLF_ERR_04            "Database error"
#define DLF_ERR_05            "Internal server error"
#define DLF_ERR_06            "Invalid facility name"
#define DLF_ERR_07            "Unknown message parameter type"
#define DLF_ERR_08            "Unknown error"

/* reply types */
#define DLF_REP_ERR           1             /**< error response                           */
#define DLF_REP_RC            2             /**< continue and disconnect                  */
#define DLF_REP_IRC           3             /**< continue, ack, next                      */

/* request types */
#define DLF_INIT              1             /**< initialisation request                   */
#define DLF_LOG               2             /**< log message request                      */


/**
 * opaque structures
 */

typedef struct dlf_write_param_t dlf_write_param_t;


/* structures
 *   - provide for c++ user convenience in castor/dlf/Param.hpp
 */

struct dlf_write_param_t {
	char               *name;          /**< name of the parameter                    */
	int                type;           /**< parameter type, one of DLF_MSG_PAARM_*   */

	/* types */
	union {
		char       *par_string;    /** param of type DLF_PARAM_STRING            */
		int        par_int;        /** param of type DLF_PARAM_INT               */
		U_HYPER    par_u64;        /** param of type DLF_PARAM_INT64             */
		double     par_double;     /** param of type DLF_PARAM_DOUBLE            */
		Cuuid_t    par_uuid;       /** param of type DLF_PARAM_UUID              */
	} par;
};


/**
 * Initialise the DLF interface. Read the castor2 common configuration file "/etc/castor/castor.conf" or
 * the environment variables for DLF and create the necessary data structures for writing to files and
 * remote servers. This function is also responsible for creating the threads for communication between
 * client and server.
 *
 * Once the interface is initialised it cannot be re-initialised without a prior call to dlf_shutdown()
 *
 * @param facility : the name of the facility to initialise
 * @param errptr   : the error text returned from the function on -1, terminated by a '\n'. The error
 *                   buffer must have a minimum size of CA_MAXLINELEN bytes
 *
 * @returns        : 0 on success
 * @returns        : -1 on failure and *errptr will contain the associated error message
 *
 * @note the interface is restricted to API_MAX_TARGETS and API_MAX_THREADS.
 */

EXTERN_C int DLL_DECL dlf_init _PROTO((const char *facility, char *errptr));


/**
 * Shutdown the DLF interface, terminate all threads and reclaim all allocated resources. If the interface
 * is already shutdown then no action will be taken. If a wait time is greater then 0, then the api will
 * wait up to X seconds for the threads to flush there queues to the central server. After this time any 
 * threads left running will have all their data destroyed.
 *
 * @param wait     : the number of seconds to wait for threads to flush there queues before thread
 *                   termination.
 *
 * @returns        : 0 on success
 */

EXTERN_C int DLL_DECL dlf_shutdown _PROTO((int wait));


/**
 * Register a message text for the given facility with the interface. All the message texts must be
 * initialised prior to the dlf_init() call. Message registration after this point is not permitted!
 * No more then DLF_MAX_MSGTEXTS may be registered at any one time.
 *
 * @param msg_no   : the message number
 * @param msg_text : the text associated with the msg_no
 *
 * @returns        : 0 on success
 * @returns        : -1 on failure
 */

EXTERN_C int DLL_DECL dlf_regtext _PROTO((unsigned short msg_no, const char *msg_text));


/**
 * This function writes a log message to the logging targets/destinations specified in the castor2 common
 * configuration file "/etc/castor/castor.conf" or through environment variables.
 *
 * @param reqid      : the unique request identifier associated with the log messages
 * @param severity   : severity level of the message
 * @param msg_no     : the message assigned number
 * @param Cns_fileid : pointer to the structure containing the Castor name server invariant associated
 *                     with the message. This consists of the hostname and file id. If no invariant is
 *                     associated with the message the value should be NULL. The nshostname and nsfileid
 *                     will read "N/A" and 0 respectively
 *
 * @param numparams  : is the number of log message parameters passed
 * @param params     : array of dlf_write_param_t parameters
 *
 *
 * @returns          : 0 on success
 * @returns          : -1 on failure
 *
 * @note when an error occurs it could be because of multiple problems. At the moment the api returns
 * no serrno and errno style return codes. The reason for this is that there is very little the client
 * can do without recompilation to correct the problem. Also as the api is asynchronous with respect to
 * sending messages to a remote host its very difficult to throw an error with an association to a
 * specific message as messages are not uniquely identifiable.
 *
 * The most common errors are using an invalid severity level or attempting to log a message for a
 * message number that has either not be pre registered with the api or is NULL.
 *
 * If the api encounters problems sending messages to the server because of network/transport issues. An
 * error message will be added to the LOGERROR log under the facility DLF. DLF errors written here are
 * not reported to the server. Users should pay particular attention to this category especially when
 * parsing the file with scripts as not all messages in the file maybe associated to one particular
 * facility!
 *
 * @warning although the function may return successfully, this does not mean that the message has been
 * successfully transmitted to the server. The api is asynchronous and depending on the load on the
 * server and the network conditions it could take several minutes for a message to make it to the server
 * and several minutes after that before insertion into the database
 *
 * @see doc/example.c
 */


EXTERN_C int DLL_DECL dlf_writep _PROTO((Cuuid_t reqid,
					 int sevetity,
					 int msg_no,
					 struct Cns_fileid *ns,
					 int numparams,
					 dlf_write_param_t params[]));


/**
 * This function writes a log message to the logging targets/destinations specified in the castor2 common
 * configuration file "/etc/castor/castor.conf" or through environment variables. dlf_write() is a wrapper
 * function around dlf_writep(). It basically takes a variable argument list '...' and converts it to an
 * array of dlf_write_param_t structures which is passed to dlf_writep() along with the other parameters. 
 * All the other arguments and returns values are identical to that of dlf_write()
 *
 * @see dlf_write()
 */

EXTERN_C int DLL_DECL dlf_write _PROTO((Cuuid_t reqid,
					int severity,
					int msg_no,
					struct Cns_fileid *ns,
					int numparams, ...));


/**
 * This function must be called prior to a fork(2) and after a dlf_init() call. It is necessary to ensure
 * that the api's mutexes remain in a valid and known state. The dlf_prepare(), dlf_child() and 
 * dlf_parent() calls effectively provide the same style functionality as pthread_atfork(3) but its
 * specific to the dlf api and portable.
 */

EXTERN_C void DLL_DECL dlf_prepare _PROTO((void));


/**
 * This function should be called from within the child process after a fork(2). It essential reverse the
 * locks imposed by dlf_prepare and recreates all the dlf server threads. The threads must be recreated as
 * fork(2)'ing does not duplicate additional threads other then the main calling thread.
 *
 * @warn failure to call dlf_child after a fork(2) will result in NO logging through the dlf api
 */

EXTERN_C void DLL_DECL dlf_child _PROTO((void));


/**
 * This function should be called from within the parent process after a fork(2). It essentially removes 
 * all locks and allows the api to resume normal logging for the parent
 */

EXTERN_C void DLL_DECL dlf_parent _PROTO((void));


#endif

/** End-of-File **/
