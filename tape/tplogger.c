/******************************************************************************************************
 *                                                                                                    *
 * tplogger.c - Castor Tape Logger Implementations                                                     *
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
** $Id: tplogger.c,v 1.14 2009/08/18 09:43:02 waldron Exp $
*/

#include <string.h>
#include <stdlib.h>
#include <stdarg.h>
#include <errno.h>
#include <syslog.h>
#include <ctype.h>

#include "Cthread_api.h"
#include "tplogger_api.h"

/**
 * @file  tplogger.c
 * @brief Implementations of the tplogger interface
 */

/* mutexes */
static int api_mutex;

/*
** DLF-initialized default tploggers.
*/
tplogger_t tl_tpdaemon = {

        .tl_init         = tl_init_dlf,
        .tl_log          = tl_log_dlf,
        .tl_exit         = tl_exit_dlf
};

tplogger_t tl_rtcpd = {

        .tl_init         = tl_init_dlf,
        .tl_log          = tl_log_dlf,
        .tl_exit         = tl_exit_dlf
};

tplogger_t tl_rmcdaemon = {

        .tl_init         = tl_init_dlf,
        .tl_log          = tl_log_dlf,
        .tl_exit         = tl_exit_dlf
};

/*
** Generic tplogger.
*/
tplogger_t tl_gen;


/**
 * Set the tplogger msg table.
 *
 * @param self   : reference to the tplogger struct
 * @param tl_msg : reference to the message table
 *
 * @returns      : 0 on success
 *                 <0 on failure
 */
static int  tl_set_msg_tbl_dlf( struct tplogger_s *self, tplogger_message_t tl_msg[] ) {

        int err = 0;

        if( NULL == self ) {

                err = -1;
                goto err_out;
        }

        if( NULL == tl_msg ) {

                err = -2;
                goto err_out;
        }

        self->tl_msg         = tl_msg;
        self->tl_msg_entries = tplogger_nb_messages( self );

 err_out:
        return err;
}


/**
 * Initialization of the DLF implementation.
 *
 * @param self : reference to the tplogger struct
 * @param init : initialization switch
 *  init == 0  --> taped
 *  init == 1  --> rtcpd
 *
 * @returns    : 0 on success
 *               a value < 0 on failure
 */
int DLL_DECL tl_init_dlf( tplogger_t *self, int init ) {

        int   err = 0, rv, i;

        if( NULL == self ) {

                err = -1;
                goto err_out;
        }

        switch( init ) {

        case 0: /* Facility: taped */

                self->tl_name = strdup( "taped" );
                tl_set_msg_tbl_dlf( self, tplogger_messages_tpdaemon );

                break;

        case 1: /* Facility: rtcpd */

                self->tl_name = strdup( "rtcpd" );
                tl_set_msg_tbl_dlf( self, tplogger_messages_rtcpd );

                break;

        case 2: /* Facility: rmcd */

                self->tl_name = strdup( "rmcd" );
                tl_set_msg_tbl_dlf( self, tplogger_messages_rmcdaemon );

                break;

        default:
                fprintf( stderr, "dlf_init() - facility unknown\n" );
                err = -2;
                goto err_out;
        }

        rv = dlf_init( self->tl_name, -1 );
        if (rv != 0) {
                fprintf( stderr, "dlf_init() - %s\n", strerror(errno) );
                free( self->tl_name );
                err = -3;
                goto err_out;
        }

        for( i=0; i<tplogger_nb_messages( self ); i++ ){

                rv = dlf_regtext( self->tl_msg[i].tm_no, self->tl_msg[i].tm_txt );
                if (rv < 0) {
                        fprintf(stderr, "Failed to dlf_regtext() %d\n", self->tl_msg[i].tm_no );
                        free( self->tl_name );
                        err = -4;
                        goto err_out;
                }
        }

 err_out:
        return err;
}


/**
 * Exit the DLF implementation.
 *
 * @param self : reference to the tplogger struct
 * @param exit : exit switch
 *  exit == 0  --> DLF standard exit
 *
 * @returns    : 0 on success
 *               a value < 0 on failure
 */
int DLL_DECL tl_exit_dlf( tplogger_t *self, int exit ) {

        int err = 0, rv;

        if( NULL == self ) {

                err = -1;
                goto err_out;
        }

        switch( exit ) {

        case 0:
                rv = dlf_shutdown( );
                if (rv < 0) {
                        fprintf(stderr, "dlf_shutdown() - failed to shutdown\n");
                        free( self->tl_name );
                        err = -2;
                        goto err_out;
                }
                break;

        default:
                fprintf( stderr, "tl_exit_dlf() - shutdown code unknown\n" );
                free( self->tl_name );
                err = -3;
                goto err_out;
        }

        free( self->tl_name );

 err_out:
        return err;
}


/**
 * Check the validity of a message number
 *
 * @returns    : the index in message table on success
 *               a value < 0 otherwise
 */
static int chk_msg_no( tplogger_t *self, unsigned int msg_no ) {

        int err = -1;
        int i;

        /* do we have a valid msg_no? */
        for( i=0; i<self->tl_msg_entries; i++ ) {

          if( self->tl_msg[i].tm_no == (int)msg_no ) {

                        return i;
                }
        }

        return err;
}

/**
 * Map the tplogger log levels to the dlf priorities.
 *
 * @param lvl : the log level to convert
 *
 * @returns   : the dlf priority
 */
static int loglevel_2_dlfpriority(int lvl) {

        int prio;

        switch (lvl) {
        case TL_LVL_EMERGENCY:
                prio = DLF_LVL_EMERGENCY;
                break;
        case TL_LVL_ALERT:
                prio = DLF_LVL_ALERT;
                break;
        case TL_LVL_ERROR:
                prio = DLF_LVL_ERROR;
                break;
        case TL_LVL_WARNING:
                prio = DLF_LVL_WARNING;
                break;
        case TL_LVL_AUTH:
                prio = DLF_LVL_AUTH;
                break;
        case TL_LVL_SECURITY:
                prio = DLF_LVL_SECURITY;
                break;
        case TL_LVL_USAGE:
        case TL_LVL_SYSTEM:
        case TL_LVL_IMPORTANT:
        case TL_LVL_MONITORING:
                prio = DLF_LVL_SYSTEM;
                break;
        case TL_LVL_DEBUG:
                prio = DLF_LVL_DEBUG;
                break;
        default:
                prio = LOG_ERR;
        }
        return prio;
}

/**
 * Log a message using the DLF implementation. Mostly a copy of dlf_write.
 *
 * @param self       : reference to the tplogger struct
 * @param msg_no     : the message number
 * @param num_params : the number of parameters in the variadic part
 *
 * @returns    : 0 on success
 *               a value < 0 on failure
 */
int DLL_DECL tl_log_dlf( tplogger_t *self, unsigned short msg_no, int num_params, ... ) {

	dlf_write_param_t plist[num_params];
	char              *name;
	char              *string;
	int               i;
	int		  rv;
	int               ok;
        int               err = 0;
        int               ndx = -1;
        int               prio = 0;
	va_list           ap;
  	Cuuid_t           req_id;

        if( NULL == self ) {

                err = -1;
                goto err_out;
        }

        ndx = chk_msg_no( self, msg_no );
        if(  ndx < 0 ) {

                err = -2;
                goto err_out;
        }

        /* convert the log level to a dlf priority */
        prio = loglevel_2_dlfpriority(self->tl_msg[ndx].tm_lvl);

	/* translate the variable argument list to a dlf_write_param_t array */
       	va_start(ap, num_params);
	for( i = 0, ok = 1; i < num_params; i++ ) {

		string = NULL;
		name   = va_arg(ap, char *);
		if( name == NULL ) {
			plist[i].name = NULL;
		} else {
			plist[i].name = strdup( name );
		}
		plist[i].type = va_arg(ap, int);

		/* process type */
		if( (plist[i].type == DLF_MSG_PARAM_TPVID) || (plist[i].type == DLF_MSG_PARAM_STR) ||
                    (plist[i].type == DLF_MSG_PARAM_STYPE) || (plist[i].type == DLF_MSG_PARAM_SNAME) ) {

			string = va_arg(ap, char *);
			if( string == NULL ) {
				plist[i].value.par_string = NULL;
			} else {
				plist[i].value.par_string = strdup( string );
			}
		}
		else if( (plist[i].type == DLF_MSG_PARAM_INT) ||
			 (plist[i].type == DLF_MSG_PARAM_UID) ||
			 (plist[i].type == DLF_MSG_PARAM_GID) ){

			plist[i].value.par_int = va_arg(ap, int);
		}
		else if( plist[i].type == DLF_MSG_PARAM_INT64 ) {
			plist[i].value.par_u64 = va_arg(ap, HYPER);
		}
		else if( plist[i].type == DLF_MSG_PARAM_DOUBLE ) {
			plist[i].value.par_double = va_arg(ap, double);
		}
		else if( plist[i].type == DLF_MSG_PARAM_UUID ) {
			plist[i].value.par_uuid = va_arg(ap, Cuuid_t);
		}
		else if( plist[i].type == DLF_MSG_PARAM_FLOAT ) {
			plist[i].value.par_double = va_arg(ap, double);
			plist[i].type = DLF_MSG_PARAM_DOUBLE;
		}
		else {
			ok = 0;
			break;
		}
	}
	va_end(ap);

	if (ok == 1) {

                Cthread_mutex_lock( &api_mutex );
		rv = dlf_writep( req_id, prio, msg_no, NULL, num_params, plist );
		Cthread_mutex_unlock( &api_mutex );

	} else {

                err = -3;
        }

	/* free resources */
	for( i = 0; i < num_params; i++ ) {
		if( plist[i].name != NULL )
			free(plist[i].name);
		if( (plist[i].type == DLF_MSG_PARAM_TPVID) || (plist[i].type == DLF_MSG_PARAM_STR) ) {
			if( plist[i].value.par_string != NULL ) {
				free( plist[i].value.par_string );
			}
		}
	}

 err_out:
	return err;
}


/*
** END DLF implementation of the tplogger interface
*/


/*
**   syslog implementation of the tplogger interface
*/

int DLL_DECL tl_init_syslog( tplogger_t *self, int init ) {

        int err = 0;

        if( NULL == self ) {

                err = -1;
                goto err_out;
        }

        switch( init ) {

        case 0:
                self->tl_name = strdup( "taped" );
                tl_set_msg_tbl_dlf( self, tplogger_messages_tpdaemon );
                openlog("taped", LOG_PID, LOG_LOCAL0);
                break;

        case 1:
                self->tl_name = strdup( "rtcpd" );
                tl_set_msg_tbl_dlf( self, tplogger_messages_rtcpd );
                openlog("rtcpd", LOG_PID, LOG_LOCAL1);
                break;

        case 2:
                self->tl_name = strdup( "rmcd" );
                tl_set_msg_tbl_dlf( self, tplogger_messages_rmcdaemon );
                openlog("rmcd", LOG_PID, LOG_LOCAL1);
                break;

        default:
                break;
        }

 err_out:
        return err;
}


int DLL_DECL tl_exit_syslog( tplogger_t *self, int exit ) {

        int err = 0;

        (void)exit;

        if( NULL == self ) {

                err = -1;
                goto err_out;
        }
        closelog();
        free( self->tl_name );

 err_out:
        return err;
}

/**
 * Map the tplogger log levels to the syslogd priorities.
 *
 * @param lvl : the log level to convert
 *
 * @returns   : the syslogd priority
 */
static int loglevel_2_syslogpriority(int lvl) {

        int prio;

        switch (lvl) {

        case TL_LVL_EMERGENCY:
                prio = LOG_EMERG;
                break;
        case TL_LVL_ALERT:
                prio = LOG_ALERT;
                break;
        case TL_LVL_ERROR:
                prio = LOG_ERR;
                break;
        case TL_LVL_WARNING:
                prio = LOG_WARNING;
                break;
        case TL_LVL_AUTH:
        case TL_LVL_SECURITY:
        case TL_LVL_USAGE:
                prio = LOG_CRIT;
                break;
        case TL_LVL_IMPORTANT:
                prio = LOG_NOTICE;
                break;
        case TL_LVL_SYSTEM:
        case TL_LVL_MONITORING:
                prio = LOG_INFO;
                break;
        case TL_LVL_DEBUG:
                prio = LOG_DEBUG;
                break;
        default:
                prio = LOG_ERR;
        }
        return prio;
}

/**
 * Convert to upper string
 *
 * @param str : pointer to the string to be converted
 *
 * @returns   : 0 on success
 *             -1 otherwise
 */
static int convert2upper(char *str) {

        unsigned int i;

        if (NULL == str) {
                return -1;
        }

        for (i=0; i<strlen(str); i++) {
                str[i] = (char)toupper((int)str[i]);
        }

        return 0;
}

/**
 * Remove trailing blanks
 *
 * @param str : pointer to the string to be trimmed
 *
 * @returns   : pointer to str
 */
static char *trimString(char *str) {

        int last = strlen(str)-1;

        while ((last>=0) && isspace((int)str[last])) {
                str[last] = '\0';
                last--;
        }

        return str;
}

/**
 * Log a message using the syslog implementation.
 *
 * @param self       : reference to the tplogger struct
 * @param msg_no     : the message number
 * @param num_params : the number of parameters in the variadic part
 *
 * @returns    : 0 on success
 *               a value < 0 on failure
 */
int DLL_DECL tl_log_syslog( tplogger_t *self, unsigned short msg_no, int num_params, ... ) {

        int err = 0, i, ndx = -1, prio, strndx, type;
	va_list ap;
        char    msg[MAX_SYSLOG_MSG_LEN];
        char    syslogmsg[MAX_SYSLOG_CHUNK+1+64];
        char   *name, *string;
        int     par_int;
        double  par_double;
        U_HYPER par_u64;
        Cuuid_t par_uuid;

        if( NULL == self ) {

                err = -1;
                goto err_out;
        }

        /* translate number to index */
        for (i=0; i<self->tl_msg_entries; i++) {
                if (self->tl_msg[i].tm_no == msg_no) {
                        ndx = i;
                        break;
                }
        }

        /* check if message found */
        if (-1 == ndx) {
                syslog(LOG_ERR, "Did not find message %d!", msg_no);
                err = -2;
                goto err_out;
        }

        /* convert the log level to a syslog priority */
        prio = loglevel_2_syslogpriority(self->tl_msg[ndx].tm_lvl);

        /* insert the message */
        strndx = snprintf(msg, MAX_SYSLOG_MSG_LEN-1, "\"TYPE\"=\"%s\", ", self->tl_msg[ndx].tm_txt);

        /* translate the variable argument list to a string */
       	va_start(ap, num_params);
	for( i = 0; i<num_params; i++ ) {

                /* get the name */
		name = va_arg(ap, char *);
		if (name == NULL) {
                        if (strndx < (MAX_SYSLOG_MSG_LEN-2)) {
                                strndx += snprintf(msg + strndx, MAX_SYSLOG_MSG_LEN-strndx-1, "\"%s\"=", "(nil)");
                        }
		} else {
                        char *name2 = strdup(name);
                        if (!convert2upper(name2)) {
                                if (strndx < (MAX_SYSLOG_MSG_LEN-2)) {
                                        strndx += snprintf(msg + strndx, MAX_SYSLOG_MSG_LEN-strndx-1, "\"%s\"=", name2);
                                }
                        } else {
                                if (strndx < (MAX_SYSLOG_MSG_LEN-2)) {
                                        strndx += snprintf(msg + strndx, MAX_SYSLOG_MSG_LEN-strndx-1, "\"%s\"=", name);
                                }
                        }
                        free(name2);
		}

                /* process type */
                type = va_arg(ap, int);
		if ((type == TL_MSG_PARAM_TPVID) || (type == TL_MSG_PARAM_STR)) {
			string = va_arg(ap, char *);
			if (string == NULL) {

                                if (strndx < (MAX_SYSLOG_MSG_LEN-2)) {
                                        strndx += snprintf(msg + strndx, MAX_SYSLOG_MSG_LEN-strndx-1, "\"%s\", ", "(nil)");
                                }
			} else {
                                char *string2 = strdup(string);
                                if (strndx < (MAX_SYSLOG_MSG_LEN-2)) {
                                        strndx += snprintf(msg + strndx, MAX_SYSLOG_MSG_LEN-strndx-1, "\"%s\", ", trimString(string2));
                                }
                                free(string2);
			}

		} else if ((type == TL_MSG_PARAM_INT) ||
                           (type == TL_MSG_PARAM_UID) ||
                           (type == TL_MSG_PARAM_GID)) {
                        par_int = va_arg(ap, int);
                        if (strndx < (MAX_SYSLOG_MSG_LEN-2)) {
                                strndx += snprintf(msg + strndx, MAX_SYSLOG_MSG_LEN-strndx-1, "\"%d\", ", par_int);
                        }

		} else if (type == TL_MSG_PARAM_INT64) {
                        par_u64 = va_arg(ap, HYPER);
                        if (strndx < (MAX_SYSLOG_MSG_LEN-2)) {
                                strndx += snprintf(msg + strndx, MAX_SYSLOG_MSG_LEN-strndx-1, "\"%lld\", ", par_u64);
                        }

		} else if (type == TL_MSG_PARAM_DOUBLE) {
                        par_double = va_arg(ap, double);
                        if (strndx < (MAX_SYSLOG_MSG_LEN-2)) {
                                strndx += snprintf(msg + strndx, MAX_SYSLOG_MSG_LEN-strndx-1, "\"%f\", ", par_double);
                        }

		} else if (type == TL_MSG_PARAM_UUID) {
                        char tmp[CUUID_STRING_LEN];
                        par_uuid = va_arg(ap, Cuuid_t);
                        Cuuid2string(tmp, CUUID_STRING_LEN, &par_uuid);
                        if (strndx < (MAX_SYSLOG_MSG_LEN-2)) {
                                strndx += snprintf(msg + strndx, MAX_SYSLOG_MSG_LEN-strndx-1, "\"%s\", ", tmp);
                        }

		} else if (type == TL_MSG_PARAM_FLOAT) {
                        par_double = va_arg(ap, double);
                        if (strndx < (MAX_SYSLOG_MSG_LEN-2)) {
                                strndx += snprintf(msg + strndx, MAX_SYSLOG_MSG_LEN-strndx-1, "\"%f\", ", par_double);
                        }

		} else {

			break;
		}
	}
	va_end(ap);

        /* remove the trailing comma */
        msg[strlen(msg)-2] = '\0';

        /* pass message in (MAX_SYSLOG_CHUNK+1) byte blocks to syslog */
        ndx = 0;
        while (ndx < (int)strlen(msg)) {

                /* print a chunk or the rest */
                int len = ((strlen(msg)-ndx)>MAX_SYSLOG_CHUNK) ? MAX_SYSLOG_CHUNK : (strlen(msg)-ndx);
                snprintf(syslogmsg, len+1, "%s", msg+ndx);
                if (ndx>0) {
                        syslog(prio, "(cont) %s", syslogmsg);
                } else {
                        syslog(prio, "%s", syslogmsg);
                }
                ndx += len;
        }

 err_out:
        return err;
}


/*
** END Syslog implementation of the tplogger interface
*/


/*
**   Stdio implementation of the tplogger interface
**
** !! only for testing, writes to /tmp/tplogger.log !!
*/

int DLL_DECL tl_init_stdio( tplogger_t *self, int init ) {

        int err = 0;

        if( NULL == self ) {

                err = -1;
                goto err_out;
        }

        self->out_stream = fopen("/tmp/tplogger.log", "a");

        if( NULL == self->out_stream ){

                err = -2;
                goto err_out;
        }

        switch( init ) {

        case 0:
                self->tl_name = strdup( "taped" );
                tl_set_msg_tbl_dlf( self, tplogger_messages_tpdaemon );

                break;

        case 1:
                self->tl_name = strdup( "rtcpd" );
                tl_set_msg_tbl_dlf( self, tplogger_messages_rtcpd );

        case 2:
                self->tl_name = strdup( "rmcd" );
                tl_set_msg_tbl_dlf( self, tplogger_messages_rmcdaemon );

                break;

        default:
                break;
        }

 err_out:
        return err;
}


int DLL_DECL tl_exit_stdio( tplogger_t *self, int exit ) {

        int err = 0;

        (void)exit;

        if( NULL == self ) {

                err = -1;
                goto err_out;
        }

        fclose( self->out_stream );

        free( self->tl_name );

 err_out:
        return err;
}


/*
** Dummy function; only extracts the message text, no parameter parsing.
*/
int DLL_DECL tl_log_stdio( tplogger_t *self, unsigned short msg_no, int num_params, ... ) {

        int err = 0;

        (void) num_params;

        if( NULL == self ) {

                err = -1;
                goto err_out;
        }

        if( (self->tl_msg_entries - 1) < msg_no ) {

                err = -2;
                goto err_out;
        }

        fprintf( self->out_stream, "--> %s\n", self->tl_msg[msg_no].tm_txt );

 err_out:
        return err;
}


/*
** END Stdio implementation of the tplogger interface
*/


/**
 * Initialize a tplogger handle
 *
 * @param self : ptr to empty tplogger struct
 * @param type : implementation (currently 'dlf' or 'stdio' )
 *
 * @returns      : 0 on success
 *                 <0 on failure
 */
int tl_init_handle( tplogger_t *self, const char *type ) {

        int err = 0;

        /* which implementation?       */
        if( 0 == strcmp( type, "dlf" ) ) {

                self->tl_init         = tl_init_dlf;
                self->tl_log          = tl_log_dlf;
                self->tl_exit         = tl_exit_dlf;

        } else if( 0 == strcmp( type, "syslog") ) {

                self->tl_init         = tl_init_syslog;
                self->tl_log          = tl_log_syslog;
                self->tl_exit         = tl_exit_syslog;

        } else if( 0 == strcmp( type, "stdio") ) {

                self->tl_init         = tl_init_stdio;
                self->tl_log          = tl_log_stdio;
                self->tl_exit         = tl_exit_stdio;

        }  else {

                err = -2;
                goto err_out;
        }

 err_out:
        return err;
}
