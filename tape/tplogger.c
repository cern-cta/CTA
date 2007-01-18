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
** $Id: tplogger.c,v 1.2 2007/01/18 16:38:01 wiebalck Exp $
*/

#include <string.h>
#include <stdlib.h>
#include <stdarg.h>
#include <errno.h>

#include "Cthread_api.h"
#include "tplogger_api.h"

/**
 * @file  tplogger.c
 * @brief Implementations of the tplogger interface
 */

/* mutexes */
static int api_mutex;


/**
 * Initialization of the DLF implementation.
 *
 * @param self : reference to the tplogger struct
 * @param init : initialization switch
 *  init == 0  --> tpdaemon
 *  init == 1  --> rtcpd
 *
 * @returns    : 0 on success
 *               a value < 0 on failure
 */
int DLL_DECL tl_init_dlf( tplogger_t *self, int init ) {

        int   err = 0, rv, i;
        char  error[CA_MAXLINELEN + 1];

        {
                FILE *fp = fopen( "/tmp/init.tst", "w+" );
                fprintf( fp, "in init" );
                fclose( fp );                
        }

        if( NULL == self ) {

                err = -1;
                goto err_out;
        }

        tl_map_defs2dlf();

        switch( init ) {
                
        case 0: /* Facility: tpdaemon */        

                self->tl_name = strdup( "tpdaemon" );  
                
                /* tl_set_msg_tbl_dlf( self, tplogger_messages_tpdaemon ); */
                tl_set_msg_tbl_dlf( self, tplogger_messages );

                break;

        case 1: /* Facility: rtcpd */        

                self->tl_name = strdup( "rtcpd" );    

                /* tl_set_msg_tbl_dlf( self, tplogger_messages_rtcpd ); */
                tl_set_msg_tbl_dlf( self, tplogger_messages );

                break;

        default:
                fprintf( stderr, "dlf_init() - facility unknown\n" );
                err = -2;
                goto err_out;
        }

        rv = dlf_init( self->tl_name, error );
        if (rv != 0) {
                fprintf( stderr, "dlf_init() - %s\n", error );
                free( self->tl_name );
                err = -3;
                goto err_out;
        }

        for( i=0; i<tplogger_nb_messages( self ); i++ ){
                
                rv = dlf_regtext( self->tl_msg[i].tm_no, self->tl_msg[i].tm_txt );
                if (rv < 0) {
                        fprintf(stderr, "Failed to dlf_regtext() %d\n", self->tl_msg[i].tm_no );
                        free(self->tl_name);
                        err = -4;
                        goto err_out;
                }
        }

 err_out:
        return err;
}


/** 
 * Prepare a call to fork                    
 *
 * @param self : reference to the tplogger struct
 * 
 * @returns    : 0 on success
 *               a value < 0 on failure
 */
int tl_fork_prepare_dlf( struct tplogger_s *self ) {

        int err = 0;

        if( NULL == self ) {

                err = -1;
                goto err_out;
        }
        
        dlf_prepare();

 err_out:
        return err;
}


/** 
 * Follow up work to a call to fork for a child                    
 *
 * @param self : reference to the tplogger struct
 * 
 * @returns    : 0 on success
 *               a value < 0 on failure
 */
int tl_fork_child_dlf( struct tplogger_s *self ) {

        int err = 0;

        if( NULL == self ) {

                err = -1;
                goto err_out;
        }
        
        dlf_child();

 err_out:
        return err;
}


/** 
 * Follow up work to a call to fork for a parent 
 *                 
 * @param self : reference to the tplogger struct
 * 
 * @returns    : 0 on success
 *               a value < 0 on failure
 */
int tl_fork_parent_dlf( struct tplogger_s *self ) {

        int err = 0;

        if( NULL == self ) {

                err = -1;
                goto err_out;
        }
        
        dlf_parent();

 err_out:
        return err;
}


/**
 * Exit the DLF implementation.
 *
 * @param self : reference to the tplogger struct
 * @param init : exit switch
 *  exit == 0  --> DLF standard exit
 *
 * @returns    : 0 on success
 *               a value < 0 on failure
 */
int DLL_DECL tl_exit_dlf( tplogger_t *self, int exit ) {

        int err = 0, rv;

        switch( exit ) {
                
        case 0:
                rv = dlf_shutdown( 5 );
                if (rv < 0) {
                        fprintf(stderr, "dlf_shutdown() - failed to shutdown\n");
                        err = -1;
                        goto err_out;
                }               
                break;

        default:
                fprintf( stderr, "dlf_exit() - shutdown code unknown\n" );
                err = -1;
                goto err_out;
        }

        free( self->tl_name );

 err_out:
        return err;
}


/**
 * Check the validity of a messag number
 *
 * @returns    : the index in message table on success
 *               a value < 0 otherwise
 */
static int chk_msg_no( tplogger_t *self, unsigned int msg_no ) {

        int err = -1;
        int i;
        
        /* do we have a valid msg_no? */
        for( i=0; i<self->tl_msg_entries; i++ ) {
                
                if( self->tl_msg[i].tm_no == msg_no ) {
                        
                        return i;
                }
        }

        return err;
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
		if( (plist[i].type == DLF_MSG_PARAM_TPVID) || (plist[i].type == DLF_MSG_PARAM_STR) ) {
			string = va_arg(ap, char *);
			if( string == NULL ) {
				plist[i].par.par_string = NULL;
			} else {
				plist[i].par.par_string = strdup( string );
			}
		}
		else if( plist[i].type == DLF_MSG_PARAM_INT ) {
			plist[i].par.par_int = va_arg(ap, int);
		}
		else if( plist[i].type == DLF_MSG_PARAM_INT64 ) {
			plist[i].par.par_u64 = va_arg(ap, HYPER);
		}
		else if( plist[i].type == DLF_MSG_PARAM_DOUBLE ) {
			plist[i].par.par_double = va_arg(ap, double);
		}
		else if( plist[i].type == DLF_MSG_PARAM_UUID ) {
			plist[i].par.par_uuid = va_arg(ap, Cuuid_t);
		}
		else if( plist[i].type == DLF_MSG_PARAM_FLOAT ) {
			plist[i].par.par_double = va_arg(ap, double);
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
		rv = dlf_writep( req_id, self->tl_msg[ndx].tm_lvl, msg_no, NULL, num_params, plist );
		Cthread_mutex_unlock( &api_mutex );

	} else {
                
                err = -3;
        }

	/* free resources */
	for( i = 0; i < num_params; i++ ) {
		if( plist[i].name != NULL )
			free(plist[i].name);
		if( (plist[i].type == DLF_MSG_PARAM_TPVID) || (plist[i].type == DLF_MSG_PARAM_STR) ) {
			if( plist[i].par.par_string != NULL ) {
				free( plist[i].par.par_string );
			}
		}
	}

 err_out:
	return err;
}


/**
 * Log a message with a specific log level using the DLF implementation. Mostly a copy of dlf_write.
 *
 * @param self       : reference to the tplogger struct
 * @param msg_no     : the message number
 * @param num_params : the number of parameters in the variadic part
 *
 * @returns    : 0 on success
 *               a value < 0 on failure
 */
int DLL_DECL tl_llog_dlf( tplogger_t *self, int lvl, unsigned short msg_no, int num_params, ... ) {

	dlf_write_param_t plist[num_params];
	char              *name;
	char              *string;
	int               i;
	int		  rv;
	int               ok;
        int               ndx = -1;
        int               err = 0;
	va_list           ap;
  	Cuuid_t           req_id;

        if( NULL == self ) {

                err = -1;
                goto err_out;
        }

        /* do we have a valid msg_no? */
        ndx = chk_msg_no( self, msg_no ); 
        if(  ndx < 0 ) {

                err = -2;
                goto err_out;
        }

	/* translate the variable argument list to a dlf_write_param_t array */
       	va_start(ap, num_params);
	for( i = 0, ok = 1; i < num_params; i++ ) {

		string = NULL;
		name   = va_arg(ap, char *);
		if (name == NULL) {
			plist[i].name = NULL;
		} else {
			plist[i].name = strdup( name );
		}
		plist[i].type = va_arg(ap, int );

		/* process type */
		if( (plist[i].type == DLF_MSG_PARAM_TPVID) || (plist[i].type == DLF_MSG_PARAM_STR) ) {
			string = va_arg(ap, char *);
			if( string == NULL ) {
				plist[i].par.par_string = NULL;
			} else {
				plist[i].par.par_string = strdup( string );
			}
		}
		else if( plist[i].type == DLF_MSG_PARAM_INT ) {
			plist[i].par.par_int = va_arg(ap, int);
		}
		else if( plist[i].type == DLF_MSG_PARAM_INT64 ) {
			plist[i].par.par_u64 = va_arg(ap, HYPER);
		}
		else if( plist[i].type == DLF_MSG_PARAM_DOUBLE ) {
			plist[i].par.par_double = va_arg(ap, double);
		}
		else if( plist[i].type == DLF_MSG_PARAM_UUID ) {
			plist[i].par.par_uuid = va_arg(ap, Cuuid_t);
		}
		else if( plist[i].type == DLF_MSG_PARAM_FLOAT ) {
			plist[i].par.par_double = va_arg(ap, double);
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
		rv = dlf_writep( req_id, lvl, msg_no, NULL, num_params, plist );
		Cthread_mutex_unlock( &api_mutex );

	} else {
                
                err = -1;
        }

	/* free resources */
	for( i = 0; i < num_params; i++ ) {
		if( plist[i].name != NULL )
			free( plist[i].name );
		if( (plist[i].type == DLF_MSG_PARAM_TPVID) || (plist[i].type == DLF_MSG_PARAM_STR) ) {
			if( plist[i].par.par_string != NULL ) {
				free( plist[i].par.par_string );
			}
		}
	}

 err_out:
	return err;
}


/**
 * Get the log level of a specific message
 *
 * @param self   : reference to the tplogger struct
 * @param msg_no : the message number
 *
 * @returns      : the log level of the message 
 */
int tl_get_lvl_dlf( tplogger_t *self, unsigned short msg_no ) {

        int err = 0;
        int ndx = -1;

        if( NULL == self ) {

                err = -1;
                goto err_out;
        }

        /* do we have a valid msg_no? */
        ndx = chk_msg_no( self, msg_no ); 
        if(  ndx < 0 ) {

                err = -2;
                goto err_out;
        }

        return self->tl_msg[ndx].tm_lvl;

 err_out:
        return err;
}


/**
 * Set the log level of a specific message
 *
 * @param self   : reference to the tplogger struct
 * @param msg_no : the message number
 * @param lvl    : the new log level
 *
 * @returns      : 0 on success
 *                 <0 on failure
 */
int DLL_DECL tl_set_lvl_dlf( tplogger_t *self, unsigned short msg_no, int lvl ) {

        int err = 0;
        int ndx = -1;

        if( NULL == self ) {

                err = -1;
                goto err_out;
        }

        /* do we have a valid msg_no? */
        ndx = chk_msg_no( self, msg_no ); 
        if(  ndx < 0 ) {

                err = -2;
                goto err_out;
        }

        if( (TL_LVL_MIN > lvl) || (TL_LVL_MAX < lvl) ) {

                err = -3;
                goto err_out;                
        }

        self->tl_msg[ndx].tm_lvl = lvl;        

 err_out:
        return err;
}


/**
 * Map the generic tplogger log levels to DLF log levels 
 */
static void tl_map_defs2dlf( void ) {

#if     TL_LVL_EMERGENCY  != DLF_LVL_EMERGENCY 
#define TL_LVL_EMERGENCY     DLF_LVL_EMERGENCY
#endif
     
#if     TL_LVL_ALERT      != DLF_LVL_ALERT 
#define TL_LVL_ALERT         DLF_LVL_ALERT         
#endif

#if     TL_LVL_ERROR      != DLF_LVL_ERROR 
#define TL_LVL_ERROR         DLF_LVL_ERROR         
#endif

#if     TL_LVL_WARNING    != DLF_LVL_WARNING 
#define TL_LVL_WARNING       DLF_LVL_WARNING       
#endif

#if     TL_LVL_AUTH       != DLF_LVL_AUTH
#define TL_LVL_AUTH          DLF_LVL_AUTH          
#endif

#if     TL_LVL_SECURITY   != DLF_LVL_SECURITY      
#define TL_LVL_SECURITY      DLF_LVL_SECURITY      
#endif

#if     TL_LVL_USAGE      != DLF_LVL_USAGE         
#define TL_LVL_USAGE         DLF_LVL_USAGE         
#endif

#if     TL_LVL_SYSTEM     != DLF_LVL_SYSTEM        
#define TL_LVL_SYSTEM        DLF_LVL_SYSTEM        
#endif

#if     TL_LVL_IMPORTANT  != DLF_LVL_IMPORTANT     
#define TL_LVL_IMPORTANT     DLF_LVL_IMPORTANT     
#endif

#if     TL_LVL_MONITORING != DLF_LVL_MONITORING    
#define TL_LVL_MONITORING    DLF_LVL_MONITORING    
#endif

#if     TL_LVL_DEBUG      != DLF_LVL_DEBUG         
#define TL_LVL_DEBUG         DLF_LVL_DEBUG         
#endif

#if     TL_MSG_PARAM_DOUBLE  != DLF_MSG_PARAM_DOUBLE  
#define TL_MSG_PARAM_DOUBLE     DLF_MSG_PARAM_DOUBLE  
#endif

#if     Tl_MSG_PARAM_INT64   != DLF_MSG_PARAM_INT64   
#define Tl_MSG_PARAM_INT64      DLF_MSG_PARAM_INT64   
#endif

#if     TL_MSG_PARAM_STR     != DLF_MSG_PARAM_STR     
#define TL_MSG_PARAM_STR        DLF_MSG_PARAM_STR     
#endif

#if     TL_MSG_PARAM_TPVID   != DLF_MSG_PARAM_TPVID   
#define TL_MSG_PARAM_TPVID      DLF_MSG_PARAM_TPVID   
#endif

#if     TL_MSG_PARAM_UUID    != DLF_MSG_PARAM_UUID    
#define TL_MSG_PARAM_UUID       DLF_MSG_PARAM_UUID    
#endif

#if     TL_MSG_PARAM_FLOAT   != DLF_MSG_PARAM_FLOAT   
#define TL_MSG_PARAM_FLOAT      DLF_MSG_PARAM_FLOAT   
#endif

#if     TL_MSG_PARAM_INT     != DLF_MSG_PARAM_INT     
#define TL_MSG_PARAM_INT        DLF_MSG_PARAM_INT     
#endif

}


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


/*
** END DLF implementation of the tplogger interface
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

        self->tl_name = strdup( "stdio" );

        switch( init ) {

        case 0:
                /* tl_set_msg_tbl_dlf( self, tplogger_messages_tpdaemon ); */
                tl_set_msg_tbl_dlf( self, tplogger_messages );

                break;

        case 1:
                /* tl_set_msg_tbl_dlf( self, tplogger_messages_rtcpd ); */
                tl_set_msg_tbl_dlf( self, tplogger_messages );

                break;

        default:
                break;
        }

 err_out:
        return err;
}


int DLL_DECL tl_exit_stdio( tplogger_t *self, int exit ) {

        int err = 0;

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

int DLL_DECL tl_llog_stdio( tplogger_t *self, int sev, unsigned short msg_no, int num_params, ... ) {}

int DLL_DECL tl_get_lvl_stdio( tplogger_t *self, unsigned short msg_no ) {}

int DLL_DECL tl_set_lvl_stdio( tplogger_t *self, unsigned short msg_no, int lvl ) {}

int DLL_DECL tl_fork_prepare_stdio( tplogger_t *self ) {}

int DLL_DECL tl_fork_child_stdio( tplogger_t *self ) {}

int DLL_DECL tl_fork_parent_stdio( tplogger_t *self ) {}

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

        if( NULL == self ) {

                err = -1;
                goto err_out;
        }

        /* which implementation?       */
        if( 0 == strcmp( type, "dlf" ) ) {
                
                self->tl_init         = tl_init_dlf;
                self->tl_log          = tl_log_dlf;
                self->tl_llog         = tl_llog_dlf;
                self->tl_get_lvl      = tl_get_lvl_dlf;
                self->tl_set_lvl      = tl_set_lvl_dlf;
                self->tl_fork_prepare = tl_fork_prepare_dlf; 
                self->tl_fork_child   = tl_fork_child_dlf; 
                self->tl_fork_parent  = tl_fork_parent_dlf; 
                self->tl_exit         = tl_exit_dlf;                
                
        } else if( 0 == strcmp( type, "stdio") ) {

                self->tl_init         = tl_init_stdio;
                self->tl_log          = tl_log_stdio;
                self->tl_llog         = tl_llog_stdio;
                self->tl_get_lvl      = tl_get_lvl_stdio;
                self->tl_set_lvl      = tl_set_lvl_stdio;
                self->tl_fork_prepare = tl_fork_prepare_stdio; 
                self->tl_fork_child   = tl_fork_child_stdio; 
                self->tl_fork_parent  = tl_fork_parent_stdio; 
                self->tl_exit         = tl_exit_stdio;                
                
        }  else {

                err = -2;
                goto err_out;
        }

 err_out:
        return err;
}
