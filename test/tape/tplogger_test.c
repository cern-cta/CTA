/******************************************************************************************************
 *                                                                                                    *
 * tplogger.c - Castor Tape Logger Test Program                                                       *
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
** $Id: tplogger_test.c,v 1.1 2007/01/18 09:10:17 wiebalck Exp $
*/

#include <errno.h>

#include "tplogger_api.h"

// #define DAEMONIZE

void test_tplogger( tplogger_t *tl ) {

        int        ret, olvl, nlvl, i;
        int        err = 0;
        
	/* Initialize the underlying logging mechanism     */
        printf( "\nInitializing the underlying logging mechanism    " );
        fflush( stdout );
        if( (ret = tl->tl_init( tl, 0 ) < 0 ) ) {

                printf( "Initialization error %d\n", ret );
                err = -1;
                goto err_out;
	}
        printf( " ...done.\n" );

        printf( "Testing tplogger \"%s\"\n", tl->tl_name );
      
#ifdef DAEMONIZE
        tl->tl_fork_prepare( tl );

        ret = fork();
        if( ret < 0 ) {

                fprintf( stderr, "dlfserver: failed to fork() : %s\n", strerror( errno ) );
                return 1;

        } else if( ret > 0 ) {

                return 0;
        }
        
        setsid();
        
        fclose( stdin );
        fclose( stdout );
        fclose( stderr );
        
        tl->tl_fork_child( tl );
#endif
 
        /* Log a message                                   */
        printf( "Logging messages                                  " );
        fflush( stdout );

        //        for( i=0; i<tl->tl_msg_entries; i++ ) {
        for( i=0; i<1; i++ ) {
                Cuuid_t  subreq_id;
                char     tape_name[DLF_LEN_TAPEID] = "Tape21-21";
                U_HYPER  int_64 = ((U_HYPER)(0X12345678) << 32) + 0X87654321;
                if( (ret = tl->tl_log( tl, i, 7, 
                                       "Double", TL_MSG_PARAM_DOUBLE, 12.34, 
                                       "Int"   , TL_MSG_PARAM_INT   , 56,
                                       "TPVID" , TL_MSG_PARAM_TPVID , tape_name,
                                       "String", TL_MSG_PARAM_STR   , "This is a String type message.",
                                       "UUID"  , TL_MSG_PARAM_UUID  , subreq_id,
                                       "Float" , TL_MSG_PARAM_FLOAT , 34.21, 
				       "Int64" , TL_MSG_PARAM_INT64 , int_64
                                       )) < 0 ) {
	      
                        printf( "Logging error %d\n", ret );
                        err = -1;
                        goto err_out;                       
                }
        }
        printf( "...done.\n" );

        /* Change a message's severity                     */
        printf( "Changing a message's severity                     " );
        fflush( stdout );
        if( (ret = tl->tl_get_lvl( tl, 1 )) < 0 ) {

                printf( "Get severity level error before set %d\n", ret );
                err = -1;
                goto err_out;
	}

        olvl = ret;
        nlvl = ++olvl;
        
        if( (ret = tl->tl_set_lvl( tl, 1, nlvl )) < 0 ) {

                printf( "Set severity level error %d\n", ret );
                err = -1;
                goto err_out;
	}

        if( (ret = tl->tl_get_lvl( tl, 1 ) < 0 ) ) {

                printf( "Get severity level error after set %d\n", ret );
                err = -1;
                goto err_out;
	}
        
        if( ret == olvl ) {

                printf( "Severity level not changed %d\n", ret );
                err = -1;
                goto err_out;
        }
        printf( "...done.\n" );

       	
	/* Shutdown the underlying logging mechanism       */
        printf( "Shutting down the underlying logging mechanism    " ); 
        fflush( stdout );
        if( ret = tl->tl_exit( tl, 0 ) ) {

                printf( "Severity level not changed %d\n", ret );
                err = -1;
                goto err_out;                
        }
        printf( "...done.\n" );

 err_out:
        if( 0 == err ) {
                
                printf( "\n--> Passed all tests.\n\n" );

        } else {

                printf( "\n--> There have been problems!\n\n" );
        }
}


int main( void )
{
        tplogger_t tl;
        int err = 0;

        err = tl_init_handle( &tl, "dlf" );
        if( 0 != err ) {
                printf( "unable to init handle: err=%d, tlp=%p\n", err, tl );
                goto err_out;
        }        
        test_tplogger( &tl );

        err = tl_init_handle( &tl, "stdio" );
        if( 0 != err ) {
                printf( "unable to init handle: err=%d, tlp=%p\n", err, tl );
                goto err_out;
        }
        test_tplogger( &tl );

 err_out:
        return err;
}
