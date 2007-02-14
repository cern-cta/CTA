/******************************************************************************************************
 *                                                                                                    *
 * tplogger_messages.c - Castor Tape Logger Messages                                                  *
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
** $Id: tplogger_messages.c,v 1.4 2007/02/14 09:31:51 wiebalck Exp $
*/

#include <string.h>

#include "tplogger_api.h" 

/**
 * @file  tplogger_messages.c
 * @brief predefined messages for tpdaemon; mainly taken from h/Ctape.h
 */

/*
** The messages struct.
** The comments contain some information on what parameters are to be added.
*/
tplogger_message_t tplogger_messages[] = {
        
        {   0, TL_LVL_ERROR,  "TP000 - tape daemon not available"               },  /* host      */
        {   1, TL_LVL_ERROR,  "TP001 - no response from tape daemon"            },
        {   2, TL_LVL_ERROR,  "TP002 - error"                                   },  /* error     */                 
        {   3, TL_LVL_ERROR,  "TP003 - illegal function"                        },  /* function  */
        {   4, TL_LVL_ERROR,  "TP004 - error getting request"                   },  /* netread   */
        {   5, TL_LVL_ERROR,  "TP005 - cannot get memory"                       },
        {   6, TL_LVL_ERROR,  "TP006 - invalid value"                           },  /* value name*/
        {   7, TL_LVL_ERROR,  "TP007 - fid is mandatory when TPPOSIT_FID"       },
        {   8, TL_LVL_ERROR,  "TP008 - not accessible"                          },  /* what      */
        {   9, TL_LVL_ERROR,  "TP009 - could not configure"                     },  /* what, why */
        {  10, TL_LVL_ERROR,  "TP010 - resources already reserved for this job" },
        {  11, TL_LVL_ERROR,  "TP011 - too many tape users"                     },
        {  12, TL_LVL_ERROR,  "TP012 - too many drives requested"               },
        {  13, TL_LVL_ERROR,  "TP013 - invalid device group name"               },
        {  14, TL_LVL_ERROR,  "TP014 - reserve not done"                        },
        {  15, TL_LVL_ERROR,  "TP015 - drive with specified name/characteristics does not exist" },
        {  16, TL_LVL_ERROR,  "TP016 - "                                        }, 
        {  17, TL_LVL_ERROR,  "TP017 - cannot use blp in write mode"            },
        {  18, TL_LVL_ERROR,  "TP018 - duplicate option"                        },  /* option    */
        {  19, TL_LVL_ERROR,  "TP019 - "                                        }, 
        {  20, TL_LVL_ERROR,  "TP020 - mount tape"                              },  /* vid       */
                                                                                    /* labels    */
                                                                                    /* rings     */
                                                                                    /* drive     */
                                                                                    /* host      */
                                                                                    /* user      */
                                                                                    /* pid       */
                                                                                    /* why       */
        {  21, TL_LVL_ERROR,  "TP021 - label type mismatch"                     },  /* request   */
                                                                                    /* tape      */
        {  22, TL_LVL_ERROR,  "TP022 - path exists already"                     },
        {  23, TL_LVL_ERROR,  "TP023 - mount cancelled by operator"             },  /* operator  */
        {  24, TL_LVL_ERROR,  "TP024 - file does not exist"                     },  /* file      */
        {  25, TL_LVL_ERROR,  "TP025 - bad label structure"                     },
        {  26, TL_LVL_ERROR,  "TP026 - system error"                            },
        {  27, TL_LVL_ERROR,  "TP027 - you are not registered in account file"  },            
        {  28, TL_LVL_ERROR,  "TP028 - file not expired"                        },
        {  29, TL_LVL_ERROR,  "TP029 - pathname is mandatory"                   },
        {  30, TL_LVL_ERROR,  "TP030 - I/O error"                               },
        {  31, TL_LVL_ERROR,  "TP031 - no vid specified"                        },
        {  32, TL_LVL_ERROR,  "TP032 - config file"                             },  /* line      */
                                                                                    /* file      */
        {  33, TL_LVL_ERROR,  "TP033 - Drive not operational" },                    /* drive     */
                                                                                    /* host      */
        {  34, TL_LVL_ERROR,  "TP034 - all entries for a given device group must be grouped together" },
        {  35, TL_LVL_ERROR,  "TP035 - configuring"                             },  /* drive     */
                                                                                    /* up/down?  */
        {  36, TL_LVL_ERROR,  "TP036 - path is mandatory when rls flags is"     },  /* flags     */
        {  37, TL_LVL_ERROR,  "TP037 - path does not exist"                     },  /* path      */
        {  38, TL_LVL_ERROR,  "TP038 - pathname too long"                       },  
        {  39, TL_LVL_ERROR,  "TP039 - vsn mismatch"                            },  /* request   */
                                                                                    /* tape vsn  */

        {  40, TL_LVL_ERROR,  "TP040 - release pending"                         },
        {  41, TL_LVL_ERROR,  "TP041 - failure"                                 },  /* action    */
                                                                                    /* cur_vid   */
                                                                                    /* cur_unm   */
                                                                                    /* why       */
        {  42, TL_LVL_ERROR,  "TP042 - error"                                   },  /* what/why  */
                                                                                    /* what/why  */
                                                                                    /* what/why  */

        {  43, TL_LVL_ERROR,  "TP043 - configuration line too long"             },  /* line      */
        {  44, TL_LVL_ERROR,  "TP044 - fid mismatch: request"                   },  /* request   */
                                                                                    /* file      */
                                                                                    /* tape fid  */
        {  45, TL_LVL_ERROR,  "TP045 - cannot write file"                       },  /* file      */
                                                                                    /* no_files  */
        {  46, TL_LVL_ERROR,  "TP046 - request too large"                       },  /* max       */
        {  47, TL_LVL_ERROR,  "TP047 - reselect server requested by operator"   },
        {  48, TL_LVL_ERROR,  "TP048 - config postponed"                        },  /* curr_ass  */
        {  49, TL_LVL_ERROR,  "TP049 - option IGNOREEOI is not valid for label type al or sl" },
        {  50, TL_LVL_ERROR,  "TP050 - vid mismatch"},                              /* req_vid   */
                                                                                    /* drv_vid   */
        {  51, TL_LVL_ERROR,  "TP051 - fatal configuration error"               },  /* what      */
                                                                                    /* why       */
        {  52, TL_LVL_ERROR,  "TP052 - WSAStartup unsuccessful"                 },
        {  53, TL_LVL_ERROR,  "TP053 - you are not registered in the unix group/passwd mapping file" },
        {  54, TL_LVL_ERROR,  "TP054 - tape not mounted or not ready"           },
        {  55, TL_LVL_ERROR,  "TP055 - parameter inconsistency"                 },  /* vid       */
                                                                                    /* par_req   */
                                                                                    /* par_TMS   */

        {  56, TL_LVL_MONITORING,  "TP056 - request"                            },  /* type      */
                                                                                    /* uid       */
                                                                                    /* gid       */
                                                                                    /* clienthost*/
        {  57, TL_LVL_ERROR,  "TP057 - drive %s is not free"                    },  /* drive     */


        {  58, TL_LVL_ERROR,  "TP058 - no free drive"                           },
        {  59, TL_LVL_ERROR,  "TP059 - invalid reason"                          },
        {  60, TL_LVL_ERROR,  "TP060 - invalid combination of method and filstat" },
        {  61, TL_LVL_ERROR,  "TP061 - filstat value incompatible with read-only mode" },
        {  62, TL_LVL_ERROR,  "TP062 - tape to be prelabelled"                  },  /* tape      */
                                                                                    /* what      */
                                                                                    /* what      */
        {  63, TL_LVL_ERROR,  "TP063 - invalid user"                            },  /* user      */
        {  64, TL_LVL_ERROR,  "TP064 - invalid method for this label type"      },
        {  65, TL_LVL_ERROR,  "TP065 - tape has bad MIR"                        },  /* tape      */
                                                                                    /* labels    */
                                                                                    /* drive     */
                                                                                    /* host      */
                                                                                    /* name      */
                                                                                    /* pid       */
        { 101, TL_LVL_EMERGENCY , "TP101 - General Emergency Message"           },  
        { 102, TL_LVL_ALERT     , "TP102 - General Alert Message"               },
        { 103, TL_LVL_ERROR     , "TP103 - General Error Message"               },
        { 104, TL_LVL_WARNING   , "TP104 - General Warning Message"             },
        { 105, TL_LVL_AUTH      , "TP105 - General Auth Message"                },
        { 106, TL_LVL_SECURITY  , "TP106 - General Security Message"            },
        { 107, TL_LVL_USAGE     , "TP107 - General Usage Message"               },
        { 108, TL_LVL_SYSTEM    , "TP108 - General System Message"              },
        { 109, TL_LVL_IMPORTANT , "TP109 - General Important Message"           },
        { 110, TL_LVL_MONITORING, "TP110 - General Monitoring Message"          },
        { 111, TL_LVL_DEBUG     , "TP111 - General Debug Message"               }
};



/*
** The predefined messages for rtcpd/tpdaemon 
** (emtpy for the time being since it
** it is not clear if different tables 
** for the different tape related facilities 
** are needed or not)  
*/
tplogger_message_t tplogger_messages_tpdaemon[] = {

        {   0, TL_LVL_DEBUG     , "Dummy message"                               }  
};

tplogger_message_t tplogger_messages_rtcpd[] = {

        {   0, TL_LVL_DEBUG     , "Dummy message"                               }  
};


/**
 * Return the number of messages for the different facilities.
 *
 * @param self : reference to the tplogger struct
 *
 * @returns    : the number of available messages
 */
int DLL_DECL tplogger_nb_messages( tplogger_t *self ) {
  
        if( 0 == strcmp( self->tl_name, "tpdaemon" ) ) {
                
                /* return( ARRAY_ENTRIES( tplogger_messages_tpdaemon ) ); */
                return( ARRAY_ENTRIES( tplogger_messages ) );
        }

        if( 0 == strcmp( self->tl_name, "rtcpd" ) ) {
                
                /* return( ARRAY_ENTRIES( tplogger_messages_rtcpd ) ); */
                return( ARRAY_ENTRIES( tplogger_messages ) );
        }

        if( 0 == strcmp( self->tl_name, "stdio" ) ) {
                
                /* return( ARRAY_ENTRIES( tplogger_messages_rtcpd ) ); */
                return( ARRAY_ENTRIES( tplogger_messages ) );
        }
        
        return 0;
}
