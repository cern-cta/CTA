/*
 * $Id: Cmonit_api.h,v 1.1 2002/05/29 13:20:06 bcouturi Exp $
 * Copyright (C) 2002 by CERN/IT/PDP/DM
 * All rights reserved
 */

/* 
 * Cmonit_api.h - Header for the CASTOR Monitoring API and related functions  
 */


#ifndef _MONITOR_H_INCLUDED_
#define _MONITOR_H_INCLUDED_

#include <time.h>
#include "Castor_limits.h"
#include "osdep.h"
#if !defined(_WIN32)
#include "Ctape.h" 
#endif
#include "Cmonit_constants.h"


/* ------------------------------------------
 * METHODS USED BY DAEMONS TO SEND STATUS MSG
 * ------------------------------------------ */

EXTERN_C int DLL_DECL Cmonit_init _PROTO ((void));
  
EXTERN_C int DLL_DECL Cmonit_check_monitor_on _PROTO ((void));

EXTERN_C int DLL_DECL Cmonit_send _PROTO ((int, char *, int));

EXTERN_C int DLL_DECL Cmonit_get_monitor_address _PROTO ((char *));


EXTERN_C int DLL_DECL Cmonit_send_disk_rec _PROTO ((int , int , 
						    int , int));
EXTERN_C int DLL_DECL Cmonit_send_stager_status _PROTO ((time_t));


EXTERN_C int DLL_DECL Cmonit_send_transfer_info _PROTO((int, 
							u_signed64, 
							u_signed64,
							u_signed64, 
							int, int));
#if !defined(_WIN32)
EXTERN_C int DLL_DECL Cmonit_send_tape_status _PROTO((struct tptab *, 
						      int));
#endif

#endif /*  _MONITOR_H_INCLUDED_ */










