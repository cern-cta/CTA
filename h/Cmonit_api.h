/*
 * $Id: Cmonit_api.h,v 1.3 2002/10/21 14:02:49 bcouturi Exp $
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

EXTERN_C int DLL_DECL Cmonit_get_monitor_address _PROTO((char *));


#endif /*  _MONITOR_H_INCLUDED_ */










