/*
 * $Id: rmc_api.h,v 1.1 2002/12/01 07:31:56 baud Exp $
 */

/*
 * Copyright (C) 2002 by CERN/IT/DS/HSM
 * All rights reserved
 */

/*
 * @(#)$RCSfile: rmc_api.h,v $ $Revision: 1.1 $ $Date: 2002/12/01 07:31:56 $ CERN IT-DS/HSM Jean-Philippe Baud
 */

#ifndef _RMC_API_H
#define _RMC_API_H
#include "smc.h"

                        /*  function prototypes */

EXTERN_C int DLL_DECL rmc_dismount _PROTO((char *, char *, char *, int, int));
EXTERN_C int DLL_DECL rmc_export _PROTO((char *, char *, char *));
EXTERN_C int DLL_DECL rmc_find_cartridge _PROTO((char *, char *, char *, int, int, int, struct smc_element_info *));
EXTERN_C int DLL_DECL rmc_get_geometry _PROTO((char *, char *, struct robot_info *));
EXTERN_C int DLL_DECL rmc_import _PROTO((char *, char *, char *));
EXTERN_C int DLL_DECL rmc_mount _PROTO((char *, char *, char *, int, int));
EXTERN_C int DLL_DECL rmc_read_elem_status _PROTO((char *, char *, int, int, int, struct smc_element_info *));
EXTERN_C int DLL_DECL rmc_seterrbuf _PROTO((char *, int));
#endif
