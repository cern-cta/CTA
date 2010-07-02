/*
 * $Id: rmc_api.h,v 1.2 2003/10/29 12:37:27 baud Exp $
 */

/*
 * Copyright (C) 2002-2003 by CERN/IT/DS/HSM
 * All rights reserved
 */

/*
 * @(#)$RCSfile: rmc_api.h,v $ $Revision: 1.2 $ $Date: 2003/10/29 12:37:27 $ CERN IT-DS/HSM Jean-Philippe Baud
 */

#ifndef _RMC_API_H
#define _RMC_API_H
#include "smc.h"

                        /*  function prototypes */

EXTERN_C int rmc_dismount (char *, char *, char *, int, int);
EXTERN_C int rmc_errmsg (char *, char *, ...);
EXTERN_C int rmc_export (char *, char *, char *);
EXTERN_C int rmc_find_cartridge (char *, char *, char *, int, int, int, struct smc_element_info *);
EXTERN_C int rmc_get_geometry (char *, char *, struct robot_info *);
EXTERN_C int rmc_import (char *, char *, char *);
EXTERN_C int rmc_mount (char *, char *, char *, int, int);
EXTERN_C int rmc_read_elem_status (char *, char *, int, int, int, struct smc_element_info *);
EXTERN_C int rmc_seterrbuf (char *, int);
EXTERN_C int send2rmc (char *, char *, int, char *, int);
#endif
