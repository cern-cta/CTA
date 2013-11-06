/*
 * $Id: rmc_api.h,v 1.2 2003/10/29 12:37:27 baud Exp $
 */

/*
 * Copyright (C) 2002-2003 by CERN/IT/DS/HSM
 * All rights reserved
 */

/*
 */

#ifndef _RMC_API_H
#define _RMC_API_H

#include "h/osdep.h"
#include "h/smc_struct.h"

                        /*  function prototypes */

EXTERN_C int rmc_dismount(const char *const server, const char *const vid, const int drvord, const int force);
EXTERN_C int rmc_errmsg(const char *const func, const char *const msg, ...);
EXTERN_C int rmc_export(const char *const server, const char *const vid);
EXTERN_C int rmc_find_cartridge(const char *const server, const char *const pattern, const int type, const int startaddr, const int nbelem, struct smc_element_info *const element_info);
EXTERN_C int rmc_get_geometry(const char *const server, struct robot_info *const robot_info);
EXTERN_C int rmc_import(const char *const server, const char *const vid);
EXTERN_C int rmc_mount(const char *const server, const char *const vid, const int side, const int drvord);
EXTERN_C int rmc_read_elem_status(const char *const server, const int type, const int startaddr, const int nbelem, struct smc_element_info *const element_info);
EXTERN_C void rmc_seterrbuf(const char *const buffer, const int buflen);
EXTERN_C int send2rmc(const char *const host, const char *const reqp, const int reql, char *const user_repbuf, const int user_repbuf_len);

#endif
