/*
 * @(#)$RCSfile: rmc_server_api.h,v $ $Revision: 1.1 $ $Date: 2007/03/14 11:05:39 $ CERN IT-DS/HSM Jean-Philippe Baud
 */

#ifndef _RMC_SERVER_API_H
#define _RMC_SERVER_API_H

                        /*  function prototypes */

EXTERN_C int rmc_srv_export (char*, char*);
EXTERN_C int rmc_srv_findcart (char*, char*);
EXTERN_C int rmc_srv_getgeom (char*, char*);
EXTERN_C int rmc_srv_import (char*, char*);
EXTERN_C int rmc_srv_mount (char*, char*);
EXTERN_C int rmc_srv_readelem (char*, char*);
EXTERN_C int rmc_srv_unmount (char*, char*);

#endif
