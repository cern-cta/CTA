/*
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
