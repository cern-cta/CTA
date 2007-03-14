/*
 * @(#)$RCSfile: rmc_server_api.h,v $ $Revision: 1.1 $ $Date: 2007/03/14 11:05:39 $ CERN IT-DS/HSM Jean-Philippe Baud
 */

#ifndef _RMC_SERVER_API_H
#define _RMC_SERVER_API_H

                        /*  function prototypes */

EXTERN_C int DLL_DECL rmc_srv_export _PROTO((char*, char*));
EXTERN_C int DLL_DECL rmc_srv_findcart _PROTO((char*, char*));
EXTERN_C int DLL_DECL rmc_srv_getgeom _PROTO((char*, char*));
EXTERN_C int DLL_DECL rmc_srv_import _PROTO((char*, char*));
EXTERN_C int DLL_DECL rmc_srv_mount _PROTO((char*, char*));
EXTERN_C int DLL_DECL rmc_srv_readelem _PROTO((char*, char*));
EXTERN_C int DLL_DECL rmc_srv_unmount _PROTO((char*, char*));

#endif
