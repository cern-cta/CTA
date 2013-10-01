/*
 */

#ifndef _RMC_SERVER_API_H
#define _RMC_SERVER_API_H

                        /*  function prototypes */

int rmc_srv_export(const int rpfd, char *const req_data, const char *const clienthost);
int rmc_srv_findcart(const int rpfd, char *const req_data, const char *const clienthost);
int rmc_srv_getgeom(const int rpfd, char *const req_data, const char *const clienthost);
int rmc_srv_import(const int rpfd, char *const req_data, const char *const clienthost);
int rmc_srv_mount( const int rpfd, char *const req_data, const char *const clienthost);
int rmc_srv_readelem(const int rpfd, char *const req_data, const char *const clienthost);
int rmc_srv_unmount(const int rpfd, char *const req_data, const char *const clienthost);
int rmc_srv_genericmount(const int rpfd, char *const req_data, const char *const clienthost);
int rmc_srv_genericunmount(const int rpfd, char *const req_data, const char *const clienthost);
#endif
