#ifndef __rm_util_h
#define __rm_util_h

#include <osdep.h>
#include "rm_constants.h"
#include "Castor_limits.h"

#include "rm_struct.h"

EXTERN_C int  DLL_DECL Crm_util_osname _PROTO((char *));
EXTERN_C int  DLL_DECL Crm_util_arch _PROTO((char *));
EXTERN_C int  DLL_DECL Crm_util_totalram _PROTO((u_signed64 *));
EXTERN_C int  DLL_DECL Crm_util_freeram _PROTO((u_signed64 *));
EXTERN_C int  DLL_DECL Crm_util_ram _PROTO((u_signed64 *, u_signed64 *));
EXTERN_C int  DLL_DECL Crm_util_usedmem _PROTO((u_signed64 *));
EXTERN_C int  DLL_DECL Crm_util_freemem _PROTO((u_signed64 *));
EXTERN_C int  DLL_DECL Crm_util_mem _PROTO((u_signed64 *, u_signed64 *));
EXTERN_C int  DLL_DECL Crm_util_totalswap _PROTO((u_signed64 *));
EXTERN_C int  DLL_DECL Crm_util_freeswap _PROTO((u_signed64 *));
EXTERN_C int  DLL_DECL Crm_util_swap _PROTO((u_signed64 *, u_signed64 *));
EXTERN_C int  DLL_DECL Crm_util_totalspace _PROTO((char *, u_signed64 *));
EXTERN_C int  DLL_DECL Crm_util_freespace _PROTO((char *, u_signed64 *));
EXTERN_C int  DLL_DECL Crm_util_countstream _PROTO((char *, int *, int *, int *));
EXTERN_C int  DLL_DECL Crm_util_space _PROTO((char *, u_signed64 *, u_signed64 *));
EXTERN_C int  DLL_DECL Crm_util_io _PROTO((char *, u_signed64 *, u_signed64 *));
EXTERN_C int  DLL_DECL Crm_util_totalproc _PROTO((int *));
EXTERN_C int  DLL_DECL Crm_util_availproc _PROTO((int *));
EXTERN_C int  DLL_DECL Crm_util_proc _PROTO((int *, int *));
EXTERN_C int  DLL_DECL Crm_util_ifconf _PROTO((struct Crm_ifconf **));
EXTERN_C int  DLL_DECL Crm_util_ifce _PROTO((struct Crm_ifconf **));
EXTERN_C int  DLL_DECL Crm_util_loadavg _PROTO((double *));
EXTERN_C void DLL_DECL Crm_sleep _PROTO((int));
EXTERN_C int  DLL_DECL Crm_util_find_partition _PROTO((char *, char *, int));

EXTERN_C int  DLL_DECL send2rmd _PROTO((char *, int, char *, int));
EXTERN_C int  DLL_DECL rmdsend2user _PROTO((char *, int, char *, int));

EXTERN_C int  DLL_DECL Crm_send_osname _PROTO((int, int, char *, int, char *));
EXTERN_C int  DLL_DECL Crm_send_stager _PROTO((int, int, char *, int, char *));
EXTERN_C int  DLL_DECL Crm_send_arch _PROTO((int, int, char *, int, char *));
EXTERN_C int  DLL_DECL Crm_send_ram _PROTO((int, int, char *, int, u_signed64, u_signed64));
EXTERN_C int  DLL_DECL Crm_send_mem _PROTO((int, int, char *, int, u_signed64, u_signed64));
EXTERN_C int  DLL_DECL Crm_send_swap _PROTO((int, int, char *, int, u_signed64, u_signed64));
EXTERN_C int  DLL_DECL Crm_send_space _PROTO((int, int, char *, int, int, char *, char *, char *, u_signed64, u_signed64, int));
EXTERN_C int  DLL_DECL Crm_send_io_rate _PROTO((int, int, char *, int, int, char *, char *, char *, u_signed64, u_signed64, u_signed64));
EXTERN_C int  DLL_DECL Crm_send_proc _PROTO((int, int, char *, int, int, int));
EXTERN_C int  DLL_DECL Crm_send_load _PROTO((int, int, char *, int, double));
EXTERN_C int  DLL_DECL Crm_send_stagerhost _PROTO((int, int, char *, int, char *));
EXTERN_C int  DLL_DECL Crm_send_stagerport _PROTO((int, int, char *, int, int));
EXTERN_C int  DLL_DECL Crm_send_ifce _PROTO((int, int, char *, int, struct Crm_ifconf *));
EXTERN_C int  DLL_DECL Crm_send_state _PROTO((int, int, char *, int, char *));
EXTERN_C int  DLL_DECL Crm_send_fs_state _PROTO((int, int, char *, int, int, char *, char *, char *, char *, int *, int *, int *));
EXTERN_C int  DLL_DECL Crm_send_loadavg _PROTO((int, int, char *, int, double));
EXTERN_C int  DLL_DECL Crm_checkdir _PROTO((char *));
EXTERN_C int  DLL_DECL Crm_strtoi _PROTO((int *,char *,char **, int));

#endif /* __rm_util_h */
