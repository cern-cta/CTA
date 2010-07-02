#ifndef __rm_util_h
#define __rm_util_h

#include <osdep.h>
#include "rm_constants.h"
#include "Castor_limits.h"

#include "rm_struct.h"

EXTERN_C int  Crm_send_maxtask (int, char *, int, int);
EXTERN_C int  Crm_send_fsstate (int, char *, int, int, char *, char *, char *, char *, int, int, int);
EXTERN_C int  Crm_util_osname (char *);
EXTERN_C int  Crm_util_arch (char *);
EXTERN_C int  Crm_util_totalram (u_signed64 *);
EXTERN_C int  Crm_util_freeram (u_signed64 *);
EXTERN_C int  Crm_util_ram (u_signed64 *, u_signed64 *);
EXTERN_C int  Crm_util_usedmem (u_signed64 *);
EXTERN_C int  Crm_util_freemem (u_signed64 *);
EXTERN_C int  Crm_util_mem (u_signed64 *, u_signed64 *);
EXTERN_C int  Crm_util_totalswap (u_signed64 *);
EXTERN_C int  Crm_util_freeswap (u_signed64 *);
EXTERN_C int  Crm_util_swap (u_signed64 *, u_signed64 *);
EXTERN_C int  Crm_util_totalspace (char *, u_signed64 *);
EXTERN_C int  Crm_util_freespace (char *, u_signed64 *);
EXTERN_C int  Crm_util_countstream (char *, int *, int *, int *);
EXTERN_C int  Crm_util_space (char *, u_signed64 *, u_signed64 *);
EXTERN_C int  Crm_util_io (char *, u_signed64 *, u_signed64 *);
EXTERN_C int  Crm_util_totalproc (int *);
EXTERN_C int  Crm_util_availproc (int *);
EXTERN_C int  Crm_util_proc (int *, int *);
EXTERN_C int  Crm_util_ifconf (struct Crm_ifconf **);
EXTERN_C int  Crm_util_ifce (struct Crm_ifconf **);
EXTERN_C int  Crm_util_loadavg (double *);
EXTERN_C int  Crm_util_find_partition (char *, char *, int);
EXTERN_C int  Crm_send_osname (int, char *, int, char *);
EXTERN_C int  Crm_send_stager (int, int, char *, int, char *);
EXTERN_C int  Crm_send_arch (int, char *, int, char *);
EXTERN_C int  Crm_send_ram (int, char *, int, u_signed64, u_signed64);
EXTERN_C int  Crm_send_mem (int, char *, int, u_signed64, u_signed64);
EXTERN_C int  Crm_send_swap (int, char *, int, u_signed64, u_signed64);
EXTERN_C int  Crm_send_space (int, char *, int, int, char *, char *, char *, u_signed64, u_signed64, int);
EXTERN_C int  Crm_send_io_rate (int, char *, int, int, char *, char *, char *, u_signed64, u_signed64, u_signed64);
EXTERN_C int  Crm_send_proc (int, char *, int, int, int);
EXTERN_C int  Crm_send_load (int, char *, int, double);
EXTERN_C int  Crm_send_stagerhost (int, char *, int, char *);
EXTERN_C int  Crm_send_stagerport (int, char *, int, int);
EXTERN_C int  Crm_send_ifce (int, char *, int, struct Crm_ifconf *);
EXTERN_C int  Crm_send_state (int, char *, int, char *);
EXTERN_C int  Crm_send_fs_state (int, int, char *, int, int, char *, char *, char *, char *, int *, int *, int *);
EXTERN_C int  Crm_send_loadavg (int, int, char *, int, double);

#endif /* __rm_util_h */
