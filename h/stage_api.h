/*
 * $Id: stage_api.h,v 1.64 2003/09/08 15:55:24 jdurand Exp $
 */

#ifndef __stage_api_h
#define __stage_api_h

#include <pwd.h>               /* For uid_t, gid_t - supported on all platforms (c.f. win32 dir.) */
#include <stddef.h>            /* For NULL definition */
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#ifndef _WIN32
#include <sys/time.h>          /* For time_t */
#else
#include <time.h>              /* For time_t */
#endif
#include "Castor_limits.h"     /* Get all hardcoded CASTOR constants */
#include "stage_constants.h"   /* To get the stager constants */
#include "stage_struct.h"      /* To get the stager structures definitions */
#include "stage_messages.h"    /* To get message format strings */
#include "stage_macros.h"      /* To get the stager macro utilities */
#include "stage_protocol.h"    /* API procotol */
#include "osdep.h"             /* For OS-dependencies */
#include "marshall.h"          /* For marshalling macros */
#include "serrno.h"            /* Because it contains ESTNACT */

/* Compatibility definition */
struct stage_hsm {
  char *xfile;         /* Recommended size: (CA_MAXHOSTNAMELEN+MAXPATH)+1 */
  char *upath;         /* Recommended size: (CA_MAXHOSTNAMELEN+MAXPATH)+1 */
  u_signed64 size;
  struct stage_hsm *next;
};
typedef struct stage_hsm stage_hsm_t;

/* -------------------------------------------------------- */
/* Generic STAGE_IN/STAGE_OUT/STAGE_WRT/STAGE_CAT interface */
/* -------------------------------------------------------- */
#if (defined(hpux) || defined(sun))
/* Why does hpux's cc complain on the following proyotype when */
/* compiled with -Ae ? */
/* On Sun old style declaration char promoted to int */
EXTERN_C int DLL_DECL stage_iowc _PROTO(());
#else /* hpux */
EXTERN_C int DLL_DECL stage_iowc _PROTO((int,                       /* req_type */
                                         char,                      /* t_or_d */
                                         u_signed64,                /* flags */
                                         int,                       /* openflags */
                                         mode_t,                    /* openmode */
                                         char *,                    /* hostname */
                                         char *,                    /* pooluser */
                                         int,                       /* nstcp_input */
                                         struct stgcat_entry *,     /* stcp_input */
                                         int *,                     /* nstcp_output */
                                         struct stgcat_entry **,    /* stcp_output */
                                         int,                       /* nstpp_input */
                                         struct stgpath_entry *));  /* stpp_input */
#endif /* hpux */
#define stage_in_tape(flags,openflags,hostname,pooluser,nstcp_input,stcp_input,nstcp_output,stcp_output,nstpp_input,stpp_input) \
  stage_iowc(STAGE_IN, 't',flags,openflags,(mode_t) 0,hostname,pooluser,nstcp_input,stcp_input,nstcp_output,stcp_output,nstpp_input,stpp_input)
#define stage_in_disk(flags,openflags,hostname,pooluser,nstcp_input,stcp_input,nstcp_output,stcp_output,nstpp_input,stpp_input) \
  stage_iowc(STAGE_IN, 'd',flags,openflags,(mode_t) 0,hostname,pooluser,nstcp_input,stcp_input,nstcp_output,stcp_output,nstpp_input,stpp_input)
#define stage_in_hsm(flags,openflags,hostname,pooluser,nstcp_input,stcp_input,nstcp_output,stcp_output,nstpp_input,stpp_input) \
  stage_iowc(STAGE_IN, 'm',flags,openflags,(mode_t) 0,hostname,pooluser,nstcp_input,stcp_input,nstcp_output,stcp_output,nstpp_input,stpp_input)
#define stage_in_castor(flags,openflags,hostname,pooluser,nstcp_input,stcp_input,nstcp_output,stcp_output,nstpp_input,stpp_input) \
  stage_iowc(STAGE_IN, 'h',flags,openflags,(mode_t) 0,hostname,pooluser,nstcp_input,stcp_input,nstcp_output,stcp_output,nstpp_input,stpp_input)
#define stage_out_tape(flags,openflags,openmode,hostname,pooluser,nstcp_input,stcp_input,nstcp_output,stcp_output,nstpp_input,stpp_input) \
  stage_iowc(STAGE_OUT,'t',flags,openflags,openmode,hostname,pooluser,nstcp_input,stcp_input,nstcp_output,stcp_output,nstpp_input,stpp_input)
#define stage_out_disk(flags,openflags,openmode,hostname,pooluser,nstcp_input,stcp_input,nstcp_output,stcp_output,nstpp_input,stpp_input) \
  stage_iowc(STAGE_OUT,'d',flags,openflags,openmode,hostname,pooluser,nstcp_input,stcp_input,nstcp_output,stcp_output,nstpp_input,stpp_input)
#define stage_out_hsm(flags,openflags,openmode,hostname,pooluser,nstcp_input,stcp_input,nstcp_output,stcp_output,nstpp_input,stpp_input) \
  stage_iowc(STAGE_OUT,'m',flags,openflags,openmode,hostname,pooluser,nstcp_input,stcp_input,nstcp_output,stcp_output,nstpp_input,stpp_input)
#define stage_out_castor(flags,openflags,openmode,hostname,pooluser,nstcp_input,stcp_input,nstcp_output,stcp_output,nstpp_input,stpp_input) \
  stage_iowc(STAGE_OUT,'h',flags,openflags,openmode,hostname,pooluser,nstcp_input,stcp_input,nstcp_output,stcp_output,nstpp_input,stpp_input)
#define stage_wrt_tape(flags,openflags,hostname,pooluser,nstcp_input,stcp_input,nstcp_output,stcp_output,nstpp_input,stpp_input) \
  stage_iowc(STAGE_WRT,'t',flags,openflags,(mode_t) 0,hostname,pooluser,nstcp_input,stcp_input,nstcp_output,stcp_output,nstpp_input,stpp_input)
#define stage_wrt_disk(flags,openflags,hostname,pooluser,nstcp_input,stcp_input,nstcp_output,stcp_output,nstpp_input,stpp_input) \
  stage_iowc(STAGE_WRT,'d',flags,openflags,(mode_t) 0,hostname,pooluser,nstcp_input,stcp_input,nstcp_output,stcp_output,nstpp_input,stpp_input)
#define stage_wrt_hsm(flags,openflags,hostname,pooluser,nstcp_input,stcp_input,nstcp_output,stcp_output,nstpp_input,stpp_input) \
  stage_iowc(STAGE_WRT,'m',flags,openflags,(mode_t) 0,hostname,pooluser,nstcp_input,stcp_input,nstcp_output,stcp_output,nstpp_input,stpp_input)
#define stage_wrt_castor(flags,openflags,hostname,pooluser,nstcp_input,stcp_input,nstcp_output,stcp_output,nstpp_input,stpp_input) \
  stage_iowc(STAGE_WRT,'h',flags,openflags,(mode_t) 0,hostname,pooluser,nstcp_input,stcp_input,nstcp_output,stcp_output,nstpp_input,stpp_input)
#define stage_cat_tape(flags,hostname,pooluser,nstcp_input,stcp_input,nstcp_output,stcp_output,nstpp_input,stpp_input) \
  stage_iowc(STAGE_CAT,'t',flags,openflags,openmode,hostname,pooluser,nstcp_input,stcp_input,nstcp_output,stcp_output,nstpp_input,stpp_input)
#define stage_cat_disk(flags,hostname,pooluser,nstcp_input,stcp_input,nstcp_output,stcp_output,nstpp_input,stpp_input) \
  stage_iowc(STAGE_CAT,'d',flags,openflags,openmode,hostname,pooluser,nstcp_input,stcp_input,nstcp_output,stcp_output,nstpp_input,stpp_input)
#define stage_cat_hsm(flags,hostname,pooluser,nstcp_input,stcp_input,nstcp_output,stcp_output,nstpp_input,stpp_input) \
  stage_iowc(STAGE_CAT,'m',flags,openflags,openmode,hostname,pooluser,nstcp_input,stcp_input,nstcp_output,stcp_output,nstpp_input,stpp_input)
#define stage_cat_castor(flags,hostname,pooluser,nstcp_input,stcp_input,nstcp_output,stcp_output,nstpp_input,stpp_input) \
  stage_iowc(STAGE_CAT,'h',flags,openflags,openmode,hostname,pooluser,nstcp_input,stcp_input,nstcp_output,stcp_output,nstpp_input,stpp_input)

/* --------------------------------- */
/* Generic STAGE_ALLOC/GET interface */
/* --------------------------------- */
#if (defined(hpux) || defined(sun))
/* Why does hpux's cc complain on the following proyotype when */
/* compiled with -Ae ? */
/* On Sun old style declaration char promoted to int */
EXTERN_C int DLL_DECL stage_alloc_or_get _PROTO(());
#else /* hpux */
EXTERN_C int DLL_DECL stage_alloc_or_get _PROTO((int,                       /* protocol */
												 u_signed64,                /* flags */
												 mode_t,                    /* openmode */
												 char *,                    /* hostname */
												 char *,                    /* pooluser */
												 char *,                    /* external filename */
												 u_signed64,                /* filesize */
												 int *,                     /* nstcp_output */
												 struct stgcat_entry **));  /* stpp_output */
#endif /* hpux */
#define stage_alloc(flags,openmode,hostname,pooluser,filename,filesize,nstcp_output,stcp_output) \
  stage_alloc_or_get(STAGE_ALLOC,flags,openmode,hostname,pooluser,filename,filesize,nstcp_output,stcp_output)
#define stage_get(flags,hostname,pooluser,filename,nstcp_output,stcp_output) \
  stage_alloc_or_get(STAGE_GET,flags,(mode_t) 0,hostname,pooluser,filename,(u_signed64) 0,nstcp_output,stcp_output)

/* ---------------------------- */
/* Generic STAGE_KILL interface */
/* ---------------------------- */
EXTERN_C int  DLL_DECL  stage_kill _PROTO((int));
EXTERN_C int  DLL_DECL  stage_admin_kill _PROTO((char *, u_signed64 *));

/* --------------------------------------------------------- */
/* RTCOPY-only interfaces : FILE COPIED and TAPE POSITIONNED */
/* --------------------------------------------------------- */
EXTERN_C int  DLL_DECL  stage_updc_filcp _PROTO((char *, int, int, char *, u_signed64, int, int, int, char *, char *, int, int, char *, char *));
EXTERN_C int  DLL_DECL  stage_updc_tppos _PROTO((char *, int, int, int, char *, char *, int, int, char *, char *));

/* ---------------------------- */
/* Generic STAGE_UPDC interface */
/* ---------------------------- */
/* Not yet done - only old protocol available */
EXTERN_C int  DLL_DECL  stage_updc_filchg _PROTO((char *, stage_hsm_t *));
EXTERN_C int  DLL_DECL  stage_updc_user _PROTO((char *, stage_hsm_t *));
EXTERN_C int  DLL_DECL  stage_updc_error _PROTO((char *, int, stage_hsm_t *));

/* ---------------------------- */
/* Generic STAGE_QRY interface  */
/* ---------------------------- */
#if (defined(hpux) || defined(sun))
/* Why does hpux's cc complain on the following proyotype when */
/* compiled with -Ae ? */
/* On Sun old style declaration char promoted to int */
EXTERN_C int DLL_DECL stage_qry _PROTO(());
#else /* hpux */
EXTERN_C int DLL_DECL stage_qry _PROTO((char,                      /* t_or_d */
                                        u_signed64,                /* flags */
                                        char *,                    /* hostname */
                                        int,                       /* nstcp_input */
                                        struct stgcat_entry *,     /* stcp_input */
                                        int *,                     /* nstcp_output */
                                        struct stgcat_entry **,    /* stcp_output */
                                        int *,                     /* nstpp_output */
                                        struct stgpath_entry **)); /* stpp_output */
#endif /* hpux */
#define stage_qry_tape(flags,hostname,nstcp_input,stcp_input,nstcp_output,stcp_output,nstpp_output,stpp_output) \
  stage_qry('t',flags,hostname,nstcp_input,stcp_input,nstcp_output,stcp_output,nstpp_output,stpp_output)
#define stage_qry_disk(flags,hostname,nstcp_input,stcp_input,nstcp_output,stcp_output,nstpp_output,stpp_output) \
  stage_qry('d',flags,hostname,nstcp_input,stcp_input,nstcp_output,stcp_output,nstpp_output,stpp_output)
#define stage_qry_hsm(flags,hostname,nstcp_input,stcp_input,nstcp_output,stcp_output,nstpp_output,stpp_output) \
  stage_qry('m',flags,hostname,nstcp_input,stcp_input,nstcp_output,stcp_output,nstpp_output,stpp_output)

/* ----------------------------- */
/* Shorthand STAGE_QRY interface */
/* ----------------------------- */

EXTERN_C int DLL_DECL stage_qry_Tape _PROTO((u_signed64,                /* flags */
											 char *,                    /* hostname */
											 char *,                    /* poolname */
											 char *,                    /* tape */
											 char *,                    /* fseq */
											 int *,                     /* nstcp_output */
											 struct stgcat_entry **,    /* stcp_output */
											 int *,                     /* nstpp_output */
											 struct stgpath_entry **)); /* stpp_output */
EXTERN_C int DLL_DECL stage_qry_Disk _PROTO((u_signed64,                /* flags */
											 char *,                    /* hostname */
											 char *,                    /* poolname */
											 char *,                    /* filename */
											 int *,                     /* nstcp_output */
											 struct stgcat_entry **,    /* stcp_output */
											 int *,                     /* nstpp_output */
											 struct stgpath_entry **)); /* stpp_output */
EXTERN_C int DLL_DECL stage_qry_Hsm _PROTO((u_signed64,                /* flags */
											char *,                    /* hostname */
											char *,                    /* poolname */
											char *,                    /* hsmname */
											int *,                     /* nstcp_output */
											struct stgcat_entry **,    /* stcp_output */
											int *,                     /* nstpp_output */
											struct stgpath_entry **)); /* stpp_output */

/* ---------------------------- */
/* Generic STAGE_CLR interface  */
/* ---------------------------- */
#if (defined(hpux) || defined(sun))
/* Why does hpux's cc complain on the following proyotype when */
/* compiled with -Ae ? */
/* On Sun old style declaration char promoted to int */
EXTERN_C int DLL_DECL stage_clr _PROTO(());
#else /* hpux */
EXTERN_C int DLL_DECL stage_clr _PROTO((char,                      /* t_or_d */
                                        u_signed64,                /* flags */
                                        char *,                    /* hostname */
                                        int,                       /* nstcp_input */
                                        struct stgcat_entry *,     /* stcp_input */
                                        int,                       /* nstpp_input */
                                        struct stgpath_entry *));  /* stpp_input */
#endif /* hpux */
#define stage_clr_tape(flags,hostname,nstcp_input,stcp_input) \
  stage_clr('t',flags,hostname,nstcp_input,stcp_input,0,(struct stgpath_entry *) NULL)
#define stage_clr_disk(flags,hostname,nstcp_input,stcp_input) \
  stage_clr('d',flags,hostname,nstcp_input,stcp_input,0,(struct stgpath_entry *) NULL)
#define stage_clr_hsm(flags,hostname,nstcp_input,stcp_input) \
  stage_clr('m',flags,hostname,nstcp_input,stcp_input,0,(struct stgpath_entry *) NULL)
#define stage_clr_path(flags,hostname,nstpp_input,stpp_input) \
  stage_clr('p',flags,hostname,0,(struct stgcat_entry *) NULL,nstpp_input,stpp_input)
#define stage_clr_link(flags,hostname,nstpp_input,stpp_input) \
  stage_clr('l',flags,hostname,0,(struct stgcat_entry *) NULL,nstpp_input,stpp_input)

/* ----------------------------- */
/* Shorthand STAGE_CLR interface */
/* ----------------------------- */

EXTERN_C int DLL_DECL stage_clr_Tape _PROTO((u_signed64,                /* flags */
											 char *,                    /* hostname */
											 char *,                    /* tape */
											 char *));                  /* fseq */
EXTERN_C int DLL_DECL stage_clr_Disk _PROTO((u_signed64,                /* flags */
											 char *,                    /* hostname */
											 char *));                  /* filename */
EXTERN_C int DLL_DECL stage_clr_Hsm _PROTO((u_signed64,                 /* flags */
											char *,                     /* hostname */
											char *));                   /* hsmname */
EXTERN_C int DLL_DECL stage_clr_Path _PROTO((u_signed64,                /* flags */
											 char *,                     /* hostname */
											 char *));                   /* pathname */
EXTERN_C int DLL_DECL stage_clr_Link _PROTO((u_signed64,                /* flags */
											 char *,                     /* hostname */
											 char *));                   /* pathname */

/* ---------------------------- */
/* Generic STAGE_UPDC interface */
/* ---------------------------- */
#ifdef hpux
/* Why does hpux's cc complain on the following prototype when */
/* compiled with -Ae ? */
EXTERN_C int DLL_DECL stage_updc _PROTO(());
#else /* hpux */
EXTERN_C int DLL_DECL stage_updc _PROTO((u_signed64,               /* flags */
										 char *,                    /* hostname */
										 char *,                    /* pooluser */
										 int,                       /* status */
										 int *,                     /* nstcp_output */
										 struct stgcat_entry **,    /* stcp_output */
										 int,                       /* nstpp_input */
										 struct stgpath_entry *));  /* stpp_input */
#endif /* hpux */

/* --------------------------- */
/* Generic STAGE_PUT interface */
/* --------------------------- */
/* Not available yet - only old protocol */
EXTERN_C int  DLL_DECL  stage_put_hsm _PROTO((char *, stage_hsm_t *));

/* ----------------------------- */
/* Generic STAGE_PING interface  */
/* ----------------------------- */
#ifdef hpux
EXTERN_C int DLL_DECL stage_ping _PROTO(());
#else
EXTERN_C int DLL_DECL stage_ping _PROTO((u_signed64, char *));
#endif

/* ----------------------------- */
/* Generic STAGE_INIT interface  */
/* ----------------------------- */
#ifdef hpux
EXTERN_C int DLL_DECL stage_init _PROTO(());
#else
EXTERN_C int DLL_DECL stage_init _PROTO((u_signed64, char *));
#endif

/* --------------------------------- */
/* Generic STAGE_SHUTDOWN interface  */
/* --------------------------------- */
#ifdef hpux
EXTERN_C int DLL_DECL stage_shutdown _PROTO(());
#else
EXTERN_C int DLL_DECL stage_shutdown _PROTO((u_signed64, char *));
#endif

/* ------------------------------------------ */
/* Utilities used by the API (stage_util.c)   */
/* ------------------------------------------ */
EXTERN_C int  DLL_DECL  send2stgd _PROTO((char *, int, u_signed64, char *, int, int, char *, int, int, struct stgcat_entry *, int *, struct stgcat_entry **, int *, struct stgpath_entry **));
/* Compatibility mode */
EXTERN_C int  DLL_DECL  send2stgd_compat _PROTO((char *, char *, int, int, char *, int)); /* Old version without forking */
EXTERN_C void DLL_DECL stage_sleep _PROTO((int)); /* Sleep thread-safe */
EXTERN_C int  DLL_DECL stage_stgmagic _PROTO(()); /* Return current magic number */
EXTERN_C void DLL_DECL dump_stpp _PROTO((int *, struct stgpath_entry *, int (*) _PROTO((int *, int, ...))));
EXTERN_C void DLL_DECL dump_stcp _PROTO((int *, struct stgcat_entry *, int (*) _PROTO((int *, int, ...))));
EXTERN_C void DLL_DECL print_stpp _PROTO((struct stgpath_entry *));
EXTERN_C void DLL_DECL print_stcp _PROTO((struct stgcat_entry *));
EXTERN_C int  DLL_DECL stage_strtoi _PROTO((int *,char *,char **, int));
EXTERN_C void DLL_DECL stage_util_time _PROTO((time_t, char *));
EXTERN_C void DLL_DECL stage_util_retenp _PROTO((int, char *));
EXTERN_C int  DLL_DECL stage_util_status2string _PROTO((char *, size_t, int));
EXTERN_C int  DLL_DECL stage_util_charconv2string _PROTO((char *, size_t, int));
EXTERN_C int  DLL_DECL stage_util_E_Tflags2string _PROTO((char *, size_t, int));
EXTERN_C int  DLL_DECL stage_util_maxtapefseq _PROTO((char*));
EXTERN_C int  DLL_DECL stage_util_check_for_strutou64 _PROTO((char*));

/* ------------------------------------------- */
/* Utilities used by the API (stage_usrmsg.c)  */
/* ------------------------------------------- */
EXTERN_C int  DLL_DECL  stage_seterrbuf _PROTO((char *, int));
EXTERN_C int  DLL_DECL  stage_setoutbuf _PROTO((char *, int));
#ifndef SWIG
EXTERN_C int  DLL_DECL  stage_setlog    _PROTO((void (*)(int, char*)));
#endif
EXTERN_C int  DLL_DECL  stage_geterrbuf _PROTO((char **, int *));
EXTERN_C int  DLL_DECL  stage_getoutbuf _PROTO((char **, int *));
#ifndef SWIG
EXTERN_C int  DLL_DECL  stage_getlog    _PROTO((void (**)(int, char*)));
#endif
EXTERN_C int  DLL_DECL  stage_errmsg _PROTO(());
EXTERN_C int  DLL_DECL  stage_outmsg _PROTO(());
EXTERN_C int  DLL_DECL  stage_setcallback _PROTO((void (*)(struct stgcat_entry *, struct stgpath_entry *)));
EXTERN_C int  DLL_DECL  stage_getcallback _PROTO((void (**)(struct stgcat_entry *, struct stgpath_entry *)));
EXTERN_C int  DLL_DECL  stage_callback _PROTO((struct stgcat_entry *, struct stgpath_entry *));
EXTERN_C int  DLL_DECL  stage_stcp2buf _PROTO((char *, size_t, struct stgcat_entry *));
EXTERN_C int  DLL_DECL  stage_buf2stcp _PROTO((struct stgcat_entry *, char *, size_t));
EXTERN_C int  DLL_DECL  stage_buf2stpp _PROTO((struct stgpath_entry *, char *, size_t));
EXTERN_C int  DLL_DECL  stage_getuniqueid _PROTO((u_signed64 *));
EXTERN_C int  DLL_DECL  stage_setuniqueid _PROTO((u_signed64));
EXTERN_C int  DLL_DECL  stage_getlaststghost _PROTO((char **));
EXTERN_C int  DLL_DECL  stage_setlaststghost _PROTO((char *));
EXTERN_C int  DLL_DECL  build_linkname _PROTO((char *, char *, size_t, int));
EXTERN_C int  DLL_DECL  build_Upath _PROTO((int, char *, size_t, int));
EXTERN_C int  DLL_DECL  rc_castor2shift _PROTO((int));

#endif /* __stage_api_h */
