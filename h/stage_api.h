/*
 * $Id: stage_api.h,v 1.18 2001/01/31 19:07:33 jdurand Exp $
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
#include "Castor_limits.h"     /* Get all hardcoded CASTOR constants                              */
#include "stage.h"             /* To get the API req_type constants - bijection with stgdaemon    */
#include "osdep.h"             /* For OS-dependencies */
#include "marshall.h"          /* For marshalling macros */

/* Compatibility definition */
struct stage_hsm {
  char *xfile;         /* Recommended size: (CA_MAXHOSTNAMELEN+MAXPATH)+1 */
  char *upath;         /* Recommended size: (CA_MAXHOSTNAMELEN+MAXPATH)+1 */
  u_signed64 size;
  struct stage_hsm *next;
};
typedef struct stage_hsm stage_hsm_t;

/* ====================================================================== */
/* ====================== DEFINITION OF API PROTOCOL  =================== */
/* ====================================================================== */

/* In the following we distringuish two modes from each marshalling : */
/* a request (INPUT) mode and a full (OUTPUT) mode. This is because */
/* quite a lot of methods, in particular all the methods in stage_iowc */
/* do not need to send the full structure to make a valid request. And */
/* anyway only very few members of structures are used by the stager daemon */
/* to process and validate a request. So, in order not to transfer useless */
/* (might be better to say : not used information), only the relevant members */
/* of the structures are transfered in the INPUT mode. */

#define STAGE_INPUT_MODE 0
#define STAGE_OUTPUT_MODE 1

#define marshall_STAGE_PATH(from,output,ptr,st) { \
  marshall_STRING(ptr,(st)->upath);               \
}
#define unmarshall_STAGE_PATH(from,output,ptr,st) { \
  output += unmarshall_STRINGN(ptr,(st)->upath,(CA_MAXHOSTNAMELEN+MAXPATH)+1); \
}

#define marshall_STAGE_CAT(from,output,ptr,st) {   \
  marshall_LONG(ptr,(st)->reqid);                  \
  marshall_LONG(ptr,(st)->blksize);                \
  marshall_BYTE(ptr,(st)->charconv);               \
  marshall_BYTE(ptr,(st)->keep);                   \
  marshall_LONG(ptr,(st)->lrecl);                  \
  marshall_LONG(ptr,(st)->nread);                  \
  marshall_STRING(ptr,(st)->poolname);             \
  marshall_STRING(ptr,(st)->recfm);                \
  marshall_LONG(ptr,(st)->size);                   \
  if (from == STAGE_OUTPUT_MODE) {                 \
    marshall_STRING(ptr,(st)->ipath);              \
  }                                                \
  marshall_BYTE(ptr,(st)->t_or_d);                 \
  if (from == STAGE_OUTPUT_MODE) {                 \
    marshall_STRING(ptr,(st)->group);              \
    marshall_STRING(ptr,(st)->user);               \
    marshall_LONG(ptr,(st)->uid);                  \
    marshall_LONG(ptr,(st)->gid);                  \
    marshall_LONG(ptr,(st)->mask);                 \
    marshall_LONG(ptr,(st)->status);               \
    marshall_HYPER(ptr,(st)->actual_size);         \
    marshall_TIME_T(ptr,(st)->c_time);             \
    marshall_TIME_T(ptr,(st)->a_time);             \
    marshall_LONG(ptr,(st)->nbaccesses);           \
  }                                                \
  switch ((st)->t_or_d) {                          \
  case 't':                                        \
    {                                              \
      int __i_stage_api;                           \
      marshall_STRING(ptr,(st)->u1.t.den);         \
      marshall_STRING(ptr,(st)->u1.t.dgn);         \
      marshall_STRING(ptr,(st)->u1.t.fid);         \
      marshall_BYTE(ptr,(st)->u1.t.filstat);       \
      marshall_STRING(ptr,(st)->u1.t.fseq);        \
      marshall_STRING(ptr,(st)->u1.t.lbl);         \
      marshall_LONG(ptr,(st)->u1.t.retentd);       \
      marshall_STRING(ptr,(st)->u1.t.tapesrvr);    \
      marshall_LONG(ptr,(st)->u1.t.E_Tflags);      \
      for (__i_stage_api = 0; __i_stage_api < MAXVSN; __i_stage_api++) {    \
        marshall_STRING(ptr,(st)->u1.t.vid[__i_stage_api]); \
        marshall_STRING(ptr,(st)->u1.t.vsn[__i_stage_api]); \
      }                                            \
    }                                              \
    break;                                         \
  case 'd':                                        \
    marshall_STRING(ptr,(st)->u1.d.xfile);         \
    marshall_STRING(ptr,(st)->u1.d.Xparm);         \
    break;                                         \
  case 'a':                                        \
    marshall_STRING(ptr,(st)->u1.d.xfile);         \
    break;                                         \
  case 'm':                                        \
    marshall_STRING(ptr,(st)->u1.m.xfile);         \
  case 'h':                                        \
    marshall_STRING(ptr,(st)->u1.h.xfile);         \
    if (from == STAGE_OUTPUT_MODE) {               \
      marshall_STRING(ptr,(st)->u1.h.server);      \
      marshall_HYPER(ptr,(st)->u1.h.fileid);       \
      marshall_SHORT(ptr,(st)->u1.h.fileclass);    \
    }                                              \
    marshall_STRING(ptr,(st)->u1.h.tppool);        \
    break;                                         \
  default:                                         \
    output = -1;                                   \
    break;                                         \
  }                                                \
}

#define unmarshall_STAGE_CAT(from,output,ptr,st) {   \
  unmarshall_LONG(ptr,(st)->reqid);                  \
  unmarshall_LONG(ptr,(st)->blksize);                \
  unmarshall_BYTE(ptr,(st)->charconv);               \
  unmarshall_BYTE(ptr,(st)->keep);                   \
  unmarshall_LONG(ptr,(st)->lrecl);                  \
  unmarshall_LONG(ptr,(st)->nread);                  \
  output += unmarshall_STRINGN(ptr,(st)->poolname,CA_MAXPOOLNAMELEN+1); \
  output += unmarshall_STRINGN(ptr,(st)->recfm,CA_MAXRECFMLEN+1); \
  unmarshall_LONG(ptr,(st)->size);                   \
  if (from == STAGE_OUTPUT_MODE) {                   \
    output += unmarshall_STRINGN(ptr,(st)->ipath,(CA_MAXHOSTNAMELEN+MAXPATH)+1); \
  }                                                  \
  unmarshall_BYTE(ptr,(st)->t_or_d);                 \
  if (from == STAGE_OUTPUT_MODE) {                   \
    output += unmarshall_STRINGN(ptr,(st)->group,CA_MAXGRPNAMELEN+1); \
    output += unmarshall_STRINGN(ptr,(st)->user,CA_MAXUSRNAMELEN+1); \
    unmarshall_LONG(ptr,(st)->uid);                  \
    unmarshall_LONG(ptr,(st)->gid);                  \
    unmarshall_LONG(ptr,(st)->mask);                 \
    unmarshall_LONG(ptr,(st)->status);               \
    unmarshall_HYPER(ptr,(st)->actual_size);         \
    unmarshall_TIME_T(ptr,(st)->c_time);             \
    unmarshall_TIME_T(ptr,(st)->a_time);             \
    unmarshall_LONG(ptr,(st)->nbaccesses);           \
  }                                                  \
  switch ((st)->t_or_d) {                            \
  case 't':                                          \
    {                                                \
      int __i_stage_api;                             \
      output += unmarshall_STRINGN(ptr,(st)->u1.t.den,CA_MAXDENLEN+1); \
      output += unmarshall_STRINGN(ptr,(st)->u1.t.dgn,CA_MAXDGNLEN+1); \
      output += unmarshall_STRINGN(ptr,(st)->u1.t.fid,CA_MAXFIDLEN+1); \
      unmarshall_BYTE(ptr,(st)->u1.t.filstat);       \
      output += unmarshall_STRINGN(ptr,(st)->u1.t.fseq,CA_MAXFSEQLEN+1); \
      output += unmarshall_STRINGN(ptr,(st)->u1.t.lbl,CA_MAXLBLTYPLEN+1); \
      unmarshall_LONG(ptr,(st)->u1.t.retentd);       \
      output += unmarshall_STRINGN(ptr,(st)->u1.t.tapesrvr,CA_MAXHOSTNAMELEN+1); \
      unmarshall_LONG(ptr,(st)->u1.t.E_Tflags);      \
      for (__i_stage_api = 0; __i_stage_api < MAXVSN; __i_stage_api++) {      \
        output += unmarshall_STRINGN(ptr,(st)->u1.t.vid[__i_stage_api],CA_MAXVIDLEN+1); \
        output += unmarshall_STRINGN(ptr,(st)->u1.t.vsn[__i_stage_api],CA_MAXVSNLEN+1); \
      }                                              \
    }                                                \
    break;                                           \
  case 'd':                                          \
    output += unmarshall_STRINGN(ptr,(st)->u1.d.xfile,(CA_MAXHOSTNAMELEN+MAXPATH)+1); \
    output += unmarshall_STRINGN(ptr,(st)->u1.d.Xparm,23);     \
    break;                                           \
  case 'a':                                          \
    output += unmarshall_STRINGN(ptr,(st)->u1.d.xfile,167);    \
    break;                                           \
  case 'm':                                          \
    output += unmarshall_STRINGN(ptr,(st)->u1.m.xfile,167);    \
  case 'h':                                          \
    output += unmarshall_STRINGN(ptr,(st)->u1.h.xfile,167);    \
    if (from == STAGE_OUTPUT_MODE) {                 \
      output += unmarshall_STRINGN(ptr,(st)->u1.h.server,CA_MAXHOSTNAMELEN+1); \
      unmarshall_HYPER(ptr,(st)->u1.h.fileid);       \
      unmarshall_SHORT(ptr,(st)->u1.h.fileclass);    \
    }                                                \
    output += unmarshall_STRINGN(ptr,(st)->u1.h.tppool,CA_MAXPOOLNAMELEN+1); \
    break;                                           \
  default:                                           \
    output = -1;                                     \
    break;                                           \
  }                                                  \
}

/* ========================================================================= */
/* ===================== DEFINITION OF API FLAGS =========================== */
/* ========================================================================= */
#define STAGE_DEFERRED    0x00001     /* -A deferred  [stage_iowc]           */
#define STAGE_GRPUSER     0x00002     /* -G           [stage_iowc,stage_qry] */
#define STAGE_COFF        0x00004     /* -c off       [stage_iowc]           */
#define STAGE_UFUN        0x00008     /* -U           [stage_iowc]           */
#define STAGE_INFO        0x00010     /* -z           [stage_iowc]           */
#define STAGE_ALL         0x00020     /* -a           [stage_qry]            */
#define STAGE_LINKNAME    0x00040     /* -L           [stage_qry]            */
#define STAGE_LONG        0x00080     /* -l           [stage_qry]            */
#define STAGE_PATHNAME    0x00100     /* -P           [stage_qry]            */
#define STAGE_SORTED      0x00200     /* -S           [stage_qry]            */
#define STAGE_STATPOOL    0x00400     /* -s           [stage_qry]            */
#define STAGE_TAPEINFO    0x00800     /* -T           [stage_qry]            */
#define STAGE_USER        0x01000     /* -u           [stage_qry]            */
#define STAGE_EXTENDED    0x02000     /* -x           [stage_qry]            */
#define STAGE_ALLOCED     0x04000     /* -A           [stage_qry]            */
#define STAGE_FILENAME    0x08000     /* -f           [stage_qry]            */
#define STAGE_EXTERNAL    0x10000     /* -I           [stage_qry]            */
#define STAGE_MULTIFSEQ   0x20000     /* -Q           [stage_qry]            */
#define STAGE_MIGRULES    0x40000     /* -X           [stage_qry]            */
#define STAGE_SILENT      0x80000     /* --silent     [stage_iowc]           */
#define STAGE_NOWAIT      0x100000    /* --nowait     [stage_iowc]           */
#define STAGE_NOREGEXP    0x200000    /* --noregexp   [stage_qry]            */
#define STAGE_DUMP        0x400000    /* --dump       [stage_qry]            */

/* For stage_qry only */
/* ------------------ */

/* ======================================================================= */
/* =================== DEFINITION OF API METHODS ========================= */
/* ======================================================================= */
#define STAGE_00        100
#define STAGE_IN        101
#define STAGE_OUT       102
#define STAGE_WRT       103
#define STAGE_PUT       104           /* Not yet supported */
#define STAGE_QRY       105           /* Not yet supported */
#define STAGE_CLR       106           /* Not yet supported */
#define	STAGE_KILL      107
#define	STAGE_UPDC      108           /* Not yet supported */
#define	STAGE_INIT      109           /* Not yet supported */
#define	STAGE_CAT       110
#define	STAGE_ALLOC     111           /* Not yet supported */
#define	STAGE_GET       112           /* Not yet supported */
#define	STAGE_MIGPOOL   113           /* Not yet supported */
#define STAGE_FILCHG    114           /* Not yet supported */

/* -------------------------------------------------------- */
/* Generic STAGE_IN/STAGE_OUT/STAGE_WRT/STAGE_CAT interface */
/* -------------------------------------------------------- */
EXTERN_C int DLL_DECL stage_iowc _PROTO((int,                       /* req_type */
                                         char,                      /* t_or_d */
                                         u_signed64,                /* flags */
                                         mode_t,                    /* mode */
                                         char *,                    /* hostname */
                                         char *,                    /* pooluser */
                                         int,                       /* nstcp_input */
                                         struct stgcat_entry *,     /* stcp_input */
                                         int *,                     /* nstcp_output */
                                         struct stgcat_entry **,    /* stcp_output */
                                         int,                       /* nstpp_input */
                                         struct stgpath_entry *));  /* stpp_input */
#define stagein_tape(flags,hostname,pooluser,nstcp_input,stcp_input,nstcp_output,stcp_output,nstpp_input,stpp_input) \
  stage_iowc(STAGE_IN, 't',flags,0,hostname,pooluser,nstcp_input,stcp_input,nstcp_output,stcp_output,nstpp_input,stpp_input)
#define stagein_disk(flags,hostname,pooluser,nstcp_input,stcp_input,nstcp_output,stcp_output,nstpp_input,stpp_input) \
  stage_iowc(STAGE_IN, 'd',flags,0,hostname,pooluser,nstcp_input,stcp_input,nstcp_output,stcp_output,nstpp_input,stpp_input)
#define stagein_hsm(flags,hostname,pooluser,nstcp_input,stcp_input,nstcp_output,stcp_output,nstpp_input,stpp_input) \
  stage_iowc(STAGE_IN, 'm',flags,0,hostname,pooluser,nstcp_input,stcp_input,nstcp_output,stcp_output,nstpp_input,stpp_input)
#define stageout_tape(flags,mode,hostname,pooluser,nstcp_input,stcp_input,nstcp_output,stcp_output,nstpp_input,stpp_input) \
  stage_iowc(STAGE_OUT,'t',flags,mode,hostname,pooluser,nstcp_input,stcp_input,nstcp_output,stcp_output,nstpp_input,stpp_input)
#define stageout_disk(flags,mode,hostname,pooluser,nstcp_input,stcp_input,nstcp_output,stcp_output,nstpp_input,stpp_input) \
  stage_iowc(STAGE_OUT,'d',flags,mode,hostname,pooluser,nstcp_input,stcp_input,nstcp_output,stcp_output,nstpp_input,stpp_input)
#define stageout_hsm(flags,mode,hostname,poolname,pooluser,nstcp_input,stcp_input,nstcp_output,stcp_output,nstpp_input,stpp_input) \
  stage_iowc(STAGE_OUT,'m',flags,mode,hostname,pooluser,nstcp_input,stcp_input,nstcp_output,stcp_output,nstpp_input,stpp_input)
#define stagewrt_tape(flags,mode,hostname,pooluser,nstcp_input,stcp_input,nstcp_output,stcp_output,nstpp_input,stpp_input) \
  stage_iowc(STAGE_WRT,'t',flags,mode,hostname,pooluser,nstcp_input,stcp_input,nstcp_output,stcp_output,nstpp_input,stpp_input)
#define stagewrt_disk(flags,mode,hostname,pooluser,nstcp_input,stcp_input,nstcp_output,stcp_output,nstpp_input,stpp_input) \
  stage_iowc(STAGE_WRT,'d',flags,mode,hostname,pooluser,nstcp_input,stcp_input,nstcp_output,stcp_output,nstpp_input,stpp_input)
#define stagewrt_hsm(flags,mode,hostname,pooluser,nstcp_input,stcp_input,nstcp_output,stcp_output,nstpp_input,stpp_input) \
  stage_iowc(STAGE_WRT,'m',flags,mode,hostname,pooluser,nstcp_input,stcp_input,nstcp_output,stcp_output,nstpp_input,stpp_input)
#define stagecat_tape(flags,hostname,pooluser,nstcp_input,stcp_input,nstcp_output,stcp_output,nstpp_input,stpp_input) \
  stage_iowc(STAGE_CAT,'t',flags,0,hostname,pooluser,nstcp_input,stcp_input,nstcp_output,stcp_output,nstpp_input,stpp_input)
#define stagecat_disk(flags,hostname,pooluser,nstcp_input,stcp_input,nstcp_output,stcp_output,nstpp_input,stpp_input) \
  stage_iowc(STAGE_CAT,'d',flags,0,hostname,pooluser,nstcp_input,stcp_input,nstcp_output,stcp_output,nstpp_input,stpp_input)
#define stagecat_hsm(flags,hostname,pooluser,nstcp_input,stcp_input,nstcp_output,stcp_output,nstpp_input,stpp_input) \
  stage_iowc(STAGE_CAT,'m',flags,0,hostname,pooluser,nstcp_input,stcp_input,nstcp_output,stcp_output,nstpp_input,stpp_input)

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

/* ---------------------------- */
/* Generic STAGE_QRY interface */
/* ---------------------------- */
EXTERN_C int DLL_DECL stage_qry _PROTO((char,                      /* t_or_d */
                                        u_signed64,                /* flags */
                                        char *,                    /* hostname */
                                        int,                       /* nstcp_input */
                                        struct stgcat_entry *,     /* stcp_input */
                                        int *,                     /* nstcp_output */
                                        struct stgcat_entry **,    /* stcp_output */
                                        int *,                     /* nstpp_output */
                                        struct stgpath_entry **)); /* stpp_output */
#define stageqry_tape(flags,hostname,nstcp_input,stcp_input,nstcp_output,stcp_output,nstpp_output,stpp_output) \
  stage_qry('t',flags,hostname,nstcp_input,stcp_input,nstcp_output,stcp_output,nstpp_output,stpp_output)
#define stageqry_disk(flags,hostname,nstcp_input,stcp_input,nstcp_output,stcp_output,nstpp_output,stpp_output) \
  stage_qry('d',flags,hostname,nstcp_input,stcp_input,nstcp_output,stcp_output,nstpp_output,stpp_output)
#define stageqry_hsm(flags,hostname,nstcp_input,stcp_input,nstcp_output,stcp_output,nstpp_output,stpp_output) \
  stage_qry('m',flags,hostname,nstcp_input,stcp_input,nstcp_output,stcp_output,nstpp_output,stpp_output)

/* --------------------------- */
/* Generic STAGE_PUT interface */
/* --------------------------- */
/* Not available yet - only old protocol */
EXTERN_C int  DLL_DECL  stage_put_hsm _PROTO((char *, int, stage_hsm_t *));

/* --------------------------- */
/* Utilities used by the API   */
/* --------------------------- */
EXTERN_C int  DLL_DECL  send2stgd _PROTO((char *, int, u_signed64, char *, int, int, char *, int, int, struct stgcat_entry *, int *, struct stgcat_entry **, int *, struct stgpath_entry **));
/* Compatibility mode */
EXTERN_C int  DLL_DECL  send2stgd_compat _PROTO((char *, char *, int, int, char *, int)); /* Old version without forking */
EXTERN_C void DLL_DECL stage_sleep _PROTO((int)); /* Sleep thread-safe */

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
EXTERN_C int  DLL_DECL  stage_stcp2buf _PROTO((char *, size_t, u_signed64, struct stgcat_entry *));
EXTERN_C int  DLL_DECL  stage_buf2stcp _PROTO((struct stgcat_entry *, char *, size_t));
EXTERN_C int  DLL_DECL  stage_buf2stpp _PROTO((struct stgpath_entry *, char *, size_t));
EXTERN_C int  DLL_DECL  stage_getuniqueid _PROTO((u_signed64 *));
EXTERN_C int  DLL_DECL  stage_setuniqueid _PROTO((u_signed64));
EXTERN_C int  DLL_DECL  stage_getlaststghost _PROTO((char **));
EXTERN_C int  DLL_DECL  stage_setlaststghost _PROTO((char *));

#endif /* __stage_api_h */
