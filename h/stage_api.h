/*
 * $Id: stage_api.h,v 1.10 2000/06/01 06:52:54 jdurand Exp $
 */

#ifndef __stage_api_h
#define __stage_api_h

#include <osdep.h>
#include <stage.h>

#if hpux
/* On HP-UX seteuid() and setegid() do not exist and have to be wrapped */
/* calls to setresuid().                                                */
#define seteuid(euid) setresuid(-1,euid,-1)
#define setegid(egid) setresgid(-1,egid,-1)
#endif

struct stage_hsm {
  char *xfile;         /* Recommended size: (CA_MAXHOSTNAMELEN+MAXPATH)+1 */
  char *upath;         /* Recommended size: (CA_MAXHOSTNAMELEN+MAXPATH)+1 */
  u_signed64 size;
  struct stage_hsm *next;
};
typedef struct stage_hsm stage_hsm_t;

EXTERN_C int  DLL_DECL  send2stgd _PROTO((char *, char *, int, int, char *, int));
EXTERN_C int  DLL_DECL  stage_seterrbuf _PROTO((char *, int));
EXTERN_C int  DLL_DECL  stage_setoutbuf _PROTO((char *, int));
EXTERN_C int  DLL_DECL  stage_setlog    _PROTO((void (*)(int, char*)));
EXTERN_C int  DLL_DECL  stage_geterrbuf _PROTO((char **, int *));
EXTERN_C int  DLL_DECL  stage_getoutbuf _PROTO((char **, int *));
EXTERN_C int  DLL_DECL  stage_getlog    _PROTO((void (**)(int, char*)));
EXTERN_C int  DLL_DECL  stage_errmsg _PROTO(());
EXTERN_C int  DLL_DECL  stage_outmsg _PROTO(());
EXTERN_C int  DLL_DECL  stage_updc_filcp _PROTO((char *, int, char *, u_signed64, int, int, int, char *, char *, int, int, char *, char *));
EXTERN_C int  DLL_DECL  stage_updc_tppos _PROTO((char *, int, int, char *, char *, int, int, char *, char *));
EXTERN_C int  DLL_DECL  stage_updc_user _PROTO((char *, stage_hsm_t *));
EXTERN_C int  DLL_DECL  stage_put_hsm _PROTO((char *, int, stage_hsm_t *));
EXTERN_C int  DLL_DECL  stage_out_hsm _PROTO((char *, int, char *, stage_hsm_t *));
EXTERN_C int  DLL_DECL  stage_in_hsm _PROTO((char *, char *, stage_hsm_t *));
EXTERN_C int  DLL_DECL  stage_wrt_hsm _PROTO((char *, int, char *, stage_hsm_t *));
EXTERN_C int  DLL_DECL  stage_cat_hsm _PROTO((char *, char *, stage_hsm_t *));

#endif /* __stage_api_h */

