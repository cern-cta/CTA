/*
  rm_api.h,v 1.3 2003/12/02 17:35:26 jdurand Exp
*/

#ifndef __rm_api_h
#define __rm_api_h

#include "Cglobals.h"
#include "rm_limits.h"
#include "rm_struct.h"

EXTERN_C int DLL_DECL rm_seterrbuf _PROTO((char *, int));
EXTERN_C int DLL_DECL rm_setoutbuf _PROTO((char *, int));
EXTERN_C int DLL_DECL rm_setlog    _PROTO((void (*)(int, char*)));
EXTERN_C int DLL_DECL rm_geterrbuf _PROTO((char **, int *));
EXTERN_C int DLL_DECL rm_getoutbuf _PROTO((char **, int *));
EXTERN_C int DLL_DECL rm_getlog    _PROTO((void (**)(int, char*)));
EXTERN_C int DLL_DECL rm_errmsg _PROTO((char *, char *, ...));
EXTERN_C int DLL_DECL rm_outmsg _PROTO((char *, char *, ...));
EXTERN_C int DLL_DECL rm_enterjob _PROTO((char *, int, u_signed64, struct rmjob *, int *, struct rmjob **));
EXTERN_C int DLL_DECL rm_modifyjob _PROTO((char *, int, u_signed64, struct rmjob *, int *, struct rmjob **));
EXTERN_C int DLL_DECL rm_deletejob _PROTO((char *, int, u_signed64, struct rmjob *, int *, struct rmjob **));
EXTERN_C int DLL_DECL rm_getjobs _PROTO((char *, int, u_signed64, struct rmjob *, int *, struct rmjob **));
EXTERN_C int DLL_DECL rm_getnodes _PROTO((char *, int, u_signed64, struct rmnode *, int *, struct rmnode **));
EXTERN_C int DLL_DECL usersend2rmd _PROTO((char *, int, char *, int, int *, struct rmjob **, int *, struct rmnode **));
EXTERN_C int DLL_DECL rm_freejob _PROTO((struct rmjob *));
EXTERN_C int DLL_DECL rm_freenode _PROTO((struct rmnode *));
EXTERN_C int DLL_DECL rm_freejobs _PROTO((int, struct rmjob *));
EXTERN_C int DLL_DECL rm_freenodes _PROTO((int, struct rmnode *));
EXTERN_C int DLL_DECL rm_printjob _PROTO((struct rmjob *));
EXTERN_C int DLL_DECL rm_printjob_relevant _PROTO((struct rmjob *));
EXTERN_C int DLL_DECL rm_printnode _PROTO((struct rmnode *));
EXTERN_C int DLL_DECL rm_printnode_relevant _PROTO((struct rmnode *));
EXTERN_C int DLL_DECL rm_dumpjob _PROTO((int *, struct rmjob *, int (*) _PROTO((int *, int, ...))));
EXTERN_C int DLL_DECL rm_dumpjob_relevant _PROTO((int *, struct rmjob *, int (*) _PROTO((int *, int, ...))));
EXTERN_C int DLL_DECL rm_dumpnode _PROTO((int *, struct rmnode *, int (*) _PROTO((int *, int, ...))));
EXTERN_C int DLL_DECL rm_dumpnode_relevant _PROTO((int *, struct rmnode *, int (*) _PROTO((int *, int, ...))));
EXTERN_C int DLL_DECL funcrep_to_log _PROTO((int *, int, ...));
EXTERN_C void DLL_DECL rm_time _PROTO((time_t, char *));
EXTERN_C char DLL_DECL **C__funcrep_name _PROTO(());
EXTERN_C int DLL_DECL rm_adminnode _PROTO((char *, int, char *, char *, u_signed64, char *, char *, char *, char *, char *, char *));

#define funcrep_name (*C__funcrep_name())

#endif /* __rm_api_h */
