/*
 * $Id: Cgetopt.h,v 1.1 2000/02/28 12:52:29 jdurand Exp $
 */

#ifndef __Cgetopt_h
#define __Cgetopt_h

#include <osdep.h>

struct Coptions {
  char *name;
  int has_arg;
  int *flag;
  int val;
};
typedef struct Coptions Coptions_t;

#define NO_ARGUMENT 0
#define REQUIRED_ARGUMENT 1
#define OPTIONAL_ARGUMENT 2

#if defined(_REENTRANT) || defined(_THREAD_SAFE) || \
   (defined(_WIN32) && (defined(_MT) || defined(_DLL)))
/*
 * Multi-thread (MT) environment
 */
EXTERN_C int  DLL_DECL  *__Copterr _PROTO((void));
EXTERN_C int  DLL_DECL  *__Coptind _PROTO((void));
EXTERN_C int  DLL_DECL  *__Coptopt _PROTO((void));
EXTERN_C int  DLL_DECL  *__Coptreset _PROTO((void));
EXTERN_C char DLL_DECL **__Coptarg _PROTO((void));

/*
 * Thread safe serrno. Note, __serrno is defined in Cglobals.c rather
 * rather than serror.c .
 */
#define Copterr (*__Copterr())
#define Coptind (*__Coptind())
#define Coptopt (*__Coptopt())
#define Coptreset (*__Coptreset())
#define Coptarg (*__Coptarg())

#else /* _REENTRANT || _THREAD_SAFE ... */
/*
 * non-MT environment
 */
extern  int     Copterr;
extern  int     Coptind;
extern  int     Coptopt;
extern  int     Coptreset;
extern  char   *Coptarg;
#endif /* _REENTRANT || _TREAD_SAFE */

EXTERN_C int DLL_DECL Cgetopt_config _PROTO(());
EXTERN_C int DLL_DECL Cgetopt _PROTO(());

#endif /* __Cgetopt_h */
