/*
 * $Id: Cgetopt.h,v 1.2 2000/05/12 15:50:59 jdurand Exp $
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
EXTERN_C int  DLL_DECL  *C__Copterr _PROTO((void));
EXTERN_C int  DLL_DECL  *C__Coptind _PROTO((void));
EXTERN_C int  DLL_DECL  *C__Coptopt _PROTO((void));
EXTERN_C int  DLL_DECL  *C__Coptreset _PROTO((void));
EXTERN_C char DLL_DECL **C__Coptarg _PROTO((void));

/*
 * Thread safe serrno. Note, C__serrno is defined in Cglobals.c rather
 * rather than serror.c .
 */
#define Copterr (*C__Copterr())
#define Coptind (*C__Coptind())
#define Coptopt (*C__Coptopt())
#define Coptreset (*C__Coptreset())
#define Coptarg (*C__Coptarg())

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
