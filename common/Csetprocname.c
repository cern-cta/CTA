/*
 * $Id: Csetprocname.c,v 1.1 2001/11/20 09:47:35 jdurand Exp $
 */

/*
 * Copyright (c) 1998-2001 Sendmail, Inc. and its suppliers.
 *	All rights reserved.
 * Copyright (c) 1983, 1995-1997 Eric P. Allman.  All rights reserved.
 * Copyright (c) 1988, 1993
 *	The Regents of the University of California.  All rights reserved.
 *
 * By using this file, you agree to the terms and conditions set
 * forth in the LICENSE file which can be found at the top level of
 * the sendmail distribution.
 *
 */

#include "Csetprocname.h"
#include <stdlib.h>
#include <string.h>
#include <serrno.h>
#if defined(__STDC__) || defined(__cplusplus)
#include <stdarg.h>
#else
#include <varargs.h>
#endif

/* Possible modes of changing ps output */
#define SPT_NONE	0	/* don't use it at all */
#define SPT_REUSEARGV	1	/* cover argv with name information [QNX] */
#define SPT_BUILTIN	2	/* use libc builtin [BSDs] */
#define SPT_PSTAT	3	/* use pstat(PSTAT_SETCMD, ...) [HPUX] */
#define SPT_PSSTRINGS	4	/* use PS_STRINGS->... [APPLE,4.4BSD]] */
#define SPT_SYSMIPS	5	/* use sysmips() supported by NEWS-OS 6 */
#define SPT_SCO		6	/* write kernel u. area [SCO UNIX 3.2v4.0 Open Desktop 2.0 and earlier] */
#define SPT_CHANGEARGV	7	/* write our own strings into argv[] [Next] */

/* Set explicitely the platforms on which we do something:
 * AIX
 * HP-UX
 * Linux
 * Tru64
 */
#if ! (defined(_AIX) || defined(hpux) || defined(linux) || (defined(__osf__) && defined(__alpha)))

/* ================================================================= */

/* We prefer to explicitely do NOTHING rather than execute dummy code */
/* This is safer -; */
int DLL_DECL Cinitsetprocname(argc, argv, envp)
     int argc;
     char **argv;
     char **envp;
{
  return(0);
}

int DLL_DECL 
#if defined(__STDC__) || defined(__cplusplus)
Csetprocname(CONST char *fmt, ...)
#else /* __STDC__ */
     Csetprocname(fmt, va_alist)
     CONST char *fmt;
     va_dcl
#endif /* __STDC__ */
{
  return(0);
}

/* ================================================================= */

#else /* ! (defined(_AIX) || defined(hpux) || defined(linux) || (defined(__osf__) && defined(__alpha))) */

/* ================================================================= */

/* Section OS-specific */
#if _AIX
#define SPT_PADCHAR	'\0'	/* pad process name with nulls */
#endif
#if hpux
#define SPT_TYPE	SPT_PSTAT
#endif

/*
 * Copyright (c) 2000-2001 Sendmail, Inc. and its suppliers.
 *      All rights reserved.
 *
 * By using this file, you agree to the terms and conditions set
 * forth in the LICENSE file which can be found at the top level of
 * the sendmail distribution.
 *
 *      Id: varargs.h,v 1.7 2001/09/13 16:45:40 ca Exp
 */

/*
**  libsm variable argument lists
*/

#if defined(__STDC__) || defined(__cplusplus)
# define SM_VA_STD 1
# define SM_VA_START(ap, f)    va_start(ap, f)
#else /* defined(__STDC__) || defined(__cplusplus) */
# define SM_VA_STD 0
# define SM_VA_START(ap, f)    va_start(ap)
#endif /* defined(__STDC__) || defined(__cplusplus) */

#if defined(va_copy)
# define SM_VA_COPY(dst, src)  va_copy((dst), (src))
#elif defined(__va_copy)
# define SM_VA_COPY(dst, src)  __va_copy((dst), (src))
#else
# define SM_VA_COPY(dst, src)  (dst) = (src)
#endif

/*
**  The following macros are useless, but are provided for symmetry.
*/

#define SM_VA_LOCAL_DECL       va_list ap
#define SM_VA_ARG(ap, type)    va_arg(ap, type)
#define SM_VA_END(ap)          va_end(ap)

/* max line length */
#ifdef MAXLINE
#undef MAXLINE
#endif
#define MAXLINE 2048

/*
**  SETPROCNAME -- set process name for ps
**
**	Parameters:
**		fmt -- a printf style format string.
**		a, b, c -- possible parameters to fmt.
**
**	Returns:
**		none.
**
**	Side Effects:
**		Clobbers argv of our main procedure so ps(1) will
**		display the name.
*/
#ifndef SPT_TYPE
# define SPT_TYPE	SPT_REUSEARGV
#endif /* ! SPT_TYPE */


#if SPT_TYPE != SPT_NONE && SPT_TYPE != SPT_BUILTIN

# if SPT_TYPE == SPT_PSTAT
#  include <sys/pstat.h>
# endif /* SPT_TYPE == SPT_PSTAT */
# if SPT_TYPE == SPT_PSSTRINGS
#  include <machine/vmparam.h>
#  include <sys/exec.h>
#  ifndef PS_STRINGS	/* hmmmm....  apparently not available after all */
#   undef SPT_TYPE
#   define SPT_TYPE	SPT_REUSEARGV
#  else /* ! PS_STRINGS */
#   ifndef NKPDE			/* FreeBSD 2.0 */
#    define NKPDE 63
typedef unsigned int	*pt_entry_t;
#   endif /* ! NKPDE */
#  endif /* ! PS_STRINGS */
# endif /* SPT_TYPE == SPT_PSSTRINGS */

# if SPT_TYPE == SPT_PSSTRINGS || SPT_TYPE == SPT_CHANGEARGV
#  define SETPROC_STATIC	static
# else /* SPT_TYPE == SPT_PSSTRINGS || SPT_TYPE == SPT_CHANGEARGV */
#  define SETPROC_STATIC
# endif /* SPT_TYPE == SPT_PSSTRINGS || SPT_TYPE == SPT_CHANGEARGV */

# if SPT_TYPE == SPT_SYSMIPS
#  include <sys/sysmips.h>
#  include <sys/sysnews.h>
# endif /* SPT_TYPE == SPT_SYSMIPS */

# if SPT_TYPE == SPT_SCO
#  include <sys/immu.h>
#  include <sys/dir.h>
#  include <sys/user.h>
#  include <sys/fs/s5param.h>
#  if PSARGSZ > MAXLINE
#   define SPT_BUFSIZE	PSARGSZ
#  endif /* PSARGSZ > MAXLINE */
# endif /* SPT_TYPE == SPT_SCO */

# ifndef SPT_PADCHAR
#  define SPT_PADCHAR	' '
# endif /* ! SPT_PADCHAR */

#endif /* SPT_TYPE != SPT_NONE && SPT_TYPE != SPT_BUILTIN */

#ifndef SPT_BUFSIZE
# define SPT_BUFSIZE	MAXLINE
#endif /* ! SPT_BUFSIZE */

/*
**  Pointers for setprocname.
**	This allows "ps" listings to give more useful information.
*/

static char buf[SPT_BUFSIZE];   /* Automatically initialized to zeroes */
static char	**Argv = NULL;		/* pointer to argument vector */
static char	*LastArgv = NULL;	/* end of argv */
#if SPT_TYPE != SPT_BUILTIN
static int	__Csetprocname _PROTO((CONST char *, ...));
#endif /* SPT_TYPE != SPT_BUILTIN */
static size_t __sm_strlcpy _PROTO((register char *, register CONST char *, ssize_t));

int DLL_DECL Cinitsetprocname(argc, argv, envp)
     int argc;
     char **argv;
     char **envp;
{
  register int i;
  extern char **environ;
  char **thisenviron;

#ifdef __INSURE__
  /* This routine is declared UNSAFE - In particular Insure will detect
   * overflow when argv[0] is reused
   */
  _Insure_set_option("runtime","off");
#endif

  /*
  **  Move the environment so setprocname can use the space at
  **  the top of memory.
  */
  
  for (i = 0; envp[i] != NULL; i++)
    continue;
  if ((thisenviron = (char **) malloc(sizeof (char *) * (i + 1))) == NULL) {
    Argv = NULL;
    serrno = SEINTERNAL;
#ifdef __INSURE__
  /* Restore runtime checking */
    _Insure_set_option("runtime","on");
#endif
    return(-1);
  }
  
  for (i = 0; envp[i] != NULL; i++) {
    size_t len = strlen(envp[i]) + 1;
    char *p = (char *) malloc(len);
    if (p == NULL) {
      int j;

      for (j = 0; j < i; j++)
        free(thisenviron[i]);
      free(thisenviron);
      Argv = NULL;
      serrno = SEINTERNAL;
#ifdef __INSURE__
      /* Restore runtime checking */
      _Insure_set_option("runtime","on");
#endif
      return(-1);
    }
    thisenviron[i] = strcpy(p, envp[i]);
  }
  thisenviron[i] = NULL;

  environ = thisenviron;

  /*
  **  Save start and extent of argv for setprocname.
  */
  
  Argv = argv;
  
  /*
  **  Determine how much space we can use for setprocname.
  **  Use all contiguous argv and envp pointers starting at argv[0]
  */
  for (i = 0; i < argc; i++)
	{
      if (i == 0 || LastArgv + 1 == argv[i])
        LastArgv = argv[i] + strlen(argv[i]);
	}
  for (i = 0; LastArgv != NULL && envp[i] != NULL; i++)
	{
      if (LastArgv + 1 == envp[i])
        LastArgv = envp[i] + strlen(envp[i]);
	}
#ifdef __INSURE__
  /* Restore runtime checking */
  _Insure_set_option("runtime","on");
#endif
  return(0);
}


#if SPT_TYPE != SPT_BUILTIN

/*VARARGS1*/
static int
# ifdef __STDC__
__Csetprocname(CONST char *fmt, ...)
# else /* __STDC__ */
     __Csetprocname(fmt, va_alist)
     CONST char *fmt;
     va_dcl
# endif /* __STDC__ */
{
# if SPT_TYPE != SPT_NONE
  register int i;
  register char *p;
  SM_VA_LOCAL_DECL;
#  if SPT_TYPE == SPT_PSTAT
  union pstun pst;
#  endif /* SPT_TYPE == SPT_PSTAT */
#  if SPT_TYPE == SPT_SCO
  int j;
  off_t seek_off;
  static int kmem = -1;
  static pid_t kmempid = -1;
  static pid_t CurrentPid = getpid();
  struct user u;
#  endif /* SPT_TYPE == SPT_SCO */

#  ifdef __INSURE__
  /* This routine is declared UNSAFE - In particular Insure will detect
   * overflow when argv[0] is reused
   */
  _Insure_set_option("runtime","off");
#  endif

  p = buf;
  
  /* print the argument string */
  SM_VA_START(ap, fmt);
#  if (defined(__osf__) && defined(__alpha))
  vsprintf (p, fmt, ap);
#  else
#  if defined(_WIN32)
  _vsnprintf (p, SPT_BUFSIZE - strlen(p), fmt, ap);
#  else
  vsnprintf (p, SPT_BUFSIZE - strlen(p), fmt, ap);
#  endif
#  endif
  SM_VA_END(ap);
  p[SPT_BUFSIZE-1] = '\0';

  i = (int) strlen(buf);

  if (i < 0) {
    serrno = SEINTERNAL;
#  ifdef __INSURE__
    /* Restore runtime checking */
    _Insure_set_option("runtime","on");
#  endif
    return(-1);
  }

#  if SPT_TYPE == SPT_PSTAT
  pst.pst_command = buf;
  if (pstat(PSTAT_SETCMD, pst, i, 0, 0) < 0) {
    serrno = SEINTERNAL;
#  ifdef __INSURE__
    /* Restore runtime checking */
    _Insure_set_option("runtime","on");
#  endif
    return(-1);
  }
#  endif /* SPT_TYPE == SPT_PSTAT */
#  if SPT_TYPE == SPT_PSSTRINGS
  PS_STRINGS->ps_nargvstr = 1;
  PS_STRINGS->ps_argvstr = buf;
#  endif /* SPT_TYPE == SPT_PSSTRINGS */
#  if SPT_TYPE == SPT_SYSMIPS
  sysmips(SONY_SYSNEWS, NEWS_SETPSARGS, buf);
#  endif /* SPT_TYPE == SPT_SYSMIPS */
#  if SPT_TYPE == SPT_SCO
  if (kmem < 0 || kmempid != CurrentPid)
	{
      if (kmem >= 0)
        (void) close(kmem);
      kmem = open(_PATH_KMEM, O_RDWR, 0);
      if (kmem < 0) {
        serrno = SEINTERNAL;
#  ifdef __INSURE__
        /* Restore runtime checking */
        _Insure_set_option("runtime","on");
#  endif
        return(-1);
      }
      if ((j = fcntl(kmem, F_GETFD, 0)) < 0 ||
          fcntl(kmem, F_SETFD, j | FD_CLOEXEC) < 0)
		{
          (void) close(kmem);
          kmem = -1;
          serrno = SEINTERNAL;
#  ifdef __INSURE__
          /* Restore runtime checking */
          _Insure_set_option("runtime","on");
#  endif
          return(-1);
		}
      kmempid = CurrentPid;
	}
  buf[PSARGSZ - 1] = '\0';
  seek_off = UVUBLK + (off_t) u.u_psargs - (off_t) &u;
  if (lseek(kmem, (off_t) seek_off, SEEK_SET) == seek_off)
    (void) write(kmem, buf, PSARGSZ);
#  endif /* SPT_TYPE == SPT_SCO */
#  if SPT_TYPE == SPT_REUSEARGV
  if (LastArgv == NULL) {
    serrno = SEINTERNAL;
#  ifdef __INSURE__
    /* Restore runtime checking */
    _Insure_set_option("runtime","on");
#  endif
    return(-1);
  }
  
  if (i > LastArgv - Argv[0] - 2)
	{
      i = LastArgv - Argv[0] - 2;
      buf[i] = '\0';
	}
  (void) __sm_strlcpy(Argv[0], buf, i + 1);
  p = &Argv[0][i];
  while (p < LastArgv) {
    *p++ = SPT_PADCHAR;
  }
  Argv[1] = NULL;
#  endif /* SPT_TYPE == SPT_REUSEARGV */
#  if SPT_TYPE == SPT_CHANGEARGV
  Argv[0] = buf;
  Argv[1] = 0;
#  endif /* SPT_TYPE == SPT_CHANGEARGV */
#  ifdef __INSURE__
  /* Restore runtime checking */
  _Insure_set_option("runtime","on");
#  endif
# endif /* SPT_TYPE != SPT_NONE */
  return(0);
}

#endif /* SPT_TYPE != SPT_BUILTIN */

/*
**  SM_STRLCPY -- size bounded string copy
**
**      This is a bounds-checking variant of strcpy.
**      If size > 0, copy up to size-1 characters from the nul terminated
**      string src to dst, nul terminating the result.  If size == 0,
**      the dst buffer is not modified.
**      Additional note: this function has been "tuned" to run fast and tested
**      as such (versus versions in some OS's libc).
**
**      The result is strlen(src).  You can detect truncation (not all
**      of the characters in the source string were copied) using the
**      following idiom:
**
**              char *s, buf[BUFSIZ];
**              ...
**              if (sm_strlcpy(buf, s, sizeof(buf)) >= sizeof(buf))
**                      goto overflow;
**
**      Parameters:
**              dst -- destination buffer
**              src -- source string
**              size -- size of destination buffer
**
**      Returns:
**              strlen(src)
*/

static size_t __sm_strlcpy(dst, src, size)
     register char *dst;
     register CONST char *src;
     ssize_t size;
{
  register ssize_t i;
  
  if (size-- <= 0)
    return strlen(src);
  for (i = 0; i < size && (dst[i] = src[i]) != 0; i++)
    continue;
  dst[i] = '\0';
  if (src[i] == '\0')
    return i;
  else
    return i + strlen(src + i);
}

/*
**  SM_SETPROCNAME -- set process task and set process name for ps
**
**	Possibly set process status and call setprocname() to
**	change the ps display.
**
**	Parameters:
**		status -- whether or not to store as process status
**		e -- the current envelope.
**		fmt -- a printf style format string.
**		a, b, c -- possible parameters to fmt.
**
**	Returns:
**		none.
*/

/*VARARGS2*/
int DLL_DECL 
#if defined(__STDC__) || defined(__cplusplus)
Csetprocname(CONST char *fmt, ...)
#else /* __STDC__ */
     Csetprocname(fmt, va_alist)
     CONST char *fmt;
     va_dcl
#endif /* __STDC__ */
{
  char buf[SPT_BUFSIZE];
  SM_VA_LOCAL_DECL;
  
  
  /* print the argument string */
  SM_VA_START(ap, fmt);
  buf[0] = '\0';
#if (defined(__osf__) && defined(__alpha))
  vsprintf (buf, fmt, ap);
#else
#if defined(_WIN32)
  _vsnprintf (buf, SPT_BUFSIZE - strlen(buf), fmt, ap);
#else
  vsnprintf (buf, SPT_BUFSIZE - strlen(buf), fmt, ap);
#endif
#endif
  buf[SPT_BUFSIZE-1] = '\0';
  SM_VA_END(ap);

  return(__Csetprocname("%s", buf));
}

#endif /* ! (defined(_AIX) || defined(hpux) || defined(linux) || (defined(__osf__) && defined(__alpha))) */
