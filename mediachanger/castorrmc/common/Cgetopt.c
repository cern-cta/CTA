/*
 * Copyright (c) 1987, 1993, 1994, 1996
 *	The Regents of the University of California.  All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 * 3. All advertising materials mentioning features or use of this software
 *    must display the following acknowledgement:
 *	This product includes software developed by the University of
 *	California, Berkeley and its contributors.
 * 4. Neither the name of the University nor the names of its contributors
 *    may be used to endorse or promote products derived from this software
 *    without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE REGENTS AND CONTRIBUTORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL THE REGENTS OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */

/* ============== */
/* System headers */
/* ============== */
#include <stdlib.h>         /* For getenv, etc... */
#include <stdio.h>
#include <string.h>

/* ============= */
/* Local headers */
/* ============= */
#include <Cglobals.h>
#include <Cgetopt.h>        /* Our decls, especially TSD's */
#include <Castor_limits.h>  /* For CA_MAXLINELEN */

/* =================== */
/* Internal prototypes */
/* =================== */
static char * _Cgetopt_progname (char *);
int _Cgetopt_internal (int, char * const *, const char *);
static char **C__place ();
static int my_place = -1; /* If Cglobals_get error in order not to crash */
#define place (*C__place())
static char *my_place_static = "";

static char * _Cgetopt_progname(char * nargv0)
{
  char * tmp = strrchr(nargv0, '/');
  if (tmp) tmp++; else tmp = nargv0;
  return(tmp);
}

#define	BADCH	(int)'?'
#define	BADARG	(int)':'
#define	EMSG	""

/*
 * Cgetopt --
 *	Parse argc/argv argument vector.
 */
int
_Cgetopt_internal(int nargc,
                      char * const *nargv,
                      const char *ostr)
{
  char *oli;				/* option letter list index */
  
  if (nargv == NULL) {
    /* Cannot be */
    return(-1);
  }

  if (ostr == NULL) {
    int i;

    /* Check if there might be long options */
    for (i = 0; i < nargc; i++) {
      if (strstr(nargv[i],"--") != NULL) {
        return(-2);
      }
    }
    /* Nope */
    return(-1);

  }

  if (Coptreset || !*place) {		/* update scanning pointer */
    Coptreset = 0;
    if (Coptind >= nargc || *(place = nargv[Coptind]) != '-') {
      place = EMSG;
      return (-1);
    }
    if (place[1] && *++place == '-') {	/* found "--" */
      /* ++Coptind; */
      place = EMSG;
      return (-2);
    }
  }					/* option letter okay? */
  if ((Coptopt = (int)*place++) == (int)':' ||
      !(oli = strchr(ostr, Coptopt))) {
    /*
     * if the user didn't specify '-' as an option,
     * assume it means -1.
     */
    if (Coptopt == (int)'-')
      return (-1);
    if (!*place)
      ++Coptind;
    if (Copterr && *ostr != ':')
      (void)fprintf(stderr,
                    "%s: illegal option -- %c\n", _Cgetopt_progname(nargv[0]), Coptopt);
    return (BADCH);
  }
  if (*++oli != ':') {			/* don't need argument */
    Coptarg = NULL;
    if (!*place)
      ++Coptind;
  } else {				/* need an argument */
    if (*place)			/* no white space */
      Coptarg = place;
    else if (nargc <= ++Coptind) {	/* no arg */
      place = EMSG;
      if ((Copterr) && (*ostr != ':'))
        (void)fprintf(stderr,
                      "%s: option requires an argument -- %c\n",
                      _Cgetopt_progname(nargv[0]), Coptopt);
      return (BADARG);
    } else				/* white space */
      Coptarg = nargv[Coptind];
    place = EMSG;
    ++Coptind;
  }
  return (Coptopt);			/* dump back option letter */
}

/*
 * Cgetopt --
 *	Parse argc/argv argument vector.
 */
int 
Cgetopt(int nargc,
             char * const *nargv,
             const char *ostr)
{
  int retval;

  if ((retval = _Cgetopt_internal(nargc, nargv, ostr)) == -2) {
    retval = -1;
    ++Coptind; 
  }
  return(retval);
}

/*
 * Cgetopt_long --
 *	Parse argc/argv argument vector.
 */
int
Cgetopt_long(int nargc,
                 char **nargv,
                 const char *options,
                 Coptions_t *long_options,
                 int *index)
{
  int retval;

  if (options == NULL && long_options == NULL) {
    /* No option at all ? */
    return(-1);
  }

  if ((retval = _Cgetopt_internal(nargc, nargv, options)) == -2) {
    char *current_argv = nargv[Coptind++] + 2, *has_equal;
    int i, current_argv_len, match = -1;
    int exact = 0;
    int ambig = 0;

    if (*current_argv == '\0') {
      return(-1);
    }
    if ((has_equal = strchr(current_argv, '=')) != NULL) {
      current_argv_len = has_equal - current_argv;
      has_equal++;
    } else
      current_argv_len = strlen(current_argv);

    for (i = 0; long_options[i].name; i++) { 
      if (strncmp(current_argv, long_options[i].name, current_argv_len)) {
        continue;
      }
      if (strlen(long_options[i].name) == (unsigned)current_argv_len) { 
        /* Exact match found */
        match = i;
        exact = 1;
        break;
      }
      if (match == -1) {
        /* First non-exact match found */
        match = i;
      } else {
        /* Second or later non-exact match found */
        ambig = 1;
      }
    }
    if (ambig && ! exact) {
      if (options != NULL) {
        if ((Copterr) && (*options != ':'))
          (void)fprintf(stderr,
                        "%s: option is ambiguous -- %s\n", _Cgetopt_progname(nargv[0]), current_argv);
        return (BADCH);
      } else {
        return (-1);
      }
    }
    if (match != -1) {
      if (long_options[match].has_arg == REQUIRED_ARGUMENT ||
          long_options[match].has_arg == OPTIONAL_ARGUMENT) {
        if (has_equal) {
          Coptarg = has_equal;
        } else {
          Coptarg = nargv[Coptind++];
        }
      }
      if ((long_options[match].has_arg == REQUIRED_ARGUMENT) && (Coptarg == NULL)) {
        /*
         * Missing argument, leading :
         * indicates no error should be generated
         */
        if ((Copterr) && (*options != ':')) {
          (void)fprintf(stderr,
                        "%s: option requires an argument -- %s\n",
                        _Cgetopt_progname(nargv[0]), current_argv);
        }
        return (BADARG);
      }
    } else { /* No matching argument */
      if (options != NULL) {
        if ((Copterr) && (*options != ':'))
          (void)fprintf(stderr,
                        "%s: illegal option -- %s\n", _Cgetopt_progname(nargv[0]), current_argv);
        return (BADCH);
      } else {
        return (-1);
      }
    }
    if (long_options[match].flag != NULL) {
      *long_options[match].flag = long_options[match].val;
      retval = 0;
    } else {
      retval = long_options[match].val;
    }
    if (index != NULL)
      *index = match;
  }
  return(retval);
}

static char **C__place() {
	char **var;
	/* Call Cglobals_get */
	if (Cglobals_get(&my_place,
					 (void **) &var,
					 sizeof(char *)) > 0) {
		/* First time : init value is EMSG */
		/* Reminder: **C_place is used like that: */
		/* place is (*C__place()), e.g. place is a */
		/* (char *), but place is always used as */
		/* pointer to a char, not a real string. */
		/* In the un-thread-safe version of Cgetopt */
		/* e;g. prior to revision 1.6, place was */
		/* declared like that: static char *place = EMSG */
		/* Since now we are a true variable, we must point */
		/* to something instead of having that static */
		/* declaration. It is harmless to use my_place_static */
		/* as an initializer. The only important thing is that */
		/* var itself is not the same between threads */
		*var = my_place_static;
	}

	/* If error, var will be NULL */
	if (var == NULL)
	{
		return(&my_place_static);
	}
	return(var);
}
