/*
 * Copyright (C) 1990-2000 by CERN/IT/PDP/DM
 * All rights reserved
 */

/* trace.c      General tracing facility                                */

#ifndef lint
static char sccsid[] = "@(#)trace.c,v 1.7 2000/05/31 10:33:54 CERN IT-PDP/DM Olof Barring";
#endif /* not lint */

#include <stdio.h>              /* standard input/output definitions    */
#include <stdlib.h>
#include <stdarg.h>             /* variable argument list definitions   */
#include <errno.h>              /* standard error numbers & codes       */
#include <serrno.h>
#include <Cglobals.h>
#include <trace.h>

int notrace = 0;

static int	trc_key = -1;
typedef struct trc_spec {
	int      _trace_initialized ;  /* trace initialized flag       */
	int      _trace_level ;        /* dynamic trace level          */
	int      _indent_level ;       /* dynamic indentation level    */
} trc_spec_t;
#define trace_level trc->_trace_level
#define trace_initialized trc->_trace_initialized
#define indent_level trc->_indent_level

extern  char* getenv();         /* get environment variable             */

void DLL_DECL print_trace(int level, char *label, char *format, ...)
{
	va_list args;           /* arguments                            */
	register int i;         /* general purpose index                */
	trc_spec_t *trc;
	int sav_errno = errno;
	int sav_serrno = serrno;

    va_start(args, format);
	Cglobals_get(&trc_key,(void **)&trc,sizeof(trc_spec_t));
	if (trace_level < level) return;
	for (i=0; i< level+indent_level ; i++)       {
		fprintf(stdout," "); /* indentation                    */
	}
	fprintf(stdout,"%s: ", label);
	(void) vfprintf(stdout, format, args);
	fprintf(stdout,"\n");
	va_end(args);
	fflush(stdout);
	errno = sav_errno;
	serrno = sav_serrno;
}

void DLL_DECL init_trace(name)  /* initialize trace level               */
char    *name;                  /* environment variable name            */
{
	register char    *p;    /* general purpose char. string pointer */
	trc_spec_t *trc;
	int sav_errno = errno;
	int sav_serrno = serrno;

	Cglobals_get(&trc_key,(void **)&trc,sizeof(trc_spec_t));
	if (!trace_initialized) {
		if ((p = getenv(name)) != NULL)       {
			if (atoi(p) != 0)       {
				trace_level = atoi(p);
				print_trace(0, "    **** ", "trace level set to %d", trace_level);
			}
		}
		trace_initialized++;
	}
	indent_level++;
	errno = sav_errno;
	serrno = sav_serrno;
}

void DLL_DECL end_trace()         /* end trace level                      */
{
	trc_spec_t *trc;
	int sav_errno = errno;
	int sav_serrno = serrno;

	Cglobals_get(&trc_key,(void **)&trc,sizeof(trc_spec_t));
	if (indent_level > 0) indent_level--;
	errno = sav_errno;
	serrno = sav_serrno;
}




void DLL_DECL print_trace_r(void *trace, int level, char *label, char *format, ...)
{
	va_list args;           /* arguments                            */
	register int i;         /* general purpose index                */
	trc_spec_t *trc;
	int sav_errno = errno;
	int sav_serrno = serrno;

	va_start(args, format);
	if (trace == NULL) return;
	trc = (trc_spec_t *)trace;
	if (trace_level < level) return;
	for (i=0; i< level+indent_level ; i++)       {
		fprintf(stdout," "); /* indentation                    */
	}
	fprintf(stdout,"%s: ", label);
	(void) vfprintf(stdout, format, args);
	fprintf(stdout,"\n");
	va_end(args);
	fflush(stdout);
	errno = sav_errno;
	serrno = sav_serrno;
}

void DLL_DECL init_trace_r(trace, name)  /* initialize trace level               */
void    **trace;
char    *name;                  /* environment variable name            */
{
	register char    *p;    /* general purpose char. string pointer */
	trc_spec_t *trc;
	int sav_errno = errno;
	int sav_serrno = serrno;

	*trace = calloc(1, sizeof(trc_spec_t));
	if (*trace == NULL) return;
	trc = (trc_spec_t *)*trace;
	if (!trace_initialized) {
		if ((p = getenv(name)) != NULL)       {
			if (atoi(p) != 0)       {
				trace_level = atoi(p);
				print_trace(0, "    **** ", "trace level set to %d", trace_level);
			}
		}
		trace_initialized++;
	}
	indent_level++;
	errno = sav_errno;
	serrno = sav_serrno;
}

void DLL_DECL end_trace_r(void *trace)         /* end trace level                      */
{
	trc_spec_t *trc;
	int sav_errno = errno;
	int sav_serrno = serrno;

	if (trace == NULL) return;
	trc = (trc_spec_t *)trace;
	if (indent_level > 0) indent_level--;
	errno = sav_errno;
	serrno = sav_serrno;
}
