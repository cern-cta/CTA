/*
 * $Id: Cregexp_test.c,v 1.1 2002/09/11 13:31:49 jdurand Exp $
 */

/*
 * Simple test program for regexp(3) stuff.  Knows about debugging hooks.
 *
 *	Copyright (c) 1986 by University of Toronto.
 *	Written by Henry Spencer.  Not derived from licensed software.
 *
 *	Permission is granted to anyone to use this software for any
 *	purpose on any computer system, and to redistribute it freely,
 *	subject to the following restrictions:
 *
 *	1. The author is not responsible for the consequences of use of
 *		this software, no matter how awful, even if they arise
 *		from defects in it.
 *
 *	2. The origin of this software must not be misrepresented, either
 *		by explicit claim or by omission.
 *
 *	3. Altered versions must be plainly marked as such, and must not
 *		be misrepresented as being the original software.
 *
 * Usage: try re [string [output [-]]]
 * The re is compiled and dumped, regexeced against the string, the result
 * is applied to output using regsub().  The - triggers a running narrative
 * from regexec().  Dumping and narrative don't happen unless DEBUG.
 *
 * If there are no arguments, stdin is assumed to be a stream of lines with
 * five fields:  a r.e., a string to match it against, a result code, a
 * source string for regsub, and the proper result.  Result codes are 'c'
 * for compile failure, 'y' for match success, 'n' for match failure.
 * Field separator is tab.
 */
#include "Cthread_api.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "osdep.h"
#include "serrno.h"
#include "Cregexp.h"

int _Cregexp_test_multiple _PROTO(());
void _Cregexp_test_try _PROTO((char **));

#ifndef BUFSIZ
#define BUFSIZ 1024
#endif
char buf[BUFSIZ];

Cregexp_t badregexp;		/* Will be initialized to 0. */

int main(argc, argv)
     int argc;
     char *argv[];
{
  Cregexp_t *r;
  int i;
  int rc;

  if (argc == 1) {
    rc = _Cregexp_test_multiple();
    goto _Cregexp_test_return;
  }
  
  if ((r = Cregexp_comp(argv[1])) == NULL) {
    printf("### Cregexp_comp failure (%s)\n",sstrerror(serrno));
    rc = EXIT_FAILURE;
    goto _Cregexp_test_return;
  }
  if (argc > 2) {
    i = Cregexp_exec(r, argv[2]);
    printf("%s", i == 0 ? "OK" : "### Cregexp_exec failure");
    if (i != 0) {
      printf(" (%s)\n",sstrerror(serrno));
      rc = EXIT_FAILURE;
      goto _Cregexp_test_return;
    } else {
      for (i = 1; i < CREGEXP_NSUBEXP; i++)
        if (r->startp[i] != NULL && r->endp[i] != NULL)
          printf(" \\%d", i);
      printf("\n");
    }
  }
  if (r != NULL && argc > 3) {
    if (Cregexp_sub(r, argv[3], buf, BUFSIZ) != 0) {
      printf("### Cregexp_sub failure (%s)\n",sstrerror(serrno));
      rc = EXIT_FAILURE;
      goto _Cregexp_test_return;
    }
    printf("%s\n", buf);
  }

  rc = EXIT_SUCCESS;

  _Cregexp_test_return:

  return(rc);
}

int _Cregexp_test_multiple() {
  char rbuf[BUFSIZ];
  char *field[5];
  char *scan;
  int i;
  Cregexp_t *r;
  
  while (fgets(rbuf, sizeof(rbuf), stdin) != NULL) {
    rbuf[strlen(rbuf)-1] = '\0';	/* Dispense with \n. */
    scan = rbuf;
    for (i = 0; i < 5; i++) {
      field[i] = scan;
      if (field[i] == NULL) {
        printf("bad testfile format\n");
        return(EXIT_FAILURE);
      }
      scan = strchr(scan, '\t');
      if (scan != NULL)
        *scan++ = '\0';
    }
    _Cregexp_test_try(field);
  }
  
  /* And finish up with some internal testing... */
  if (Cregexp_comp((char *) NULL) != NULL)
    printf("Cregexp_comp(NULL) doesn't complain\n");
  if (Cregexp_exec((Cregexp_t *) NULL, "foo") == 0)
    printf("Cregexp_exec(NULL, ...) doesn't complain\n");
  if ((r = Cregexp_comp("foo")) == NULL) {
    printf("Cregexp_comp(\"foo\") fails (%s)\n",sstrerror(serrno));
    return(EXIT_FAILURE);
  }
  if (Cregexp_exec(r, (char *) NULL) == 0)
    printf("Cregexp_exec(..., NULL) doesn't complain\n");
  if (Cregexp_sub((Cregexp_t *) NULL, "foo", rbuf, BUFSIZ) == 0)
    printf("Cregexp_sub(NULL, ..., ...) doesn't complain\n");
  if (Cregexp_sub(r, (char *) NULL, rbuf, BUFSIZ) == 0)
    printf("Cregexp_sub(..., NULL, ...) doesn't complain\n");
  if (Cregexp_sub(r, "foo", (char *) NULL, BUFSIZ) == 0)
    printf("Cregexp_sub(..., ..., NULL) doesn't complain\n");
  memset(&badregexp,0,sizeof(Cregexp_t));
  if (Cregexp_exec(&badregexp, "foo") == 0)
    printf("Cregexp_exec(nonsense, ...) doesn't complain\n");
  if (Cregexp_sub(&badregexp, "foo", rbuf, BUFSIZ) == 0)
    printf("Cregexp_sub(nonsense, ..., ...) doesn't complain\n");
  return(EXIT_SUCCESS);
}

void _Cregexp_test_try(fields)
     char **fields;
{
  Cregexp_t *r;
  char dbuf[BUFSIZ];
  
  r = Cregexp_comp(fields[0]);
  if (r == NULL) {
    if (*fields[2] != 'c')
      printf("Cregexp_comp failure in `%s' (%s)\n", fields[0],sstrerror(serrno));
    return;
	}
  if (*fields[2] == 'c') {
    printf("unexpected regcomp success in `%s'\n", fields[0]);
    free((char *)r);
    return;
  }
  if (Cregexp_exec(r, fields[1]) != 0) {
    if (*fields[2] != 'n')
      printf("Cregexp_exec failure of `%s' in `%s' (%s)\n",fields[0],fields[1],sstrerror(serrno));
    free((char *)r);
    return;
  }
  if (*fields[2] == 'n') {
    printf("unexpected regexec success\n");
    free((char *)r);
    return;
  }
  if (Cregexp_sub(r, fields[3], dbuf, BUFSIZ) != 0) {
    printf("Cregexp_sub complaint of `%s' in `%s' (%s)\n",fields[0],fields[1],sstrerror(serrno));
    free((char *)r);
    return;
  }
  if (strcmp(dbuf, fields[4]) != 0)
    printf("Cregexp_sub result of `%s' on `%s' gives `%s' wrong\n", fields[0],fields[1],dbuf);
  else
    printf("Cregexp_sub result of `%s' on `%s' gives `%s' OK\n", fields[0],fields[1],dbuf);

  free((char *)r);
}


