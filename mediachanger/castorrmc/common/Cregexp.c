/*
 * $Id: Cregexp.c,v 1.7 2008/07/28 16:55:04 waldron Exp $
 */

/*
 * regcomp and regexec -- regsub and regerror are elsewhere
 * @(#)regexp.c	1.3 of 18 April 87
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
 * Beware that some of this code is subtly aware of the way operator
 * precedence is structured in regular expressions.  Serious changes in
 * regular-expression syntax might require a total rethink.
 */

/* ======================================= */
/* Local headers for thread-safe variables */
/* ======================================= */
#include "mediachanger/castorrmc/h/Cglobals.h"

/* ============== */
/* System headers */
/* ============== */
#include <stdio.h>
#include <osdep.h>
#include <string.h>
#include <stdlib.h>
#include <errno.h>

/* ==================== */
/* Malloc Debug Library */
/* ==================== */
#ifdef USE_DMALLOC
#include <dmalloc.h>
#endif

/* ============= */
/* Local headers */
/* ============= */
#include "mediachanger/castorrmc/h/serrno.h"
#include "Cregexp.h"
#include "Cregexp_magic.h"

/*
 * The "internal use only" fields in regexp.h are present to pass info from
 * compile to execute that permits the execute phase to run lots faster on
 * simple cases.  They are:
 *
 * regstart	char that must begin a match; '\0' if none obvious
 * reganch	is the match anchored (at beginning-of-line only)?
 * regmust	string (pointer into program) that match must include, or NULL
 * regmlen	length of regmust string
 *
 * Regstart and reganch permit very fast decisions on suitable starting points
 * for a match, cutting down the work a lot.  Regmust permits fast rejection
 * of lines that cannot possibly match.  The regmust tests are costly enough
 * that regcomp() supplies a regmust only if the r.e. contains something
 * potentially expensive (at present, the only such thing detected is * or +
 * at the start of the r.e., which can involve a lot of backup).  Regmlen is
 * supplied because the test in regexec() needs it and regcomp() is computing
 * it anyway.
 */

/*
 * Structure for regexp "program".  This is essentially a linear encoding
 * of a nondeterministic finite-state machine (aka syntax charts or
 * "railroad normal form" in parsing technology).  Each node is an opcode
 * plus a "next" pointer, possibly plus an operand.  "Next" pointers of
 * all nodes except BRANCH implement concatenation; a "next" pointer with
 * a BRANCH on both ends of it is connecting two alternatives.  (Here we
 * have one of the subtle syntax dependencies:  an individual BRANCH (as
 * opposed to a collection of them) is never concatenated with anything
 * because of operator precedence.)  The operand of some types of node is
 * a literal string; for others, it is a node leading into a sub-FSM.  In
 * particular, the operand of a BRANCH node is the first node of the branch.
 * (NB this is *not* a tree structure:  the tail of the branch connects
 * to the thing following the set of BRANCHes.)  The opcodes are:
 */

/* definition	number	opnd?	meaning */
#ifdef END
#undef END
#endif
#define	END	0	/* no	End of program. */

#ifdef BOL
#undef BOL
#endif
#define	BOL	1	/* no	Match "" at beginning of line. */

#ifdef EOL
#undef EOL
#endif
#define	EOL	2	/* no	Match "" at end of line. */

#ifdef ANY
#undef ANY
#endif
#define	ANY	3	/* no	Match any one character. */

#ifdef ANYOF
#undef ANYOF
#endif
#define	ANYOF	4	/* str	Match any character in this string. */

#ifdef ANYBUT
#undef ANYBUT
#endif
#define	ANYBUT	5	/* str	Match any character not in this string. */

#ifdef BRANCH
#undef BRANCH
#endif
#define	BRANCH	6	/* node	Match this alternative, or the next... */

#ifdef BACK
#undef BACK
#endif
#define	BACK	7	/* no	Match "", "next" ptr points backward. */

#ifdef EXACTLY
#undef EXACTLY
#endif
#define	EXACTLY	8	/* str	Match this string. */

#ifdef NOTHING
#undef NOTHING
#endif
#define	NOTHING	9	/* no	Match empty string. */

#ifdef STAR
#undef STAR
#endif
#define	STAR	10	/* node	Match this (simple) thing 0 or more times. */

#ifdef PLUS
#undef PLUS
#endif
#define	PLUS	11	/* node	Match this (simple) thing 1 or more times. */

#ifdef OPEN
#undef OPEN
#endif
#define	OPEN	20	/* no	Mark this point in input as start of #n. */
/*	OPEN+1 is number 1, etc. */

#ifdef CLOSE
#undef CLOSE
#endif
#define	CLOSE	30	/* no	Analogous to OPEN. */

/*
 * Opcode notes:
 *
 * BRANCH	The set of branches constituting a single choice are hooked
 *		together with their "next" pointers, since precedence prevents
 *		anything being concatenated to any individual branch.  The
 *		"next" pointer of the last BRANCH in a choice points to the
 *		thing following the whole choice.  This is also where the
 *		final "next" pointer of each individual branch points; each
 *		branch starts with the operand node of a BRANCH node.
 *
 * BACK		Normal "next" pointers all implicitly point forward; BACK
 *		exists to make loop structures possible.
 *
 * STAR,PLUS	'?', and complex '*' and '+', are implemented as circular
 *		BRANCH structures using BACK.  Simple cases (one character
 *		per match) are implemented with STAR and PLUS for speed
 *		and to minimize recursive plunges.
 *
 * OPEN,CLOSE	...are numbered at compile time.
 */

/*
 * A node is one char of opcode followed by two chars of "next" pointer.
 * "Next" pointers are stored as two 8-bit pieces, high order first.  The
 * value is a positive offset from the opcode of the node containing it.
 * An operand, if any, simply follows the node.  (Note that much of the
 * code generation knows about this implicit relationship.)
 *
 * Using two bytes for the "next" pointer is vast overkill for most things,
 * but allows patterns to get big without disasters.
 */

#ifdef OP
#undef OP
#endif
#define	OP(p)	(*(p))

#ifdef NEXT
#undef NEXT
#endif
#define	NEXT(p)	(((*((p) + 1) & 0377) <<8) + (*((p) + 2) & 0377))

#ifdef OPERAND
#undef OPERAND
#endif
#define	OPERAND(p)	((p) + 3)

/*
 * See Cregexp_magic.h for one further detail of program structure.
 */


/*
 * Utility definitions.
 */
#ifdef UCHARAT
#undef UCHARAT
#endif
#ifndef CHARBITS
#define	UCHARAT(p)	((int)*(unsigned char *)(p))
#else
#define	UCHARAT(p)	((int)*(p)&CHARBITS)
#endif

#ifdef ISMULT
#undef ISMULT
#endif
#define	ISMULT(c)	((c) == '*' || (c) == '+' || (c) == '?')

#ifdef META
#undef META
#endif
#define	META	"^$.[()|?+*\\"

/*
 * Flags to be passed up and down.
 */
#ifdef HASWIDTH
#undef HASWIDTH
#endif
#define	HASWIDTH	01	/* Known never to match null string. */

#ifdef SIMPLE
#undef SIMPLE
#endif
#define	SIMPLE		02	/* Simple enough to be STAR/PLUS operand. */

#ifdef SPSTART
#undef SPSTART
#endif
#define	SPSTART		04	/* Starts with * or +. */

#ifdef WORST
#undef WORST
#endif
#define	WORST		0	/* Worst case. */

/* =============== */
/* Local variables */
/* =============== */
static int _Cregexp_parse_key  = -1;
static int _Cregexp_npar_key   = -1;
static int _Cregexp_dummy_key  = -1;
static int _Cregexp_code_key   = -1;
static int _Cregexp_size_key   = -1;
static int _Cregexp_input_key  = -1;
static int _Cregexp_bol_key    = -1;
static int _Cregexp_startp_key = -1;
static int _Cregexp_endp_key   = -1;

/*
 * Forward declarations for regcomp()'s friends.
 */
char *_Cregexp_reg (int, int *, char **, int *, char *, char **, long *);
char *_Cregexp_branch (int *, char **, int *, char *, char **, long *);
char *_Cregexp_piece (int *, char **, int *, char *, char **, long *);
char *_Cregexp_atom (int *, char **, int *, char *, char **, long *);
char *_Cregexp_node (char op, char *_Cregexp_dummy, char **_Cregexp_code, long *_Cregexp_size);
char *_Cregexp_next (char *, char *);
void _Cregexp_c (char b, char *_Cregexp_dummy, char **_Cregexp_code, long *_Cregexp_size);
void _Cregexp_insert (char op, char *opnd, char *_Cregexp_dummy, char **_Cregexp_code, long *_Cregexp_size);
void _Cregexp_tail (char *, char *, char *);
void _Cregexp_optail (char *, char *, char *);
int _Cregexp_try (Cregexp_t *, char *, char **, int *, char *, char **, long *, char **, char **, char ***, char ***);
int _Cregexp_match (char *, char **, int *, char *, char **, long *, char **, char **, char ***, char ***);
int _Cregexp_repeat (char *, char **);
int _Cregexp_gettsd (char ***, int **, char **, char ***, long **, char ***, char ***, char ****, char ****);
char *_Cregexp_prop (char *);


int _Cregexp_gettsd(char ***_Cregexp_parse,
                    int  **_Cregexp_npar,
                    char **_Cregexp_dummy,
                    char ***_Cregexp_code,
                    long **_Cregexp_size,
                    char ***_Cregexp_input,
                    char ***_Cregexp_bol,
                    char ****_Cregexp_startp,
                    char ****_Cregexp_endp)
{
	
	/* Get TSD variables */
	if (Cglobals_get(&_Cregexp_parse_key,  (void **) _Cregexp_parse , sizeof(char * )) < 0 ||
		Cglobals_get(&_Cregexp_npar_key,   (void **) _Cregexp_npar  , sizeof(int    )) < 0 ||
		Cglobals_get(&_Cregexp_dummy_key,  (void **) _Cregexp_dummy , sizeof(char   )) < 0 ||
		Cglobals_get(&_Cregexp_code_key,   (void **) _Cregexp_code  , sizeof(char * )) < 0 ||
		Cglobals_get(&_Cregexp_size_key,   (void **) _Cregexp_size  , sizeof(long   )) < 0 ||
		Cglobals_get(&_Cregexp_input_key,  (void **) _Cregexp_input , sizeof(char * )) < 0 ||
		Cglobals_get(&_Cregexp_bol_key,    (void **) _Cregexp_bol   , sizeof(char * )) < 0 ||
		Cglobals_get(&_Cregexp_startp_key, (void **) _Cregexp_startp, sizeof(char **)) < 0 ||
		Cglobals_get(&_Cregexp_endp_key,   (void **) _Cregexp_endp  , sizeof(char **)) < 0) {
		return(-1);
	}
	return(0);
}


/*
 - regcomp - compile a regular expression into internal code
 *
 * We can't allocate space until we know how big the compiled form will be,
 * but we can't compile it (and thus know how big it is) until we've got a
 * place to put the code.  So we cheat:  we compile it twice, once with code
 * generation turned off and size counting turned on, and once "for real".
 * This also means that we don't allocate space until we are sure that the
 * thing really will compile successfully, and we never have to move the
 * code and thus invalidate pointers into it.  (Note that it has to be in
 * one piece because free() must be able to free it all.)
 *
 * Beware that the optimization-preparation code in here knows about some
 * of the structure of the compiled regexp.
 */
Cregexp_t *Cregexp_comp(char *exp)
{
	Cregexp_t *r;
	char *scan;
	char *longest;
	int len;
	int flags;
	/*
	 * Global Thread-Safe work variables Pointers for regcomp() (a-la Cthread).
	 */
	char **_Cregexp_parse;     /* Input-scan pointer. */
	int   *_Cregexp_npar;      /* () count. */
	char  *_Cregexp_dummy;
	char **_Cregexp_code;      /* Code-emit pointer; &regdummy = don't. */
	long  *_Cregexp_size;      /* Code size. */
	char **_Cregexp_input;
	char **_Cregexp_bol;
	char ***_Cregexp_startp;
	char ***_Cregexp_endp;

	if (exp == NULL) {
		serrno = EINVAL;
		return(NULL);
	}

	if (_Cregexp_gettsd(&_Cregexp_parse,
						&_Cregexp_npar,
						&_Cregexp_dummy,
						&_Cregexp_code,
						&_Cregexp_size,
						&_Cregexp_input,
						&_Cregexp_bol,
						&_Cregexp_startp,
						&_Cregexp_endp) != 0) {
		return(NULL);
	}

	/* First pass: determine size, legality. */
	*_Cregexp_parse = exp;
	*_Cregexp_npar = 1;
	*_Cregexp_size = 0;
	*_Cregexp_code = _Cregexp_dummy;
	_Cregexp_c((char) CREGEXP_MAGIC,
			   _Cregexp_dummy,
			   _Cregexp_code,
			   _Cregexp_size);
	if (_Cregexp_reg(0,
					 &flags,
					 _Cregexp_parse,
					 _Cregexp_npar,
					 _Cregexp_dummy,
					 _Cregexp_code,
					 _Cregexp_size) == NULL) {
		return(NULL);
	}

	/* Small enough for pointer-storage convention? */
	if (*_Cregexp_size >= 32767) {
		serrno = ENOMEM;
		return(NULL);
	}

	/* Allocate space. */
	if ((r = (Cregexp_t *) malloc(sizeof(Cregexp_t) + (unsigned) (*_Cregexp_size))) == NULL) {
		serrno = errno;
		return(NULL);
	}

	/* Second pass: emit code. */
	*_Cregexp_parse = exp;
	*_Cregexp_npar = 1;
	*_Cregexp_code = r->program;
	_Cregexp_c((char) CREGEXP_MAGIC,
			   _Cregexp_dummy,
			   _Cregexp_code,
			   _Cregexp_size);
	if (_Cregexp_reg(0,
					 &flags,
					 _Cregexp_parse,
					 _Cregexp_npar,
					 _Cregexp_dummy,
					 _Cregexp_code,
					 _Cregexp_size) == NULL) {
		free(r);
		return(NULL);
	}

	/* Dig out information for optimizations. */
	r->regstart = '\0';	/* Worst-case defaults. */
	r->reganch = 0;
	r->regmust = NULL;
	r->regmlen = 0;
	scan = r->program + 1;			/* First BRANCH. */
	if (OP(_Cregexp_next(scan,
						 _Cregexp_dummy
		)) == END) {		/* Only one top-level choice. */
		scan = OPERAND(scan);

		/* Starting-point info. */
		if (OP(scan) == EXACTLY)
			r->regstart = *OPERAND(scan);
		else if (OP(scan) == BOL)
			r->reganch++;

		/*
		 * If there's something expensive in the r.e., find the
		 * longest literal string that must appear and make it the
		 * regmust.  Resolve ties in favor of later strings, since
		 * the regstart check works with the beginning of the r.e.
		 * and avoiding duplication strengthens checking.  Not a
		 * strong reason, but sufficient in the absence of others.
		 */
		if (flags & SPSTART) {
			longest = NULL;
			len = 0;
			for (; scan != NULL; scan = _Cregexp_next(scan,
													  _Cregexp_dummy))
				if (OP(scan) == EXACTLY && (int)strlen(OPERAND(scan)) >= len) {
					longest = OPERAND(scan);
					len = strlen(OPERAND(scan));
				}
			r->regmust = longest;
			r->regmlen = len;
		}
	}

	return(r);
}

/*
 - reg - regular expression, i.e. main body or parenthesized thing
 *
 * Caller must absorb opening parenthesis.
 *
 * Combining parenthesis handling with the base level of regular expression
 * is a trifle forced, but the need to tie the tails of the branches to what
 * follows makes it hard to avoid.
 */
char *_Cregexp_reg(int paren,			/* Parenthesized? */
                   int *flagp,
                   char **_Cregexp_parse,
                   int *_Cregexp_npar,
                   char *_Cregexp_dummy,
                   char **_Cregexp_code,
                   long *_Cregexp_size)
{
	char *ret;
	char *br;
	char *ender;
	int parno = 0;
	int flags;
  
	*flagp = HASWIDTH;	/* Tentatively. */

	/* Make an OPEN node, if parenthesized. */
	if (paren) {
		if (*_Cregexp_npar >= CREGEXP_NSUBEXP) {
			serrno = EINVAL;
			return(NULL);
		}
		parno = *_Cregexp_npar;
		(*_Cregexp_npar)++;
		ret = _Cregexp_node((char) (OPEN+parno),
							_Cregexp_dummy,
							_Cregexp_code,
							_Cregexp_size);
	} else {
		ret = NULL;
	}

	/* Pick up the branches, linking them together. */
	if ((br = _Cregexp_branch(&flags,
							  _Cregexp_parse,
							  _Cregexp_npar,
							  _Cregexp_dummy,
							  _Cregexp_code,
							  _Cregexp_size)) == NULL) {
		return(NULL);
	}
	if (ret != NULL) {
		_Cregexp_tail(ret,
					  br,
					  _Cregexp_dummy);	/* OPEN -> first. */
	} else {
		ret = br;
	}
	if (! (flags & HASWIDTH)) {
		*flagp &= ~HASWIDTH;
	}
	*flagp |= flags & SPSTART;
	while (*(*_Cregexp_parse) == '|') {
		(*_Cregexp_parse)++;
		if ((br = _Cregexp_branch(&flags,
								  _Cregexp_parse,
								  _Cregexp_npar,
								  _Cregexp_dummy,
								  _Cregexp_code,
								  _Cregexp_size)) == NULL) {
			return(NULL);
		}
		_Cregexp_tail(ret,
					  br,
					  _Cregexp_dummy);	/* BRANCH -> BRANCH. */
		if (! (flags & HASWIDTH)) {
			*flagp &= ~HASWIDTH;
		}
		*flagp |= flags & SPSTART;
	}

	/* Make a closing node, and hook it on the end. */
	ender = _Cregexp_node((char) ((paren) ? CLOSE+parno : END),
						  _Cregexp_dummy,
						  _Cregexp_code,
						  _Cregexp_size);
	_Cregexp_tail(ret,
				  ender,
				  _Cregexp_dummy);

	/* Hook the tails of the branches to the closing node. */
	for (br = ret; br != NULL; br = _Cregexp_next(br,
												  _Cregexp_dummy)) {
		_Cregexp_optail(br,
						ender,
						_Cregexp_dummy);
	}

	/* Check for proper termination. */
	if (paren && *(*_Cregexp_parse)++ != ')') {
		serrno = EINVAL; /* Unmatched () */
		return(NULL);
	} else if (!paren && *(*_Cregexp_parse) != '\0') {
		if (*(*_Cregexp_parse) == ')') {
			serrno = EINVAL; /* Unmatched () */
			return(NULL);
		} else {
			serrno = EINVAL;
			return(NULL); /* "Can't happen". junk on end */
		}
	}

	return(ret);
}

/*
 - regbranch - one alternative of an | operator
 *
 * Implements the concatenation operator.
 */
char *_Cregexp_branch(int *flagp,
                      char **_Cregexp_parse,
                      int *_Cregexp_npar,
                      char *_Cregexp_dummy,
                      char **_Cregexp_code,
                      long *_Cregexp_size)
{
	char *ret;
	char *chain;
	char *latest;
	int flags;

	*flagp = WORST;		/* Tentatively. */

	ret = _Cregexp_node((char) BRANCH,
						_Cregexp_dummy,
						_Cregexp_code,
						_Cregexp_size);
	chain = NULL;
	while (*(*_Cregexp_parse) != '\0' && *(*_Cregexp_parse) != '|' && *(*_Cregexp_parse) != ')') {
		latest = _Cregexp_piece(&flags,
								_Cregexp_parse,
								_Cregexp_npar,
								_Cregexp_dummy,
								_Cregexp_code,
								_Cregexp_size);
		if (latest == NULL) {
			return(NULL);
		}
		*flagp |= flags & HASWIDTH;
		if (chain == NULL) {	/* First piece. */
			*flagp |= flags&SPSTART;
		} else {
			_Cregexp_tail(chain,
						  latest,
						  _Cregexp_dummy);
		}
		chain = latest;
	}
	if (chain == NULL) {	/* Loop ran zero times. */
		_Cregexp_node((char) NOTHING,
					  _Cregexp_dummy,
					  _Cregexp_code,
					  _Cregexp_size);
	}
	return(ret);
}

/*
 - regpiece - something followed by possible [*+?]
 *
 * Note that the branching code sequences used for ? and the general cases
 * of * and + are somewhat optimized:  they use the same NOTHING node as
 * both the endmarker for their branch list and the body of the last branch.
 * It might seem that this node could be dispensed with entirely, but the
 * endmarker role is not redundant.
 */
char *_Cregexp_piece(int *flagp,
                     char **_Cregexp_parse,
                     int *_Cregexp_npar,
                     char *_Cregexp_dummy,
                     char **_Cregexp_code,
                     long *_Cregexp_size)
{
	char *ret;
	char op;
	char *next;
	int flags;

	ret = _Cregexp_atom(&flags,
						_Cregexp_parse,
						_Cregexp_npar,
						_Cregexp_dummy,
						_Cregexp_code,
						_Cregexp_size);
	if (ret == NULL) {
		return(NULL);
	}

	op = *(*_Cregexp_parse);
	if (! ISMULT(op)) {
		*flagp = flags;
		return(ret);
	}

	if (! (flags & HASWIDTH) && op != '?') {
		/* *+ operand could be empty */
		serrno = EINVAL;
		return(NULL);
	}
	*flagp = (op != '+') ? (WORST | SPSTART) : (WORST | HASWIDTH);

	if (op == '*' && (flags & SIMPLE)) {
		_Cregexp_insert(STAR,
						ret,
						_Cregexp_dummy,
						_Cregexp_code,
						_Cregexp_size);
	} else if (op == '*') {
		/* Emit x* as (x&|), where & means "self". */
		_Cregexp_insert(BRANCH,
						ret,
						_Cregexp_dummy,
						_Cregexp_code,
						_Cregexp_size);			/* Either x */
		_Cregexp_optail(ret,
						_Cregexp_node((char) BACK,
									  _Cregexp_dummy,
									  _Cregexp_code,
									  _Cregexp_size),
						_Cregexp_dummy);		/* and loop */
		_Cregexp_optail(ret,
						ret,
						_Cregexp_dummy);			/* back */
		_Cregexp_tail(ret,
					  _Cregexp_node((char) BRANCH,
									_Cregexp_dummy,
									_Cregexp_code,
									_Cregexp_size),
					  _Cregexp_dummy);		/* or */
		_Cregexp_tail(ret,
					  _Cregexp_node((char) NOTHING,
									_Cregexp_dummy,
									_Cregexp_code,
									_Cregexp_size),
					  _Cregexp_dummy);		/* null. */
	} else if (op == '+' && (flags&SIMPLE)) {
		_Cregexp_insert(PLUS,
						ret,
						_Cregexp_dummy,
						_Cregexp_code,
						_Cregexp_size);
	} else if (op == '+') {
		/* Emit x+ as x(&|), where & means "self". */
		next = _Cregexp_node((char) BRANCH,
							 _Cregexp_dummy,
							 _Cregexp_code,
							 _Cregexp_size);			/* Either */
		_Cregexp_tail(ret,
					  next,
					  _Cregexp_dummy);
		_Cregexp_tail(_Cregexp_node((char) BACK,
									_Cregexp_dummy,
									_Cregexp_code,
									_Cregexp_size),
					  ret,
					  _Cregexp_dummy);		/* loop back */
		_Cregexp_tail(next,_Cregexp_node((char) BRANCH,
										 _Cregexp_dummy,
										 _Cregexp_code,
										 _Cregexp_size),
					  _Cregexp_dummy);		/* or */
		_Cregexp_tail(ret,_Cregexp_node((char) NOTHING,
										_Cregexp_dummy,
										_Cregexp_code,
										_Cregexp_size),
					  _Cregexp_dummy);		/* null. */
	} else if (op == '?') {
		/* Emit x? as (x|) */
		_Cregexp_insert(BRANCH,
						ret,
						_Cregexp_dummy,
						_Cregexp_code,
						_Cregexp_size);			/* Either x */
		_Cregexp_tail(ret,_Cregexp_node((char) BRANCH,
										_Cregexp_dummy,
										_Cregexp_code,
										_Cregexp_size),
					  _Cregexp_dummy);		/* or */
		next = _Cregexp_node((char) NOTHING,
							 _Cregexp_dummy,
							 _Cregexp_code,
							 _Cregexp_size);		/* null. */
		_Cregexp_tail(ret,next,
					  _Cregexp_dummy);
		_Cregexp_optail(ret,
						next,
						_Cregexp_dummy);
	}
	(*_Cregexp_parse)++;
	if (ISMULT(*(*_Cregexp_parse))) {
		/* nested *?+ */
		serrno = EINVAL;
		return(NULL);
	}

	return(ret);
}

/*
 - regatom - the lowest level
 *
 * Optimization:  gobbles an entire sequence of ordinary characters so that
 * it can turn them into a single node, which is smaller to store and
 * faster to run.  Backslashed characters are exceptions, each becoming a
 * separate node; the code is simpler that way and it's not worth fixing.
 */
char *_Cregexp_atom(int *flagp,
                    char **_Cregexp_parse,
                    int *_Cregexp_npar,
                    char *_Cregexp_dummy,
                    char **_Cregexp_code,
                    long *_Cregexp_size)
{
	char *ret;
	int flags;
  
	*flagp = WORST;		/* Tentatively. */
  
	switch (*(*_Cregexp_parse)++) {
	case '^':
		ret = _Cregexp_node((char) BOL,
							_Cregexp_dummy,
							_Cregexp_code,
							_Cregexp_size);
		break;
	case '$':
		ret = _Cregexp_node((char) EOL,
							_Cregexp_dummy,
							_Cregexp_code,
							_Cregexp_size);
		break;
	case '.':
		ret = _Cregexp_node((char) ANY,
							_Cregexp_dummy,
							_Cregexp_code,
							_Cregexp_size);
		*flagp |= HASWIDTH | SIMPLE;
		break;
	case '[': {
		int class;
		int classend;

		if (*(*_Cregexp_parse) == '^') {	/* Complement of range. */
			ret = _Cregexp_node((char) ANYBUT,
								_Cregexp_dummy,
								_Cregexp_code,
								_Cregexp_size);
			(*_Cregexp_parse)++;
		} else {
			ret = _Cregexp_node((char) ANYOF,
								_Cregexp_dummy,
								_Cregexp_code,
								_Cregexp_size);
		}
		if (*(*_Cregexp_parse) == ']' || *(*_Cregexp_parse) == '-') {
			_Cregexp_c((char) *(*_Cregexp_parse)++,
					   _Cregexp_dummy,
					   _Cregexp_code,
					   _Cregexp_size);
		}
		while (*(*_Cregexp_parse) != '\0' && *(*_Cregexp_parse) != ']') {
			if (*(*_Cregexp_parse) == '-') {
				(*_Cregexp_parse)++;
				if (*(*_Cregexp_parse) == ']' || *(*_Cregexp_parse) == '\0')
					_Cregexp_c((char) '-',
							   _Cregexp_dummy,
							   _Cregexp_code,
							   _Cregexp_size);
				else {
					class = UCHARAT(*_Cregexp_parse - 2) + 1;
					classend = UCHARAT(*_Cregexp_parse);
					if (class > classend+1) {
						/* invalid [] range */
						serrno = EINVAL;
						return(NULL);
					}
					for (; class <= classend; class++) {
						_Cregexp_c((char) class,
								   _Cregexp_dummy,
								   _Cregexp_code,
								   _Cregexp_size);
					}
					(*_Cregexp_parse)++;
				}
			} else
				_Cregexp_c((char) *(*_Cregexp_parse)++,
						   _Cregexp_dummy,
						   _Cregexp_code,
						   _Cregexp_size);
		}
		_Cregexp_c((char) '\0',
				   _Cregexp_dummy,
				   _Cregexp_code,
				   _Cregexp_size);
		if (*(*_Cregexp_parse) != ']') {
			/* unmatched [] */
			serrno = EINVAL;
			return(NULL);
		}
		(*_Cregexp_parse)++;
		*flagp |= HASWIDTH | SIMPLE;
	}
	break;
	case '(':
		ret = _Cregexp_reg(1,
						   &flags,
						   _Cregexp_parse,
						   _Cregexp_npar,
						   _Cregexp_dummy,
						   _Cregexp_code,
						   _Cregexp_size);
		if (ret == NULL) {
			return(NULL);
		}
		*flagp |= flags & (HASWIDTH | SPSTART);
		break;
	case '\0':
	case '|':
	case ')':
		/* internal urp */
		serrno = EINVAL;
		return(NULL);
	case '?':
	case '+':
	case '*':
		/* ?+* follows nothing */
		serrno = EINVAL;
		return(NULL);
	case '\\':
		if (*(*_Cregexp_parse) == '\0') {
			/* tailing \\ */
			serrno = EINVAL;
			return(NULL);
		}
		ret = _Cregexp_node((char) EXACTLY,
							_Cregexp_dummy,
							_Cregexp_code,
							_Cregexp_size);
		_Cregexp_c((char) *(*_Cregexp_parse)++,
				   _Cregexp_dummy,
				   _Cregexp_code,
				   _Cregexp_size);
		_Cregexp_c((char) '\0',
				   _Cregexp_dummy,
				   _Cregexp_code,
				   _Cregexp_size);
		*flagp |= HASWIDTH | SIMPLE;
		break;
	default:
    {
		int len;
		char ender;

		(*_Cregexp_parse)--;
		len = strcspn(*_Cregexp_parse, META);
		if (len <= 0) {
			/* internal disaster */
			serrno = SEINTERNAL;
			return(NULL);
		}
		ender = *(*_Cregexp_parse + len);
		if (len > 1 && ISMULT(ender)) {
			len--;		/* Back off clear of ?+* operand. */
		}
		*flagp |= HASWIDTH;
		if (len == 1) {
			*flagp |= SIMPLE;
		}
		ret = _Cregexp_node((char) EXACTLY,
							_Cregexp_dummy,
							_Cregexp_code,
							_Cregexp_size);
		while (len > 0) {
			_Cregexp_c((char) *(*_Cregexp_parse)++,
					   _Cregexp_dummy,
					   _Cregexp_code,
					   _Cregexp_size);
			len--;
		}
		_Cregexp_c((char) '\0',
				   _Cregexp_dummy,
				   _Cregexp_code,
				   _Cregexp_size);
    }
		break;
	}
  
	return(ret);
}

/*
  - regnode - emit a node
*/
char *_Cregexp_node(char op,
                    char *_Cregexp_dummy,
                    char **_Cregexp_code,
                    long *_Cregexp_size)
{
	char *ret;
	char *ptr;

	ret = *_Cregexp_code;
	if (ret == _Cregexp_dummy) {
		*_Cregexp_size += 3;
		return(ret);
	}

	ptr = ret;
	*ptr++ = op;
	*ptr++ = '\0';		/* Null "next" pointer. */
	*ptr++ = '\0';
	*_Cregexp_code = ptr;

	return(ret);
}

/*
 - regc - emit (if appropriate) a byte of code
 */
void _Cregexp_c(char b,
                char *_Cregexp_dummy,
                char **_Cregexp_code,
                long *_Cregexp_size)
{
	if (*_Cregexp_code != _Cregexp_dummy) {
		*(*_Cregexp_code)++ = b;
	} else {
		(*_Cregexp_size)++;
	}
}

/*
 - reginsert - insert an operator in front of already-emitted operand
 *
 * Means relocating the operand.
 */
void _Cregexp_insert(char op,
                     char *opnd,
                     char *_Cregexp_dummy,
                     char **_Cregexp_code,
                     long *_Cregexp_size)
{
	char *src;
	char *dst;
	char *place;

	if (*_Cregexp_code == _Cregexp_dummy) {
		*_Cregexp_size += 3;
		return;
	}

	src = *_Cregexp_code;
	*_Cregexp_code += 3;
	dst = *_Cregexp_code;
	while (src > opnd)
		*--dst = *--src;

	place = opnd;		/* Op node, where operand used to be. */
	*place++ = op;
	*place++ = '\0';
	*place++ = '\0';
}

/*
 - regtail - set the next-pointer at the end of a node chain
 */
void _Cregexp_tail(char *p,
                   char *val,
                   char *_Cregexp_dummy)
{
	char *scan;
	char *temp;
	int offset;

	if (p == _Cregexp_dummy)
		return;

	/* Find last node. */
	scan = p;
	for (;;) {
		temp = _Cregexp_next(scan,
							 _Cregexp_dummy);
		if (temp == NULL) {
			break;
		}
		scan = temp;
	}

	if (OP(scan) == BACK) {
		offset = scan - val;
	} else {
		offset = val - scan;
	}
	*(scan+1) = (offset >> 8) & 0377;
	*(scan+2) = offset & 0377;
}

/*
 - regoptail - regtail on operand of first argument; nop if operandless
 */
void _Cregexp_optail(char *p,
                     char *val,
                     char *_Cregexp_dummy)
{
	/* "Operandless" and "op != BRANCH" are synonymous in practice. */
	if (p == NULL || p == _Cregexp_dummy || OP(p) != BRANCH) {
		return;
	}
	_Cregexp_tail(OPERAND(p),
				  val,
				  _Cregexp_dummy);
}

/*
 * Regexec and friends
 */

/*
 * Global work variables for regexec().
 */

/*
 * Forwards.
 */

/*
 - regexec - match a regexp against a string
 */
int Cregexp_exec(Cregexp_t *prog,
                 char *string)
{
	char *s;
	char **_Cregexp_parse;     /* Input-scan pointer. */
	int   *_Cregexp_npar;      /* () count. */
	char  *_Cregexp_dummy;
	char **_Cregexp_code;      /* Code-emit pointer; &regdummy = don't. */
	long  *_Cregexp_size;      /* Code size. */
	char **_Cregexp_input;		/* String-input pointer. */
	char **_Cregexp_bol;		/* Beginning of input, for ^ check. */
	char ***_Cregexp_startp;	/* Pointer to startp array. */
	char ***_Cregexp_endp;		/* Ditto for endp. */

	if (_Cregexp_gettsd(&_Cregexp_parse,
						&_Cregexp_npar,
						&_Cregexp_dummy,
						&_Cregexp_code,
						&_Cregexp_size,
						&_Cregexp_input,
						&_Cregexp_bol,
						&_Cregexp_startp,
						&_Cregexp_endp) != 0) {
		return(-1);
	}

	/* Be paranoid... */
	if (prog == NULL || string == NULL) {
		serrno = EINVAL;
		return(-1);
	}

	/* Check validity of program. */
	if (UCHARAT(prog->program) != CREGEXP_MAGIC) {
		/* Corrupted program */
		serrno = SEINTERNAL;
		return(-1);
	}

	/* If there is a "must appear" string, look for it. */
	if (prog->regmust != NULL) {
		s = string;
		while ((s = strchr(s, prog->regmust[0])) != NULL) {
			if (strncmp(s, prog->regmust, (size_t) prog->regmlen) == 0)
				break;	/* Found it. */
			s++;
		}
		if (s == NULL) {	/* Not present. */
			serrno = EINVAL;
			return(-1);
		}
	}

	/* Mark beginning of line for ^ . */
	*_Cregexp_bol = string;

	/* Simplest case:  anchored match need be tried only once. */
	if (prog->reganch) {
		return(_Cregexp_try(prog,
							string,
							_Cregexp_parse,
							_Cregexp_npar,
							_Cregexp_dummy,
							_Cregexp_code,
							_Cregexp_size,
							_Cregexp_input,
							_Cregexp_bol,
							_Cregexp_startp,
							_Cregexp_endp));
	}

	/* Messy cases:  unanchored match. */
	s = string;
	if (prog->regstart != '\0')
		/* We know what char it must start with. */
		while ((s = strchr(s, prog->regstart)) != NULL) {
			if (_Cregexp_try(prog,
							 s,
							 _Cregexp_parse,
							 _Cregexp_npar,
							 _Cregexp_dummy,
							 _Cregexp_code,
							 _Cregexp_size,
							 _Cregexp_input,
							 _Cregexp_bol,
							 _Cregexp_startp,
							 _Cregexp_endp) == 0)
				return(0);
			s++;
		}
	else
		/* We don't -- general case. */
		do {
			if (_Cregexp_try(prog,
							 s,
							 _Cregexp_parse,
							 _Cregexp_npar,
							 _Cregexp_dummy,
							 _Cregexp_code,
							 _Cregexp_size,
							 _Cregexp_input,
							 _Cregexp_bol,
							 _Cregexp_startp,
							 _Cregexp_endp) == 0)
				return(0);
		} while (*s++ != '\0');

	/* Failure. */
	serrno = ENOENT;
	return(-1);
}

/*
 - regtry - try match at specific point
 */
int	_Cregexp_try(Cregexp_t *prog,
                 char *string,
                 char **_Cregexp_parse,
                 int *_Cregexp_npar,
                 char *_Cregexp_dummy,
                 char **_Cregexp_code,
                 long *_Cregexp_size,
                 char **_Cregexp_input,
                 char **_Cregexp_bol,
                 char ***_Cregexp_startp,
                 char ***_Cregexp_endp)
{
	int i;
	char **sp;
	char **ep;

	*_Cregexp_input = string;
	*_Cregexp_startp = prog->startp;
	*_Cregexp_endp = prog->endp;

	sp = prog->startp;
	ep = prog->endp;
	for (i = CREGEXP_NSUBEXP; i > 0; i--) {
		*sp++ = NULL;
		*ep++ = NULL;
	}
	if (_Cregexp_match(prog->program + 1,
					   _Cregexp_parse,
					   _Cregexp_npar,
					   _Cregexp_dummy,
					   _Cregexp_code,
					   _Cregexp_size,
					   _Cregexp_input,
					   _Cregexp_bol,
					   _Cregexp_startp,
					   _Cregexp_endp) == 0) {
		prog->startp[0] = string;
		prog->endp[0] = *_Cregexp_input;
		return(0);
	} else {
		serrno = ENOENT;
		return(-1);
	}
}

/*
 - regmatch - main matching routine
 *
 * Conceptually the strategy is simple:  check to see whether the current
 * node matches, call self recursively to see whether the rest matches,
 * and then act accordingly.  In practice we make some effort to avoid
 * recursion, in particular by going through "ordinary" nodes (that don't
 * need to know whether the rest of the match failed) by a loop instead of
 * by recursion.
 */
int _Cregexp_match(char *prog,
                   char **_Cregexp_parse,
                   int *_Cregexp_npar,
                   char *_Cregexp_dummy,
                   char **_Cregexp_code,
                   long *_Cregexp_size,
                   char **_Cregexp_input,
                   char **_Cregexp_bol,
                   char ***_Cregexp_startp,
                   char ***_Cregexp_endp)
{
	char *scan;	/* Current node. */
	char *next;		/* Next node. */

	scan = prog;
	while (scan != NULL) {
		next = _Cregexp_next(scan,
							 _Cregexp_dummy);
    
		switch (OP(scan)) {
		case BOL:
			if (*_Cregexp_input != *_Cregexp_bol)
				return(-1);
			break;
		case EOL:
			if (*(*_Cregexp_input) != '\0')
				return(-1);
			break;
		case ANY:
			if (*(*_Cregexp_input) == '\0')
				return(-1);
			(*_Cregexp_input)++;
			break;
		case EXACTLY:
		{
			int len;
			char *opnd;

			opnd = OPERAND(scan);
			/* Inline the first character, for speed. */
			if (*opnd != *(*_Cregexp_input)) {
				return(-1);
			}
			len = strlen(opnd);
			if (len > 1 && strncmp(opnd, *_Cregexp_input, (size_t) len) != 0) {
				return(-1);
			}
			(*_Cregexp_input) += len;
		}
		break;
		case ANYOF:
			if (*(*_Cregexp_input) == '\0' || strchr(OPERAND(scan), *(*_Cregexp_input)) == NULL) {
				return(-1);
			}
			(*_Cregexp_input)++;
			break;
		case ANYBUT:
			if (*(*_Cregexp_input) == '\0' || strchr(OPERAND(scan), *(*_Cregexp_input)) != NULL) {
				return(-1);
			}
			(*_Cregexp_input)++;
			break;
		case NOTHING:
			break;
		case BACK:
			break;
		case OPEN+1:
		case OPEN+2:
		case OPEN+3:
		case OPEN+4:
		case OPEN+5:
		case OPEN+6:
		case OPEN+7:
		case OPEN+8:
		case OPEN+9:
		{
			int no;
			char *save;
        
			no = OP(scan) - OPEN;
			save = *_Cregexp_input;
        
			if (_Cregexp_match(next,
							   _Cregexp_parse,
							   _Cregexp_npar,
							   _Cregexp_dummy,
							   _Cregexp_code,
							   _Cregexp_size,
							   _Cregexp_input,
							   _Cregexp_bol,
							   _Cregexp_startp,
							   _Cregexp_endp) == 0) {
				/*
				 * Don't set startp if some later
				 * invocation of the same parentheses
				 * already has.
				 */
				if ((*_Cregexp_startp)[no] == NULL) {
					(*_Cregexp_startp)[no] = save;
				}
				return(0);
			} else {
				return(-1);
			}
		}
		case CLOSE+1:
		case CLOSE+2:
		case CLOSE+3:
		case CLOSE+4:
		case CLOSE+5:
		case CLOSE+6:
		case CLOSE+7:
		case CLOSE+8:
		case CLOSE+9:
		{
			int no;
			char *save;
        
			no = OP(scan) - CLOSE;
			save = *_Cregexp_input;
        
			if (_Cregexp_match(next,
							   _Cregexp_parse,
							   _Cregexp_npar,
							   _Cregexp_dummy,
							   _Cregexp_code,
							   _Cregexp_size,
							   _Cregexp_input,
							   _Cregexp_bol,
							   _Cregexp_startp,
							   _Cregexp_endp) == 0) {
				/*
				 * Don't set endp if some later
				 * invocation of the same parentheses
				 * already has.
				 */
				if ((*_Cregexp_endp)[no] == NULL) {
					(*_Cregexp_endp)[no] = save;
				}
				return(0);
			} else {
				return(-1);
			}
		}
		case BRANCH:
		{
			char *save;
        
			if (OP(next) != BRANCH)		/* No choice. */
				next = OPERAND(scan);	/* Avoid recursion. */
			else {
				do {
					save = *_Cregexp_input;
					if (_Cregexp_match(OPERAND(scan),
									   _Cregexp_parse,
									   _Cregexp_npar,
									   _Cregexp_dummy,
									   _Cregexp_code,
									   _Cregexp_size,
									   _Cregexp_input,
									   _Cregexp_bol,
									   _Cregexp_startp,
									   _Cregexp_endp) == 0) {
						return(0);
					}
					*_Cregexp_input = save;
					scan = _Cregexp_next(scan,
										 _Cregexp_dummy);
				} while (scan != NULL && OP(scan) == BRANCH);
				return(-1);
				/* NOTREACHED */
			}
		}
		break;
		case STAR:
		case PLUS:
		{
			char nextch;
			int no;
			char *save;
			int min;

			/*
			 * Lookahead to avoid useless match attempts
			 * when we know what character comes next.
			 */
			nextch = '\0';
			if (OP(next) == EXACTLY)
				nextch = *OPERAND(next);
			min = (OP(scan) == STAR) ? 0 : 1;
			save = *_Cregexp_input;
			no = _Cregexp_repeat(OPERAND(scan),
								 _Cregexp_input);
			while (no >= min) {
				/* If it could work, try it. */
				if (nextch == '\0' || *(*_Cregexp_input) == nextch)
					if (_Cregexp_match(next,
									   _Cregexp_parse,
									   _Cregexp_npar,
									   _Cregexp_dummy,
									   _Cregexp_code,
									   _Cregexp_size,
									   _Cregexp_input,
									   _Cregexp_bol,
									   _Cregexp_startp,
									   _Cregexp_endp) == 0) {
						return(0);
					}
				/* Couldn't or didn't -- back up. */
				no--;
				*_Cregexp_input = save + no;
			}
			return(-1);
		}
		case END:
			return(0);	/* Success! */
		default:
			serrno = SEINTERNAL;
			return(-1);
		}
    
		scan = next;
	}
  
	/*
	 * We get here only if there's trouble -- normally "case END" is
	 * the terminating point.
	 */
	serrno = SEINTERNAL;
	return(-1);
}

/*
 - regrepeat - repeatedly match something simple, report how many
 */
int _Cregexp_repeat(char *p,
                    char **_Cregexp_input)
{
	int count = 0;
	char *scan;
	char *opnd;

	scan = *_Cregexp_input;
	opnd = OPERAND(p);
	switch (OP(p)) {
	case ANY:
		count = strlen(scan);
		scan += count;
		break;
	case EXACTLY:
		while (*opnd == *scan) {
			count++;
			scan++;
		}
		break;
	case ANYOF:
		while (*scan != '\0' && strchr(opnd, *scan) != NULL) {
			count++;
			scan++;
		}
		break;
	case ANYBUT:
		while (*scan != '\0' && strchr(opnd, *scan) == NULL) {
			count++;
			scan++;
		}
		break;
	default:
		/* Oh dear.  Called inappropriately. */
		serrno = SEINTERNAL;
		count = 0;	/* Best compromise. */
		break;
	}
	*_Cregexp_input = scan;
  
	return(count);
}

/*
 - regnext - dig the "next" pointer out of a node
 */
char *_Cregexp_next(char *p,
                    char *_Cregexp_dummy)
{
	int offset;

	if (p == _Cregexp_dummy)
		return(NULL);

	offset = NEXT(p);
	if (offset == 0)
		return(NULL);

	if (OP(p) == BACK)
		return(p-offset);
	else
		return(p+offset);
}

/*
 - regsub - perform substitutions after a regexp match
 */
int Cregexp_sub(Cregexp_t *prog,
                char *source,
                char *dest,
                size_t maxsize)
{
	char *src;
	char *dst;
	char c;
	int no;
	int len;

	if (prog == NULL || source == NULL || dest == NULL || maxsize <= 0) {
		/* NULL parm to regsub */
		serrno = EINVAL;
		return(-1);
	}
	if (UCHARAT(prog->program) != CREGEXP_MAGIC) {
		/* damaged regexp fed to regsub */
		serrno = SEINTERNAL;
		return(-1);
	}

	src = source;
	dst = dest;
	while ((c = *src++) != '\0') {
		if (c == '&')
			no = 0;
		else if (c == '\\' && '0' <= *src && *src <= '9')
			no = *src++ - '0';
		else
			no = -1;
    
		if (no < 0) {	/* Ordinary character. */
			if (c == '\\' && (*src == '\\' || *src == '&'))
				c = *src++;
			*dst++ = c;
		} else if (prog->startp[no] != NULL && prog->endp[no] != NULL) {
			len = prog->endp[no] - prog->startp[no];
			if (dst + len > dest + maxsize) {
				serrno = ENOMEM;
				/* Makes sure that null terminates */
				dest[maxsize] = '\0';
				return(-1);
			}
			strncpy(dst, prog->startp[no], (size_t) len);
			dst += len;
			if (len != 0 && *(dst-1) == '\0') {	/* strncpy hit NUL. */
				/* damaged match string */
				serrno = SEINTERNAL;
				return(-1);
			}
		}
	}
	*dst++ = '\0';
	return(0);
}

/*
 - regprop - printable representation of opcode
 */
char *_Cregexp_prop(char *op)
{
	char *p = NULL;
	static char buf[50];
  
	(void) strcpy(buf, ":");
  
	switch (OP(op)) {
	case BOL:
		p = "BOL";
		break;
	case EOL:
		p = "EOL";
		break;
	case ANY:
		p = "ANY";
		break;
	case ANYOF:
		p = "ANYOF";
		break;
	case ANYBUT:
		p = "ANYBUT";
		break;
	case BRANCH:
		p = "BRANCH";
		break;
	case EXACTLY:
		p = "EXACTLY";
		break;
	case NOTHING:
		p = "NOTHING";
		break;
	case BACK:
		p = "BACK";
		break;
	case END:
		p = "END";
		break;
	case OPEN+1:
	case OPEN+2:
	case OPEN+3:
	case OPEN+4:
	case OPEN+5:
	case OPEN+6:
	case OPEN+7:
	case OPEN+8:
	case OPEN+9:
		sprintf(buf+strlen(buf), "OPEN%d", OP(op)-OPEN);
		p = NULL;
		break;
	case CLOSE+1:
	case CLOSE+2:
	case CLOSE+3:
	case CLOSE+4:
	case CLOSE+5:
	case CLOSE+6:
	case CLOSE+7:
	case CLOSE+8:
	case CLOSE+9:
		sprintf(buf+strlen(buf), "CLOSE%d", OP(op)-CLOSE);
		p = NULL;
		break;
	case STAR:
		p = "STAR";
		break;
	case PLUS:
		p = "PLUS";
		break;
	default:
		/* regerror("corrupted opcode"); */
		break;
	}
	if (p != NULL)
		(void) strcat(buf, p);
	return(buf);
}

