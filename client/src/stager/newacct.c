/*
 * $Id: newacct.c,v 1.8 2005/10/20 16:01:51 jdurand Exp $
 */

/*
 * Copyright (C) 1990-2000 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: newacct.c,v $ $Revision: 1.8 $ $Date: 2005/10/20 16:01:51 $ CERN/IT/PDP/DM Antoine Trannoy";
#endif /* not lint */

/* newacct		Command to change current account	*/

#include <stdlib.h>
#include <stdio.h>
#if !defined(apollo) && !defined(_WIN32)
#include <unistd.h>
#endif
#include <string.h>
#if defined(_WIN32)
#include <stdlib.h>
#else
#include <sys/param.h>
#endif
#include <sys/types.h>
#include <grp.h>
#include <pwd.h>

extern char * getacctent() ; 
extern struct group * getgrnam() ; 

static void setacct(pwd,account,shell_opt)
	struct passwd    * pwd ;
	char 	     * account ;
	char 	   * shell_opt ;
{
	extern int    putenv() ; 
	extern char * getenv() ; 
	static char strenv[16] ;
#if defined(_WIN32)
	char shell[_MAX_PATH] ;
#else
	char shell[MAXPATHLEN] ;
#endif
	char 		  * cp ; 
	struct group       * g ;

	(void) sprintf(strenv,"ACCOUNT=%s",account) ; 
	if ( putenv(strenv) ) {
		(void) fprintf(stderr,"newacct: insufficient space to expand the environment\n") ;
		exit(1) ; 
	}

#if !defined(_WIN32)
	/* change group in case of multiple accounts */
	if ( (cp= strchr(account,'$')) != NULL ) {
		if ( (g= getgrnam(++cp)) == NULL ) { ;
			(void) fprintf(stderr,"newacct: invalid group %s\n",cp) ;
			exit(1) ;
		}
		setgid(g->gr_gid) ;
#if (defined(_AIX) && defined(_IBMR2)) || defined(sgi)
		setgroups(1,&g->gr_gid) ;
#endif
#if hpux
		setresuid(-1,pwd->pw_uid,-1) ;
#else
		seteuid(pwd->pw_uid) ;
#endif
	}
#endif

	if (shell_opt)
		cp= shell_opt ;
	else if ( ( cp= getenv("SHELL")) == NULL ) 
		cp= pwd->pw_shell ;
	if ( strlen(cp) >= sizeof(shell) ) {
		(void) fprintf(stderr,"newacct: invalid shell name %s\n",cp) ; 
		exit(1) ; 
	}
	if ( *cp == 0)
		cp= "/bin/sh" ;

	(void) strcpy(shell,cp) ; 	 
	if ( (cp= strrchr(cp,'/')) != NULL ) 
		cp ++ ; 
	else
		cp= shell;
	execl(shell,cp,NULL) ;
	(void) fprintf(stderr,"newacct: %s not found\n",shell) ; 
	exit(1) ; 
}

main(argc,argv)
	int     argc ; 
	char ** argv ; 
{
	char        buffer[80] ;
	int		     c ;
	char 		  * cp ; 
	struct passwd    * pwd ;
	char     * shell= NULL ; 
	char 	     * account ; 
    extern char *optarg;
    extern int optind;

	/*
	 * Checking arguments
	 * Setting account.
	 */
	while ((c = getopt (argc, argv, "s:")) != EOF) {
		switch (c) {
		case 's':
			shell= optarg;
			break;
		case '?':
			exit(1);
		default:
			break;
		}
	}
	if ( (argc-optind) > 1 ) {
		(void) fprintf(stderr,"usage is: newacct [accountid] \n") ; 
		exit(1) ; 
	}
	if ( optind < argc && (int)strlen(argv[optind]) > 6 ) {
		(void) fprintf(stderr,"newacct:  invalid account id specified\n") ; 
		exit(1) ; 
	}
	account= ( optind == argc ) ? NULL : argv[optind] ;	

	/*
	 * Getting password entry.
	 */
	if ( (pwd= getpwuid(getuid())) == NULL ) {
		(void) fprintf(stderr,"newacct: who are you ?\n") ; 
		exit(1) ; 
	} 

	if ( (cp= getacctent(pwd,account,buffer,sizeof(buffer))) == NULL ) {
		if ( account )
			(void) fprintf(stderr,"newacct: invalid account id specified\n") ;
		else
			(void) fprintf(stderr,"newacct: no entry for %s in account file\n",
			    pwd->pw_name) ;
		exit(1) ;
	}

	/*
	 *  Parsing the entry
	 */
	cp= strtok(cp,":") ;	/* login name */
	cp= strtok((char *)NULL,":") ;	/* account id */
	/*
	 * Setting the new account.
	 */
	setacct(pwd,cp,shell) ; 
	/* NOTREACHED */
}
