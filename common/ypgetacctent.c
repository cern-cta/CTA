/*
 * Copyright (C) 1990-1995 by CERN CN-PDP/CS
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)ypgetacctent.c	1.2 01/29/96 CERN CN-PDP/CS Antoine Trannoy";
#endif /* not lint */


/* 	ypgetacctent() - Get account id in YP	*/

#if defined(NIS)
#include <stdio.h>

#if !defined(apollo)
#include <unistd.h>
#endif

#include <string.h>
#include <sys/types.h>
#include <pwd.h>
#include <rpcsvc/ypclnt.h>

#include "ypgetacctent.h"


static int ypcall(instatus, inkey, inkeylen, inval, invallen, indata)
	int	instatus;
	char	*inkey;
	int	inkeylen;
	char	*inval;
	int	invallen;
	char	*indata;

	{ static char	name[NAME_LEN];
	  static char	account[ACCT_LEN];
	  static int	firsttime = TRUE; 
	  char		*cp = NULL; 
	  char		buf[BUFSIZ];

	  /*
	   * There is an error
	   */

	  if (instatus != 1)
	    { *indata = ENDSTR_CHR;

	      return(1);
	    }

	  /*
	   * It is the first time
	   */

	  if (firsttime == TRUE)
	    { (void)strcpy(name, strtok(indata, COLON_STR));

	      (void)strcpy(account, strtok((char *)NULL, COLON_STR));


	      firsttime = FALSE; 
	    }
	
	  (void)strncpy(buf, inval, (size_t)invallen);

	  buf[invallen] = '\0';		/* see man strncpy */

	  cp = strtok(inval, COLON_STR);

	  if (strcmp(cp, name) != 0)
	    /*
	     * Username does not match.
	     */
	    return(0);
	  else
	    { cp = strtok((char *)NULL, COLON_STR);

	      if (*account == STAR_CHR)
		{ (void)strcpy(indata, buf);

		  return(1);
		} 
	      else
		{ if (strcmp(cp, account) != 0)
		    return(0);

		  (void)strcpy(indata, buf); 

		  return(1);
		}
	    }	
	}


char	*ypgetacctent(pwd, account, buffer, bufferlen)
	struct passwd	*pwd; 
	char		*account;
	char		*buffer;
	int		bufferlen;

	{ struct ypall_callback	call;
	  char			*domain = NULL; 
	  char			buf[BUFSIZ];
	  char			*outval = NULL;
	  int			outvallen = 0;

	  /*
	   * Consulting YP.
	   */

	  if (yp_get_default_domain(&domain) != 0) 
	    return(NULL);

	  if (account == NULL)
	    { (void)sprintf(buf, "%s:%s", pwd->pw_name, DFLT_SEQ_STR);

	      if (yp_match(domain, ACCT_MAP_NAME, buf, (int)strlen(buf), &outval, &outvallen) == 0)
		{ (void)strncpy(buffer, outval, (size_t)bufferlen);

		  return(buffer);
		}
	    }
	  else
	    { int	seq = 0;

	      for (seq = 0; seq < MAX_SEQ_NUM; seq++)
		{ (void)sprintf(buf, "%s:%d", pwd->pw_name, seq);

		  if (yp_match(domain, ACCT_MAP_NAME, buf, (int)strlen(buf), &outval, &outvallen) == 0)
		    { char	fub[BUFSIZ],
				*acctfld = NULL;;

		      (void)strcpy(fub, buf);

		      acctfld = strtok(fub, COLON_STR);

		      acctfld = strtok((char *)NULL, COLON_STR);

		      if (strcmp(account, acctfld) == 0)
			{ (void)strncpy(buffer, outval, (size_t)bufferlen);

			  return(buffer);
			}
		    }
		}

	      (void)sprintf(buf, "%s:%s", pwd->pw_name, account);

	      call.foreach	= ypcall;
	      call.data		= buf;

	      if (yp_all(domain, ACCT_MAP_NAME, &call) != 0) 
	        return(NULL);

	      if (*(char *)(call.data) == ENDSTR_CHR)
	        return(NULL);
	      else
	        { (void)strncpy(buffer, call.data, (size_t)bufferlen);

	          return(buffer);
	        }
	    }

	  return(NULL);
	}

#endif /* NIS */
