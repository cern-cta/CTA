/*
 * Copyright (C) 2002 by CERN/IT/DS/HSM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: Cdomainname.c,v $ $Revision: 1.4 $ $Date: 2003/10/31 12:46:56 $ CERN IT-DS/HSM Jean-Philippe Baud";
#endif /* not lint */

#include <sys/types.h>
#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#if defined(_WIN32)
#include <winsock2.h>
#else
#include <unistd.h>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>
#endif
#include "Castor_limits.h"
#include "Cnetdb.h"
#include "serrno.h"

/* Cdomainname - get domain name */

int DLL_DECL Cdomainname(char *name, int namelen)
{
	char hostname[CA_MAXHOSTNAMELEN+1];
	struct hostent *hp;
	char *p;

#ifndef _WIN32
	FILE *fd;
	/*
	 * try looking in /etc/resolv.conf
	 * putting this here and assuming that it is correct, eliminates
	 * calls to gethostbyname, and therefore DNS lookups. This helps
	 * those on dialup systems.
	 */
	if ((fd = fopen ("/etc/resolv.conf", "r")) != NULL) {
		char line[300];
		while (fgets (line, sizeof(line), fd) != NULL) {
			if ((strncmp (line, "domain", 6) == 0 ||
			    strncmp (line, "search", 6) == 0) && line[6] == ' ') {
				fclose (fd);
				p = line + 6;
				while (*p == ' ')
					p++;
				if (*p)
					*(p + strlen (p) - 1) = '\0';
				if (strlen (p) > namelen) {
					serrno = EINVAL;
					return (-1);
				}
				strcpy (name, p);
				return (0);
			}
		}
		fclose (fd);
	}
#endif

	/* Try gethostname */

	gethostname (hostname, CA_MAXHOSTNAMELEN+1);

	if (hp = Cgethostbyname (hostname)) {
		struct in_addr *haddrarray;
		struct in_addr **haddrlist;
		struct hostent *hp2;
		int i;
		int naddr = 0;

		/* need to save list (at least on Digital Unix) */

		haddrlist = (struct in_addr **)hp->h_addr_list;
		while (*haddrlist) {
			naddr++;
			haddrlist++;
		}
		if ((haddrarray = (struct in_addr *) malloc (naddr * sizeof(struct in_addr))) == NULL) {
			serrno = ENOMEM;
			return (-1);
		}
		haddrlist = (struct in_addr **)hp->h_addr_list;
		for (i = 0; i < naddr; i++) {
			memcpy (haddrarray + i, *haddrlist, sizeof(struct in_addr));
			haddrlist++;
		}

		for (i = 0; i < naddr; i++) {
			if (hp2 = Cgethostbyaddr (haddrarray + i,
			    sizeof(struct in_addr), AF_INET)) {
				char **hal;
				if (p = strchr (hp2->h_name, '.')) {
					free (haddrarray);
					p++;
					if (strlen (p) > namelen) {
						serrno = EINVAL;
						return (-1);
					}
					strcpy (name, p);
					return (0);
				}

				/* Look for aliases */

				hal = hp2->h_aliases;
				while (*hal) {
					if (p = strchr (*hal, '.')) {
						free (haddrarray);
						p++;
						if (strlen (p) > namelen) {
							serrno = EINVAL;
							return (-1);
						}
						strcpy (name, p);
						return (0);
					}
					hal++;
				}
			}
		}
		free (haddrarray);
	}
	serrno = SEINTERNAL;
	return (-1);
}
