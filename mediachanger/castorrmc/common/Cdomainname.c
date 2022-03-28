/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2002-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#include <sys/types.h>
#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include "Castor_limits.h"
#include "Cnetdb.h"
#include "serrno.h"

/* Cdomainname - get domain name */

int Cdomainname(char *name, int namelen)
{
	char hostname[CA_MAXHOSTNAMELEN+1];
	struct hostent *hp;
	char *p;

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
				if ((int)strlen (p) > namelen) {
					serrno = EINVAL;
					return (-1);
				}
				strcpy (name, p);
				return (0);
			}
		}
		fclose (fd);
	}

	/* Try gethostname */

	gethostname (hostname, CA_MAXHOSTNAMELEN+1);

	if ((hp = Cgethostbyname (hostname)) != NULL) {
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
			if ((hp2 = Cgethostbyaddr (haddrarray + i, sizeof(struct in_addr), AF_INET)) != NULL) {
				char **hal;
				if ((p = strchr (hp2->h_name, '.')) != NULL) {
					free (haddrarray);
					p++;
					if ((int)strlen (p) > namelen) {
						serrno = EINVAL;
						return (-1);
					}
					strcpy (name, p);
					return (0);
				}

				/* Look for aliases */

				hal = hp2->h_aliases;
				while (*hal) {
					if ((p = strchr (*hal, '.')) != NULL) {
						free (haddrarray);
						p++;
						if ((int)strlen (p) > namelen) {
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
