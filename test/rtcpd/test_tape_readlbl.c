/*
 * $Id: readlbl.c,v 1.19 2009/03/13 14:20:24 wiebalck Exp $
 */

/*	readlbl - read one possible label record 
 *
 *	return	-1 and serrno set in case of error
 *		 0 if a 80 characters record was read
 *		 1 if the record length was different
 *		 2 for EOF
 *		 3 for blank tape
 */

#include <errno.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>
#include <stdio.h>
#include <sys/utsname.h>
#include "Ctape.h"
#include "Ctape_api.h"
#include "serrno.h"

int readlbl(int tapefd,
            char *path,
            char *lblbuf)
{
	char func[16];
	ENTRY (readlbl);
	RETURN (0);
}
