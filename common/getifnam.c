/*
 * Copyright (C) 1991-1998 by CERN IT-PDP/SC
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)getifnam.c	1.19 06/05/98   CERN IT-PDP/SC Frederic Hemmer";
#endif /* not lint */

/* getifnam.c   Get connected socket interface name                     */

#include <stdio.h>                      /* Standard input/output        */
#include <sys/types.h>                  /* Standard data types          */
#if !defined(_WIN32)
#include <sys/socket.h>                 /* Socket interface             */
#include <netinet/in.h>                 /* Internet data types          */
#endif
#if defined(SOLARIS)
#include <sys/sockio.h>
#endif
#include <errno.h>                      /* Error numbers                */
#include <serrno.h>                     /* Special error numbers        */
#include <log.h>                        /* Genralized error logger      */
#if defined(_AIX)
#include <sys/time.h>			/* needed for if.h		*/
#endif /* AIX */
#if !defined(_WIN32)
#include <net/if.h>                     /* Network interfaces           */
#include <sys/ioctl.h>                  /* ioctl() definitions          */
#endif
#if defined(vms)
#include <string.h>			/* String manipulation 		*/
#endif
#include <trace.h>                      /* tracing definitions          */

#if defined(vms) && (vms == 1)

#if (!defined(TWG) && !defined(MULTINET) && !defined(TCPWARE) && !defined(UCX))
/* We generate a compiler error (#error not defined everywhere!) */
!!! YOU MUST SPECIFY either MULTINET or TWG or TCPWARE or UCX !!!
#endif /* TWG */

#endif /* vms */

#if !defined(vms) && !defined(linux)
extern char     *sys_errlist[];         /* External error list          */
#endif /* vms && linux */

#include <net.h>			/* Networking specifics		*/

static char     ifname[16];             /* Interface name               */

/*
 * Note: originally, we did not create a socket to get the interfaces
 *       but (at least on SunOS 4.0.3), the first call on the connected
 *       socket returned a ifc.ifc_len == 0. Even with consecutive calls
 *       it failed. With a new socket creation it works however.
 */

#if defined(CRAY) || ( defined(apollo) && ULTRA ) || ( defined(sun) && ULTRA )

/*
 * CRAY/Ultra doesn't support yet the full SIOCGIFCONF ioctl()
 * We then just use the UNET_GETO ioctl() and return uvm0 if
 * it is the right interface, calling _getifnam if not.
 */

#include <ultra/unetiso.h>
#include <ultra/unettlad.h>
#include <ultra/unetrb.h>
#include <ultra/unetioct.h>

char    *_getifnam();                    /* Forward reference    */

char    *getifnam(s)
int     s;
{
	union   uioc_req ur;

	if (ioctl(s, UNET_GETO, &ur) < 0)       {
		return(_getifnam(s));
	}
	else    {
		return("uvm0");
	}
}
#define getifnam(x)     _getifnam(x)
#endif /* CRAY || ( apollo && ULTRA )*/

char    *getifnam(s)
int     s;
{
#if defined(_WIN32)
	static char dummy_ifce[4]="???";	/* We don't know yet how to get it */
	return (dummy_ifce);
#else
	struct  ifconf  ifc;            /* Interface configuration      */
	struct  ifreq   *ifr;           /* Interface request            */
	char    buf[BUFSIZ];            /* A buffer                     */
	struct  sockaddr_in addr;       /* Internet address             */
	int     addrlen;                /* Internet address length      */
	long    binaddr;                /* Store address                */
	int     n;                      /* # of interfaces              */
	int     found=0;                /* Found the interface ?        */
	int     s_s;                    /* A socket to get interfaces   */
#if defined(_AIX) || defined(__Lynx__)
	struct	sockaddr_in	*sp;	/* Ptr to sockaddr in ifreq buf	*/  
	char	*endp;			/* End of ifreq buffer		*/	
#endif

	INIT_TRACE("COMMON_TRACE");
	TRACE(1,"getifnam", "getifnam(%d) entered",s);
	addrlen = sizeof(struct sockaddr_in);
	if (getsockname(s, (struct  sockaddr *)&addr, &addrlen) < 0) {
		TRACE(2,"getifnam", "getsockname returned %d", errno);
#if !defined(vms)
		log(LOG_ERR, "getsockname: %s\n",sys_errlist[errno]);
#else
		log(LOG_ERR, "getsockname: %s\n", vms_errno_string());
#endif /* vms */
		END_TRACE();
		return(NULL);
	}
	else    {
		binaddr = addr.sin_addr.s_addr;
	}
	if ((s_s = socket(AF_INET, SOCK_DGRAM, 0)) < 0) {
#if !defined(vms)
		log(LOG_ERR, "socket: %s\n",sys_errlist[errno]);
#else
		log(LOG_ERR, "socket: %s\n", vms_errno_string());
#endif /* vms */
		return(NULL);
	}

	ifc.ifc_len = sizeof(buf);
	ifc.ifc_buf = buf;
	ifr = ifc.ifc_req;
	if ((n = ioctl(s_s, SIOCGIFCONF, (char *)&ifc)) < 0) {
		TRACE(2,"getifnam", "netioctl returned %d", errno);
#if !defined(vms)
		log(LOG_ERR, "ioctl(SIOCGIFCONF): %s\n",sys_errlist[errno]);
#else
		log(LOG_ERR, "ioctl(SIOCGIFCONF): %s\n",vms_errno_string());
#endif /* vms */
		(void) netclose(s_s);
		END_TRACE();
		return(NULL);
	}
	else    {
#if defined(_AIX) || defined(__Lynx__)
		endp = (char *) ifr + ifc.ifc_len;
		sp = (struct sockaddr_in *) &ifr->ifr_addr;
		while ((char *) sp < endp) {
			if ((sp->sin_family == AF_INET) &&
				(binaddr == sp->sin_addr.s_addr)) {
				strcpy(ifname, ifr->ifr_name);
				found ++;
				break;
			}
			ifr = (struct ifreq *)((char *) sp + sp->sin_len);
			sp = (struct sockaddr_in *) &ifr->ifr_addr;
		}
#else
		for (n = ifc.ifc_len/sizeof(struct ifreq); --n >= 0; ifr++){
			memcpy (&addr, &ifr->ifr_addr, sizeof(struct sockaddr_in));
			TRACE(2, "getifnam", "interface # %d, comparing 0X%X to 0X%X", n, binaddr, addr.sin_addr.s_addr);
			if (binaddr == addr.sin_addr.s_addr)    {
				strcpy(ifname, ifr->ifr_name);
				found ++;
				break;
			}
		}
#endif
	}
	(void) netclose(s_s);
	if (found)      {
		TRACE(2,"getifnam", "returning %s", ifname);
		END_TRACE();
		return(ifname);
	}
	else    {
		serrno = SEBADIFNAM;
		TRACE(2,"getifnam", "returning NULL");
		END_TRACE();
		return((char *) NULL);
	}
#endif
}


