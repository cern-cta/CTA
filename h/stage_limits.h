/*
 * $Id: stage_limits.h,v 1.1 2005/06/22 11:59:18 jdurand Exp $
 */

#ifndef __stage_limits_h
#define __stage_limits_h

/* =================================== */
/* stage configuration maximum values  */
/* =================================== */
#ifdef  MAXPATH
#undef  MAXPATH
#endif
#define MAXPATH CA_MAXNAMELEN	/* maximum path length */

#ifdef STAGE_MAX_HSMLENGTH
#undef STAGE_MAX_HSMLENGTH /* Limitation on hsm filename length until we get rid of catalog in memory */
#endif
#define STAGE_MAX_HSMLENGTH CA_MAXPATHLEN

#ifdef  MAXVSN
#undef  MAXVSN
#endif
#define	MAXVSN 3	/* maximum number of vsns/vids on a stage command */

#ifdef MAX_NETDATA_SIZE
#undef MAX_NETDATA_SIZE
#endif
#define MAX_NETDATA_SIZE ONE_MB

#ifdef ROOTUIDLIMIT
#undef ROOTUIDLIMIT
#endif
#define ROOTUIDLIMIT 100

#ifdef ROOTGIDLIMIT
#undef ROOTGIDLIMIT
#endif
#define ROOTGIDLIMIT 100

#endif /* __stage_limits_h */
