/*
 * $Id: stage_macros.h,v 1.3 2002/11/19 09:04:58 baud Exp $
 */

#ifndef __stage_macros_h
#define __stage_macros_h

#include "stage_constants.h"

/* ==================== */
/* stage generic macros */
/* ==================== */
#ifdef ISSTAGED
#undef ISSTAGED
#endif
#define ISSTAGED(stcp)     ((stcp->status & 0xF0) == STAGED)

#ifdef ISPUT_FAILED
#undef ISPUT_FAILED
#endif
#define ISPUT_FAILED(stcp) ((stcp->status & PUT_FAILED) == PUT_FAILED)

#ifdef ISSTAGEIN
#undef ISSTAGEIN
#endif
#define ISSTAGEIN(stcp)    ((stcp->status & 0xF) == STAGEIN)

#ifdef ISSTAGEOUT
#undef ISSTAGEOUT
#endif
#define ISSTAGEOUT(stcp)   ((stcp->status & 0xF) == STAGEOUT)

#ifdef ISSTAGEWRT
#undef ISSTAGEWRT
#endif
#define ISSTAGEWRT(stcp)   ((stcp->status & 0xF) == STAGEWRT)

#ifdef ISSTAGEPUT
#undef ISSTAGEPUT
#endif
#define ISSTAGEPUT(stcp)   ((stcp->status & 0xF) == STAGEPUT)

#ifdef ISSTAGEALLOC
#undef ISSTAGEALLOC
#endif
#define ISSTAGEALLOC(stcp) ((stcp->status & 0xF) == STAGEALLOC)

#ifdef ISWAITING
#undef ISWAITING
#endif
#define ISWAITING(stcp)    (((stcp->status & 0xF0) == WAITING_SPC) || ((stcp->status & 0xF0) == WAITING_REQ) || ((stcp->status & (WAITING_MIGR|WAITING_NS)) != 0))

#ifdef ISCASTORMIG
#undef ISCASTORMIG
#endif
#define ISCASTORMIG(stcp) ((stcp->t_or_d == 'h') && (! ISPUT_FAILED(stcp)) && ((stcp->status & CAN_BE_MIGR) == CAN_BE_MIGR))

#ifdef ISCASTORBEINGMIG
#undef ISCASTORBEINGMIG
#endif
#define ISCASTORBEINGMIG(stcp) (ISCASTORMIG(stcp) && (((stcp->status & BEING_MIGR) == BEING_MIGR) || (stcp->status == (STAGEPUT|CAN_BE_MIGR))))

#ifdef ISCASTORWAITINGMIG
#undef ISCASTORWAITINGMIG
#endif
#define ISCASTORWAITINGMIG(stcp) (ISCASTORMIG(stcp) && ((stcp->status & WAITING_MIGR) == WAITING_MIGR))

#ifdef ISCASTORWAITINGNS
#undef ISCASTORWAITINGNS
#endif
#define ISCASTORWAITINGNS(stcp) ((stcp->t_or_d == 'h') && ((stcp->status & WAITING_NS) == WAITING_NS))

#ifdef ISCASTORCANBEMIG
#undef ISCASTORCANBEMIG
#endif
#define ISCASTORCANBEMIG(stcp) (ISCASTORMIG(stcp) && (! (ISCASTORBEINGMIG(stcp) || ISCASTORWAITINGMIG(stcp))))

#ifdef ISCASTORFREE4MIG
#undef ISCASTORFREE4MIG
#endif
#define ISCASTORFREE4MIG(stcp) ((stcp->status == STAGEOUT) || (stcp->status == (STAGEOUT|PUT_FAILED)) || (stcp->status == (STAGEOUT|CAN_BE_MIGR)) || (stcp->status == (STAGEOUT|CAN_BE_MIGR|PUT_FAILED)) || (stcp->status == (STAGEWRT|CAN_BE_MIGR|PUT_FAILED)))

#ifdef ISHPPSMIG
#undef ISHPPSMIG
#endif
#define ISHPSSMIG(stcp) ((stcp->t_or_d == 'm') && ((stcp->status == STAGEPUT) || (stcp->status == STAGEWRT)))

#ifdef ISUIDROOT
#undef ISUIDROOT
#endif
#define ISUIDROOT(uid) ((uid) < ROOTUIDLIMIT)

#ifdef ISGIDROOT
#undef ISGIDROOT
#endif
#define ISGIDROOT(gid) ((gid) < ROOTGIDLIMIT)

#ifdef ISROOT
#undef ISROOT
#endif
#define ISROOT(uid,gid) (ISUIDROOT(uid) || ISGIDROOT(gid))

#ifdef ISHPSS
#undef ISHPSS
#endif
#define ISHPSS(xfile)   (strncmp (xfile, "/hpss/"  , 6) == 0 || strstr (xfile, ":/hpss/"  ) != NULL)

#ifdef ISHPSSHOST
#undef ISHPSSHOST
#endif
#define ISHPSSHOST(xfile) (strstr(xfile,"hpss") == xfile )

#ifdef ISCASTOR
#undef ISCASTOR
#endif
#ifdef NSROOT
#define ISCASTOR(xfile) (strncmp (xfile, NSROOT "/", strlen(NSROOT) + 1) == 0 || strstr (xfile, ":" NSROOT "/") != NULL)
#else
#define ISCASTOR(xfile) 0
#endif

#ifdef ISCASTORHOST
#undef ISCASTORHOST
#endif
#if (defined(NSHOST) && defined(NSHOSTPFX))
#define ISCASTORHOST(xfile) ((strstr(xfile,NSHOST) == xfile) || (strstr(xfile,NSHOSTPFX) == xfile))
#else
#ifdef NSHOST
#define ISCASTORHOST(xfile) (strstr(xfile,NSHOST) == xfile)
#else
#ifdef NSHOSTPFX
#define ISCASTORHOST(xfile) (strstr(xfile,NSHOSTPFX) == xfile)
#else
#define ISCASTORHOST(xfile) 0
#endif
#endif
#endif

#ifdef HPSSFILE
#undef HPSSFILE
#endif
#define HPSSFILE(xfile)   strstr(xfile,"/hpss/")

#ifdef CASTORFILE
#undef CASTORFILE
#endif
#ifdef NSROOT
#define CASTORFILE(xfile) strstr(xfile,NSROOT "/")
#else
#define CASTORFILE(xfile) NULL
#endif

/* Returns uppercased version of a string */
#ifdef UPPER
#undef UPPER
#endif
#define UPPER(s) \
	{ \
	char *q; \
	if (((s) != NULL) && (*(s) != '\0')) \
		for (q = s; *q; q++) \
			if (*q >= 'a' && *q <= 'z') *q = *q + ('A' - 'a'); \
	}


#define RFIO_STAT64(path,statbuf) ((findpoolname(path) == NULL) ? rfio_stat64(path,statbuf) : rfio_mstat64(path,statbuf))

#define RFIO_STAT64_FUNC(path) ((findpoolname(path) == NULL) ? "rfio_stat64" : "rfio_mstat64")

#define RFIO_UNLINK(path) ((findpoolname(path) == NULL) ? rfio_unlink(path) : rfio_munlink(path))

#define RFIO_UNLINK_FUNC(path) ((findpoolname(path) == NULL) ? "rfio_unlink" : "rfio_munlink")

#define PRE_RFIO { rfio_errno = serrno = 0; }

#define ISTAPESERRNO(serrno) ((serrno == ETDNP)    || \
                              (serrno == ETSYS)    || \
                              (serrno == ETPRM)    || \
                              (serrno == ETRSV)    || \
                              (serrno == ETNDV)    || \
                              (serrno == ETIDG)    || \
                              (serrno == ETNRS)    || \
                              (serrno == ETIDN)    || \
                              (serrno == ETLBL)    || \
                              (serrno == ETFSQ)    || \
                              (serrno == ETINTR)   || \
                              (serrno == ETEOV)    || \
                              (serrno == ETRLSP)   || \
                              (serrno == ETBLANK)  || \
                              (serrno == ETCOMPA)  || \
                              (serrno == ETHWERR)  || \
                              (serrno == ETPARIT)  || \
                              (serrno == ETUNREC)  || \
                              (serrno == ETNOSNS)  || \
                              (serrno == ETRSLT)   || \
                              (serrno == ETVBSY)   || \
                              (serrno == ETDCA)    || \
                              (serrno == ETNRDY)   || \
                              (serrno == ETABSENT) || \
                              (serrno == ETARCH)   || \
                              (serrno == ETHELD)   || \
                              (serrno == ETNXPD)   || \
                              (serrno == ETOPAB)   || \
                              (serrno == ETVUNKN)  || \
                              (serrno == ETWLBL)   || \
                              (serrno == ETWPROT)  || \
                              (serrno == ETWVSN))

#define ISVDQMSERRNO(serrno) ((serrno == EVQSYERR)     || \
                              (serrno == EVQINCONSIST) || \
                              (serrno == EVQREPLICA)   || \
                              (serrno == EVQNOVOL)     || \
                              (serrno == EVQNODRV)     || \
                              (serrno == EVQNOSVOL)    || \
                              (serrno == EVQNOSDRV)    || \
                              (serrno == EVQALREADY)   || \
                              (serrno == EVQUNNOTUP)   || \
                              (serrno == EVQBADSTAT)   || \
                              (serrno == EVQBADID)     || \
                              (serrno == EVQBADJOBID)  || \
                              (serrno == EVQNOTASS)    || \
                              (serrno == EVQBADVOLID)  || \
                              (serrno == EVQREQASS)    || \
                              (serrno == EVQDGNINVL)   || \
                              (serrno == EVQPIPEFULL)  || \
                              (serrno == EVQHOLD)      || \
                              (serrno == EVQEOQREACHED))

#endif /* __stage_macros_h */
