/*
 * $Id: stage_constants.h,v 1.1 2001/02/01 16:28:16 jdurand Exp $
 */

#ifndef __stage_constants_h
#define __stage_constants_h

#ifdef MAXPATH
#undef MAXPATH
#endif
#define MAXPATH 80	/* maximum path length */
#ifdef MAXVSN
#undef MAXVSN
#endif
#define	MAXVSN 3	/* maximum number of vsns/vids on a stage command */

#endif /* __stage_constants_h */
