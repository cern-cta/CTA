/*
 * $Id: stage.h,v 1.71 2002/05/30 12:42:58 bcouturi Exp $
 */

/*
 * Copyright (C) 1993-2000 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef __stage_h
#define __stage_h

#include "Cns_api.h"

/* ======================================= */
/* stage constants used inside daemon only */
/* ======================================= */
#define LAST_RWCOUNTERSFS_TPPOS 1
#define LAST_RWCOUNTERSFS_FILCP 2

/* ==================================== */
/* stage macros used inside daemon only */
/* ==================================== */
/* Returns real size on disk taking number of blocks used (using rfio_stat()) and filename */
#define BLOCKS_TO_SIZE(blocks,ipath) (((u_signed64) blocks) * findblocksize(ipath))

#include "Ctape_constants.h"
#include "rtcp_constants.h"
#include "stage_constants.h"
#include "stage_messages.h"
#include "stage_macros.h"

			/* stage daemon stream modes */

#define WRITE_MODE 1
#define READ_MODE -1

			/* stage generic structures */

#include "stage_struct.h"

			/* stage daemon internal tables */

#include "stage_server_struct.h"

#ifdef MONITOR

/* Defining monitoring method prototype from Cmonit_stageclient.c */
EXTERN_C int DLL_DECL Cmonit_send_stager_status _PROTO ((time_t));

#endif

#endif /* __stage_h */
