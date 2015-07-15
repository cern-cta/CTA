/******************************************************************************
 *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2003  CERN
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 *
 *
 * 
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#pragma once

			/* rbtsubr return codes */

#define	RBT_OK		0	/* Ok or error should be ignored */
#define	RBT_NORETRY	1	/* Unrecoverable error (just log it) */
#define	RBT_SLOW_RETRY	2	/* Should release drive & retry in 600 seconds */
#define	RBT_FAST_RETRY	3	/* Should retry in 60 seconds */
#define	RBT_DMNT_FORCE	4	/* Should do first a demount force */
#define	RBT_CONF_DRV_DN	5	/* Should configure the drive down */
#define	RBT_OMSG_NORTRY	6	/* Should send a msg to operator and exit */
#define	RBT_OMSG_SLOW_R 7	/* Ops msg (nowait) + release drive + slow retry */
#define	RBT_OMSGR	8	/* Should send a msg to operator and wait */
#define	RBT_UNLD_DMNT	9	/* Should unload the tape and retry demount */
 
