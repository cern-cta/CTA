/******************************************************************************
 *                      rtcp_lbl.h
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
 * @(#)$RCSfile$ $Revision$ $Release$ $Date$ $Author$
 *
 * 
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef H_RTCP_LBL_H 
#define H_RTCP_LBL_H 1

#include <stdint.h>

int checkeofeov (int	tapefd, char	*path);

int wrthdrlbl(
	const int      tapefd,
	char *const    path,
	const uint32_t tapeFlushMode);

int wrteotmrk(
	const int      tapefd,
	char *const    path,
	const uint32_t tapeFlushMode);

int wrttrllbl(
	const int   tapefd,
	char *const path,
	char *const labelid,
	const int  nblocks,
	const uint32_t tapeFlushMode);

int deltpfil (int	tapefd, char	*path);

#endif /* H_RTCP_LBL_H */
