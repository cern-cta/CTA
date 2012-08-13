/******************************************************************************
 *                h/rtcpdIsConfiguredToReuseMount.h
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
 * @author Steven.Murray@cern.ch
 *****************************************************************************/

#ifndef H_RTCPDISCONFIGUREDTOREUSEMOUNT_H
#define H_RTCPDISCONFIGUREDTOREUSEMOUNT_H 1

#include "h/osdep.h"

/**
 * Returns true if the rtpcd daemon has been configured to reuse drive mounts
 * where possible.
 *
 * The rtcpd daemon has been configured to reuse drive mounts if the
 * RTCOPYD/REUSEMOUNT parameter within the /etc/castor.conf file has been set
 * to the value of TRUE.  This value is case insensitive.
 *
 * This function returns false if it encounters an error such as a failure to
 * allocate memory whilst trying to determine the value of the
 * RTCOPYD/REUSEMOUNT parameter.
 *
 * @return True if the RTCOPYD/REUSEMOUNT parameter within the
 *         /etc/castor.conf file has been set to the value of TRUE.
 */
EXTERN_C int rtcpdIsConfiguredToReuseMount();

#endif /* H_RTCPDISCONFIGUREDTOREUSEMOUNT_H */
