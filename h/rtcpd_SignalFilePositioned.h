/******************************************************************************
 *                h/rtcpd_SignalFilePositioned.h
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

#ifndef H_RTCPD_SIGNALFILEPOSITIONED_H
#define H_RTCPD_SIGNALFILEPOSITIONED_H 1

#include "h/log.h"
#include "h/osdep.h"
#include "h/rtcp.h"
#include "h/rtcp_server.h"
#include "h/serrno.h"
#include "h/tplogger_api.h"

#include <errno.h>

/*
 * Signal to disk IO thread that file has been positioned (tape read with
 * stager only).
 */
EXTERN_C int rtcpd_SignalFilePositioned(processing_cntl_t *const proc_cntl,
  tape_list_t *const tape, file_list_t *const file);

#endif /* H_RTCPD_SIGNALFILEPOSITIONED_H */
