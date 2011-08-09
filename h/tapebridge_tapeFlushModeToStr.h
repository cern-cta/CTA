/******************************************************************************
 *                h/tapebridge_tapeFlushModeToStr.h
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

#ifndef H_TAPEBRIDGE_TAPEFLUSHMODETOSTR_H
#define H_TAPEBRIDGE_TAPEFLUSHMODETOSTR_H 1

#include "h/osdep.h"

#include <stdint.h>

/**
 * Returns a pointer to the string literal representing the specified tape-flush
 * mode.  If the tape-flush mode is unknown then a pointer to the the string literal
 * "UNKNOWN" is returned.
 *
 * @return A pointer to the string literal representing the specified tape-flush
 *         mode, or a pointer to the string literal "UNKNOWN" if the tape-flush mode
 *         is unknown.
 */
EXTERN_C const char *tapebridge_tapeFlushModeToStr(const uint32_t tapeFlushMode);

#endif /* H_TAPEBRIDGE_TAPEFLUSHMODETOSTR_H */
