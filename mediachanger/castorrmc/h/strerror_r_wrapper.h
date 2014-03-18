/******************************************************************************
 *         h/strerror_r_wrapper.h
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

#ifndef H_STRERROR_R_WRAPPER_H
#define H_STRERROR_R_WRAPPER_H 1

#include <stddef.h>

/**
 * This function wraps the XSI compliant version of strerror_r() and therefore
 * writes the string representation of the specified error number to the
 * specified buffer.
 * 
 * @param errnum The error number.
 * @param buf The buffer.
 * @param buflen The length of the buffer.
 */
int strerror_r_wrapper(int errnum, char *buf, size_t buflen);

#endif /* H_STRERROR_R_WRAPPER_H */
