/******************************************************************************
 *                      Utils.cpp
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
 * @author Nicola.Bessone@cern.ch Steven.Murray@cern.ch
 *****************************************************************************/

#include "castor/tape/aggregator/Utils.hpp"


void castor::tape::aggregator::Utils::copyString(char *const dst,
  const char *src, const size_t n) throw (castor::exception::Exception) {

  const size_t srcLen = strlen(src);

  if(srcLen >= n) {
    castor::exception::Exception e(EINVAL);

    e.getMessage() << __PRETTY_FUNCTION__
      << ": Source string is longer than destination.  Source length: "
      << srcLen << " Max destination length: " << (n-1);

    throw e;
  }

  strncpy(dst, src, n);
    *(dst+n-1) = '\0'; // Ensure destination is null terminated
}
