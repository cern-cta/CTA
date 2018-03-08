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

#include "XrootCl.hpp"
#include <sstream>

namespace cta { namespace exception {

XrootCl::XrootCl(const XrdCl::XRootDStatus& status, const std::string & what) {
  std::stringstream w;
  if (what.size())
    w << what << " ";
  w << status.ToStr() << " code:" << status.code 
    << " errNo:" << status.errNo 
    << " status:" << status.status;
  getMessage().str(w.str());
}

void XrootCl::throwOnError(const XrdCl::XRootDStatus& status, std::string context)
{
  if (!status.IsOK()) {
    throw XrootCl(status, context);
  }
}

}} // namespace cta::exception
