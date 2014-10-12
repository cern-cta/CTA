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

#include "castor/tape/tapeserver/exception/OpenSSL.hpp"
#include <openssl/err.h>

castor::tape::server::exception::OpenSSL::OpenSSL( const std::string & what) {
  std::stringstream w;
  if (what.size())
    w << what << " ";
  // Dump the OpenSSL error stack
  // (open SSL maintains a stack of errors per thread)
  while (unsigned long SSLError = ::ERR_get_error()) {
    // SSL errors are stored in at least 120 char buffers
    const size_t len = 200;
    char buff[len];
    ::ERR_error_string_n(SSLError, buff, len);
    w << "[" << buff << "]";
  }
  // Flush the SSL error queue
  ::ERR_clear_error();
  getMessage().str(w.str());
}
