/******************************************************************************
 *                      castor/tape/net/Constants.hpp
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

#ifndef CASTOR_TAPE_NET_CONSTANTS_HPP
#define CASTOR_TAPE_NET_CONSTANTS_HPP 1

#include <stdint.h>
#include <stdlib.h>


namespace castor {
namespace tape   {
namespace net    {
  	
  const size_t HOSTNAMEBUFLEN    = 256;
  const int    LISTENBACKLOG     = 2;
  const size_t SERVICENAMEBUFLEN = 256;

} // namespace net
} // namespace tape
} // namespace castor


#endif // CASTOR_TAPE_NET_CONSTANTS_HPP
