/******************************************************************************
 *                      RequestReplierCInt.hpp
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
 * @(#)$RCSfile: RequestReplierCInt.hpp,v $ $Revision: 1.1 $ $Release$ $Date: 2004/11/16 15:45:34 $ $Author: sponcec3 $
 *
 * 
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef REPLIER_REQUESTREPLIERCINT_HPP 
#define REPLIER_REQUESTREPLIERCINT_HPP 1

/// Definition of the RequestReplier C struct
struct Creplier_RequestReplier_t {
  /// The C++ object
  castor::replier::RequestReplier* rr;
  /// A placeholder for an error message
  std::string errorMsg;
};

#endif // REPLIER_REQUESTREPLIERCINT_HPP
