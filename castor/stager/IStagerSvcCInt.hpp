/******************************************************************************
 *                      IStagerSvcCInt.hpp
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
 * @(#)$RCSfile: IStagerSvcCInt.hpp,v $ $Revision: 1.1 $ $Release$ $Date: 2004/05/26 15:46:25 $ $Author: sponcec3 $
 *
 * 
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef STAGER_ISTAGERSVCCINT_HPP 
#define STAGER_ISTAGERSVCCINT_HPP 1

/// Definition of the IStagerSvc C struct
struct Cstager_IStagerSvc_t {
  /// The C++ object
  castor::stager::IStagerSvc* stgSvc;
  /// A placeholder for an error message
  std::string errorMsg;
};

#endif // STAGER_ISTAGERSVCCINT_HPP
