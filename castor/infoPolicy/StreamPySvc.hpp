/******************************************************************************
 *                      StreamPySvc.hpp
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
 * @(#)$RCSfile: StreamPySvc.hpp,v $ $Revision: 1.2 $ $Release$ $Date: 2008/05/28 08:07:46 $ $Author: gtaur $
 *
 * @author Giulia Taurelli
 *****************************************************************************/

#ifndef STREAM_PYSVC_HPP
#define STREAM_PYSVC_HPP 

#include "castor/infoPolicy/PySvc.hpp"
#include <string>

namespace castor{
  namespace infoPolicy{
      class StreamPySvc: public castor::infoPolicy::PySvc{
      public:
	StreamPySvc(): PySvc(){};
	StreamPySvc(std::string module):PySvc(module){};
	virtual int applyPolicy(castor::infoPolicy::PolicyObj* pObj) throw(castor::exception::Exception);
      };
     
  } // end namespace infoPolicy
} // end namespace castor 



#endif //  STREAM_PYSVC_HPP 
