/******************************************************************************
 *                      Param.cpp
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
 * @(#)$RCSfile: Param.cpp,v $ $Revision: 1.1 $ $Release$ $Date: 2005/04/05 11:51:33 $ $Author: sponcec3 $
 *
 * 
 *
 * @author Sebastien Ponce
 *****************************************************************************/

// Include Files
#include <sstream>
#include "castor/dlf/Param.hpp"
#include "castor/ObjectSet.hpp"
#include "castor/IObject.hpp"

// -----------------------------------------------------------------------
// Constructor
// -----------------------------------------------------------------------
castor::dlf::Param::Param(char* name,
                          castor::IObject* value) :
  m_deallocate(true) {
  m_cParam.name = name;
  m_cParam.type = DLF_MSG_PARAM_STR;
  std::ostringstream s;
  castor::ObjectSet set;
  value->print(s, "", set);
  m_cParam.par.par_string = strdup(s.str().c_str());
}
