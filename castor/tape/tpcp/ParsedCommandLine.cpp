/******************************************************************************
 *                 castor/tape/tpcp/ParsedCommandLine.cpp
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
 * @author Nicola.Bessone@cern.ch Steven.Murray@cern.ch
 *****************************************************************************/
 
#include "castor/tape/tpcp/ParsedCommandLine.hpp"


/*
std::ostream &operator<<(std::ostream &s,
  const castor::tape::tpcp::ParsedCommandLine::RangeList &list) {

  castor::tape::tpcp::ParsedCommandLine::RangeList::iterator funny;

  funny = list.begin();

  list.begin();
  list.end();

  if(funny != list.end()) {
    // Do nothing
  }

  for(castor::tape::tpcp::ParsedCommandLine::RangeList::iterator itor =
    list.begin(); itor != list.end(); itor++) {

    // Write a separating comma if not the first item in the list
    if(itor!=list.begin()) {
      os << ",";
    }

    os << itor->lower << "-";

    // Write out the  upper bound, where 0 means end of tape
    if(itor->upper > 0) {
      os << itor->upper;
    } else {
      os << "END";
    }
  }
}
*/
