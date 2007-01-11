/******************************************************************************
 *                      OraResultSet.cpp
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
 * @(#)$RCSfile: OraResultSet.cpp,v $ $Revision: 1.3 $ $Release$ $Date: 2007/01/11 10:03:45 $ $Author: itglp $
 *
 * 
 *
 * @author Giuseppe Lo Presti, giuseppe.lopresti@cern.ch
 *****************************************************************************/

#include "OraResultSet.hpp" 

castor::db::ora::OraResultSet::OraResultSet(oracle::occi::ResultSet* rset, oracle::occi::Statement* statement) :
  m_statement(statement),
  m_rset(rset)
{
}

castor::db::ora::OraResultSet::~OraResultSet()
{
  try {
	  m_statement->closeResultSet(m_rset);
  } catch(oracle::occi::SQLException ignored) {}
}

bool castor::db::ora::OraResultSet::next()
{
	return (m_rset->next() != oracle::occi::ResultSet::END_OF_FETCH);
}

int castor::db::ora::OraResultSet::getInt(int i)
{
	return m_rset->getInt(i);
}

signed64 castor::db::ora::OraResultSet::getInt64(int i)
{
	return (signed64)m_rset->getDouble(i);
}

u_signed64 castor::db::ora::OraResultSet::getUInt64(int i)
{
	return (u_signed64)m_rset->getDouble(i);
}

float castor::db::ora::OraResultSet::getFloat(int i)
{
	return m_rset->getFloat(i);
}

double castor::db::ora::OraResultSet::getDouble(int i)
{
	return m_rset->getDouble(i);
}
 
std::string castor::db::ora::OraResultSet::getString(int i)
{
	return m_rset->getString(i);
}


