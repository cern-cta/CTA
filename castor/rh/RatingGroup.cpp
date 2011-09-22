/******************************************************************************
 *                      RatingGroup.cpp
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
 * @author Dennis Waldron
 *****************************************************************************/

// Include files
#include "getconfent.h"
#include "RatingGroup.hpp"
#include <errno.h>
#include <sstream>
#include <string.h>


//-----------------------------------------------------------------------------
// Constructor
//-----------------------------------------------------------------------------
castor::rh::RatingGroup::RatingGroup(const std::string name)
  throw(castor::exception::Exception) :
  m_nbRequests(0),
  m_interval(0),
  m_groupName(name) {
  
  // Variables
  const char *value;

  // Extract the threshold value for the rating group, this option defines the
  // number of allowed requests within a given time interval.
  std::ostringstream option;
  option << name << "Threshold";

  value = getconfent("RH", option.str().c_str(), 0);
  if (value == NULL) {
    castor::exception::Exception ex(EINVAL);
    ex.getMessage() << "Missing configuration option RH/" << option.str();
    throw ex;
  }

  // Parse the threshold value making sure that the format is correct and that
  // the values are all positive.
  std::string token;
  std::istringstream iss(value);
  iss >> m_nbRequests;                // Requests
  std::getline(iss, token, '/');
  iss >> m_interval;                  // Interval

  // A failure occurred in parsing?
  if (iss.fail()) {
    castor::exception::Exception ex(EINVAL);
    ex.getMessage() << "Invalid syntax for configuration option RH/"
                    << option.str();
    throw ex;
  }

  // Check that the interval is a multiple of 10
  if (m_interval % 10 != 0) {
    castor::exception::Exception ex(EINVAL);
    ex.getMessage() << "Invalid interval for configuration option RH/"
                    << option.str()
                    << " - must be a multiple of 10";
    throw ex;
  }

  // Extract the response value for the rating group
  option.str("");
  option << name << "Response";

  value = getconfent("RH", option.str().c_str(), 0);
  if (value == NULL) {
    castor::exception::Exception ex(EINVAL);
    ex.getMessage() << "Missing configuration option RH/" << option.str();
    throw ex;
  }

  // Check that the response is valid
  if (strcmp(value, "always-accept")    &&
      strcmp(value, "close-connection") &&
      strcmp(value, "reject-with-error")) {
    castor::exception::Exception ex(EINVAL);
    ex.getMessage() << "Invalid option for RH/" << option.str()
                    << " - must be one of 'always-accept', 'close-connection' "
                    << "or 'reject-with-error'";
    throw ex;
  }
  m_response = value;
}
