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
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#pragma once

#include <exception>
#include <string>

namespace tape {

/**
 * Exception class used to implement the tpconfig command-line tool.
 */
class TpConfigException: public std::exception {
public:

  /**
   * Constructor.
   */
  TpConfigException(const std::string &what, const int returnValue) throw();

  /**
   * Destructor.
   */
  ~TpConfigException() throw();

  /**
   * Returns the description of the exception.
   */
  const char *what() const throw();

  /**
   * Returns the exit value of the tpconfig command-line tool.
   */
  int exitValue() const throw();

private:

  /**
   * Description of the exception.
   */
  std::string m_what;

  /**
   * The exit value of the tpconfig command-line tool.
   */
  int m_exitValue;

}; // class TpConfigException

} // namespace tape
