/******************************************************************************
 *                test/tapeunittests/test_exception.hpp
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
 * @author Steven.Murray@cern.ch
 *****************************************************************************/

#ifndef TEST_TAPEUNITTESTS_TESTEXCEPTION_HPP
#define TEST_TAPEUNITTESTS_TESTEXCEPTION_HPP 1

#include <exception>
#include <string>

class test_exception: public std::exception {
private:
  std::string m_msg;

public:
  test_exception(const std::string msg);

  test_exception(const test_exception &tx);

  ~test_exception() throw();

  test_exception &operator=(const test_exception &tx);

  const char* what() const throw();
};

#endif // TEST_TAPEUNITTESTS_TESTEXCEPTION_HPP
