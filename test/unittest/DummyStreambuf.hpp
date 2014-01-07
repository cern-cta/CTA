/******************************************************************************
 *                test/unittest/DummyStreambuf.hpp
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

#ifndef TEST_UNITTEST_DUMMYSTREAMBUF_HPP
#define TEST_UNITTEST_DUMMYSTREAMBUF_HPP 1

#include <iomanip>
#include <iostream>

namespace unittest {

/**
 * A dummy stream buffer that does nothing.
 */
class DummyStreambuf: public std::streambuf {
  int overflow(int x);
};

} // namespace unittest

#endif // TEST_UNITTEST_DUMMYSTREAMBUF_HPP
