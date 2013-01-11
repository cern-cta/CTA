/******************************************************************************
 *    test/unittest/castor/tape/tapebridge/TestingTapeFlushConfigParams.hpp
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

#ifndef TEST_UNITTEST_CASTOR_TAPE_TAPEBRIDGE_TESTINGTAPEFLUSHCONFIGPARAMS_HPP
#define TEST_UNITTEST_CASTOR_TAPE_TAPEBRIDGE_TESTINGTAPEFLUSHCONFIGPARAMS_HPP 1

#include "castor/tape/tapebridge/TapeFlushConfigParams.hpp"
#include "test/unittest/test_exception.hpp"

#include <cppunit/extensions/HelperMacros.h>

namespace castor     {
namespace tape       {
namespace tapebridge {

class TestingTapeFlushConfigParams: public TapeFlushConfigParams {
public:
  void determineTapeFlushMode()
    throw(castor::exception::InvalidArgument, std::exception) {
    try {
      TapeFlushConfigParams::determineTapeFlushMode();
    } catch(castor::exception::InvalidArgument &ia) {
      throw ia;
    } catch(castor::exception::Exception &ex) {
      test_exception te(ex.getMessage().str());
      throw te;
    } catch(std::exception &se) {
      throw se;
    } catch(...) {
      test_exception te("Caught an unknown exception");
      throw te;
    }
  }

  void determineMaxBytesBeforeFlush()
    throw(castor::exception::InvalidArgument, std::exception) {
    try {
      TapeFlushConfigParams::determineMaxBytesBeforeFlush();
    } catch(castor::exception::InvalidArgument &ia) {
      throw ia;
    } catch(castor::exception::Exception &ex) {
      test_exception te(ex.getMessage().str());
      throw te;
    } catch(std::exception &se) {
      throw se;
    } catch(...) {
      test_exception te("Caught an unknown exception");
      throw te;
    }
  }

  void determineMaxFilesBeforeFlush()
    throw(castor::exception::InvalidArgument, std::exception) {
    try {
      TapeFlushConfigParams::determineMaxFilesBeforeFlush();
    } catch(castor::exception::InvalidArgument &ia) {
      throw ia;
    } catch(castor::exception::Exception &ex) {
      test_exception te(ex.getMessage().str());
      throw te;
    } catch(std::exception &se) {
      throw se;
    } catch(...) {
      test_exception te("Caught an unknown exception");
      throw te;
    }
  }

  using TapeFlushConfigParams::setTapeFlushMode;
  using TapeFlushConfigParams::setMaxBytesBeforeFlush;
  using TapeFlushConfigParams::setMaxFilesBeforeFlush;
}; // class TestingTapeFlushConfigParams

} // namespace tapebridge
} // namespace tape
} // namespace castor

#endif // TEST_UNITTEST_CASTOR_TAPE_TAPEBRIDGE_TESTINGTAPEFLUSHCONFIGPARAMS_HPP
