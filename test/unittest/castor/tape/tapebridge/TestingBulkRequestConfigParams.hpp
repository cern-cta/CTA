/******************************************************************************
 *    test/unittest/castor/tape/tapebridge/TestingBulkRequestConfigParams.hpp
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

#ifndef TEST_UNITTEST_CASTOR_TAPE_TAPEBRIDGE_TESTINGBULKREQUESTCONFIGPARAMS_HPP
#define TEST_UNITTEST_CASTOR_TAPE_TAPEBRIDGE_TESTINGBULKREQUESTCONFIGPARAMS_HPP 1

#include "castor/tape/tapebridge/BulkRequestConfigParams.hpp"
#include "test/unittest/test_exception.hpp"

#include <exception>

namespace castor     {
namespace tape       {
namespace tapebridge {

class TestingBulkRequestConfigParams: public BulkRequestConfigParams {
public:
  void determineBulkRequestMigrationMaxBytes()
    throw(castor::exception::InvalidArgument, std::exception) {
    try {
      BulkRequestConfigParams::determineBulkRequestMigrationMaxBytes();
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

  void determineBulkRequestMigrationMaxFiles()
    throw(castor::exception::InvalidArgument, std::exception) {
    try {
      BulkRequestConfigParams::determineBulkRequestMigrationMaxFiles();
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

  void determineBulkRequestRecallMaxBytes()
    throw(castor::exception::InvalidArgument, std::exception) {
    try {
      BulkRequestConfigParams::determineBulkRequestRecallMaxBytes();
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

  void determineBulkRequestRecallMaxFiles()
    throw(castor::exception::InvalidArgument, std::exception) {
    try {
      BulkRequestConfigParams::determineBulkRequestRecallMaxFiles();
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

  using BulkRequestConfigParams::setBulkRequestMigrationMaxBytes;
  using BulkRequestConfigParams::setBulkRequestMigrationMaxFiles;
  using BulkRequestConfigParams::setBulkRequestRecallMaxBytes;
  using BulkRequestConfigParams::setBulkRequestRecallMaxFiles;
}; // class TestingBulkRequestConfigParams

} // namespace tapebridge
} // namespace tape
} // namespace castor

#endif // TEST_UNITTEST_CASTOR_TAPE_TAPEBRIDGE_TESTINGBULKREQUESTCONFIGPARAMS_HPP
