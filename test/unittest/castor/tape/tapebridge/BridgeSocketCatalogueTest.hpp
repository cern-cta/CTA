/******************************************************************************
 *    test/unittest/castor/tape/tapebridge/BridgeSocketCatalogueTest.hpp
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

#ifndef TEST_UNITTEST_CASTOR_TAPE_TAPEBRIDGE_BRIDGESOCKETCATALOGUETEST_HPP
#define TEST_UNITTEST_CASTOR_TAPE_TAPEBRIDGE_BRIDGESOCKETCATALOGUETEST_HPP 1

#include "castor/tape/tapebridge/BridgeSocketCatalogue.hpp"
#include "castor/tape/tapebridge/SystemFileCloser.hpp"
#include "castor/tape/utils/utils.hpp"
#include "test/unittest/castor/tape/tapebridge/TestingBridgeSocketCatalogue.hpp"
#include "test/unittest/castor/tape/tapebridge/TraceableDummyFileCloser.hpp"

#include <cppunit/extensions/HelperMacros.h>
#include <exception>
#include <stdint.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <vector>

namespace castor     {
namespace tape       {
namespace tapebridge {

class BridgeSocketCatalogueTest: public CppUnit::TestFixture {
public:

  BridgeSocketCatalogueTest() {
  }

  void setUp() {
  }

  void tearDown() {
  }

  void testConstructor() {
    TraceableDummyFileCloser fileCloser;

    {
      BridgeSocketCatalogue catalogue(fileCloser);

      CPPUNIT_ASSERT_THROW(
        catalogue.getInitialRtcpdConn(),
        castor::exception::Exception);
      CPPUNIT_ASSERT_THROW(
        catalogue.getListenSock(),
        castor::exception::Exception);
      CPPUNIT_ASSERT_EQUAL(
        false,
        catalogue.clientMigrationReportSockIsSet());
      CPPUNIT_ASSERT_EQUAL(
        false,
        catalogue.getMoreWorkConnectionIsSet());
      CPPUNIT_ASSERT_EQUAL(
        0,
        catalogue.getNbDiskTapeIOControlConns());
    }

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "No file-descriptors should have been closed",
      true,
      fileCloser.m_closedFds.empty());
  }

  void testAddListenSockWithNegativeSock() {
    TraceableDummyFileCloser fileCloser;
    const int                sock = -1;

    {
      BridgeSocketCatalogue catalogue(fileCloser);

      CPPUNIT_ASSERT_THROW(
        catalogue.addListenSock(sock),
        castor::exception::InvalidArgument);
    }

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "No file-descriptors should have been closed",
      true,
      fileCloser.m_closedFds.empty());
  }

  void testAddListenSockWithEqualToFD_SETSIZE() {
    TraceableDummyFileCloser fileCloser;
    const int                sock = FD_SETSIZE;

    {
      BridgeSocketCatalogue catalogue(fileCloser);

      CPPUNIT_ASSERT_THROW(
        catalogue.addListenSock(sock),
        castor::exception::InvalidArgument);
    }

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "No file-descriptors should have been closed",
      true,
      fileCloser.m_closedFds.empty());
  }

  void testAddListenSockWithGreaterThanFD_SETSIZE() {
    TraceableDummyFileCloser fileCloser;
    const int                sock = FD_SETSIZE + 1;

    {
      BridgeSocketCatalogue catalogue(fileCloser);

      CPPUNIT_ASSERT_THROW(
        catalogue.addListenSock(sock),
        castor::exception::InvalidArgument);
    }

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "No file-descriptors should have been closed",
      true,
      fileCloser.m_closedFds.empty());
  }

  void testAddListenSock() {
    TraceableDummyFileCloser fileCloser;
    const int                sock = 12;

    {
      BridgeSocketCatalogue catalogue(fileCloser);

      CPPUNIT_ASSERT_NO_THROW(
        catalogue.addListenSock(sock));
      CPPUNIT_ASSERT_EQUAL(
        sock,
        catalogue.getListenSock());
    }

    // It is not the responsibility of the catalogue to act as a smart-pointer
    // for the listen socket
    CPPUNIT_ASSERT_EQUAL(
      true,
      fileCloser.m_closedFds.empty());
  }

  void testAddListenSockTwice() {
    TraceableDummyFileCloser fileCloser;
    const int                sock = 12;

    {
      BridgeSocketCatalogue catalogue(fileCloser);

      CPPUNIT_ASSERT_NO_THROW_MESSAGE(
        "Add once",
        catalogue.addListenSock(sock));
      CPPUNIT_ASSERT_EQUAL(
        sock,
        catalogue.getListenSock());
      CPPUNIT_ASSERT_THROW_MESSAGE(
        "Add twice",
        catalogue.addListenSock(sock),
        castor::exception::Exception);
      CPPUNIT_ASSERT_EQUAL(
        sock,
        catalogue.getListenSock());
    }

    // It is not the responsibility of the catalogue to act as a smart-pointer
    // for the listen socket
    CPPUNIT_ASSERT_EQUAL(
      true,
      fileCloser.m_closedFds.empty());
  }

  void testAddInitialRtcpdConnWithNegativeSock() {
    TraceableDummyFileCloser fileCloser;
    const int                sock = -1;

    {
      BridgeSocketCatalogue catalogue(fileCloser);

      CPPUNIT_ASSERT_THROW(
        catalogue.addInitialRtcpdConn(sock),
        castor::exception::InvalidArgument);
    }

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "No file-descriptors should have been closed",
      true,
      fileCloser.m_closedFds.empty());
  }

  void testAddInitialRtcpdConnWithEqualToFD_SETSIZE() {
    TraceableDummyFileCloser fileCloser;
    const int                sock = FD_SETSIZE;

    {
      BridgeSocketCatalogue catalogue(fileCloser);

      CPPUNIT_ASSERT_THROW(
        catalogue.addInitialRtcpdConn(sock),
        castor::exception::InvalidArgument);
    }

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "No file-descriptors should have been closed",
      true,
      fileCloser.m_closedFds.empty());
  }

  void testAddInitialRtcpdConnWithGreaterThanFD_SETSIZE() {
    TraceableDummyFileCloser fileCloser;
    const int                sock = FD_SETSIZE + 1;

    {
      BridgeSocketCatalogue catalogue(fileCloser);

      CPPUNIT_ASSERT_THROW(
        catalogue.addInitialRtcpdConn(sock),
        castor::exception::InvalidArgument);
    }

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "No file-descriptors should have been closed",
      true,
      fileCloser.m_closedFds.empty());
  }

  void testAddInitialRtcpdConn() {
    TraceableDummyFileCloser fileCloser;
    const int                sock = 12;

    {
      BridgeSocketCatalogue catalogue(fileCloser);

      CPPUNIT_ASSERT_NO_THROW(
        catalogue.addInitialRtcpdConn(sock));
      CPPUNIT_ASSERT_EQUAL(
        sock,
        catalogue.getInitialRtcpdConn());
    }

    // The catalogue should act as a smart-pointer
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check catalogue closed file-descriptor",
      (std::vector<int>::size_type)1,
      fileCloser.m_closedFds.size());
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check value of closed file-descriptor",
      sock,
      fileCloser.m_closedFds.front());
  }

  void testAddInitialRtcpdConnTwice() {
    TraceableDummyFileCloser fileCloser;
    const int                sock = 12;

    {
      BridgeSocketCatalogue catalogue(fileCloser);

      CPPUNIT_ASSERT_NO_THROW_MESSAGE(
        "Add once",
        catalogue.addInitialRtcpdConn(sock));
      CPPUNIT_ASSERT_EQUAL(
        sock,
        catalogue.getInitialRtcpdConn());
      CPPUNIT_ASSERT_THROW_MESSAGE(
        "Add twice",
        catalogue.addInitialRtcpdConn(sock),
        castor::exception::Exception);
      CPPUNIT_ASSERT_EQUAL(
        sock,
        catalogue.getInitialRtcpdConn());
    }

    // The catalogue should act as a smart-pointer
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check catalogue closed file-descriptor",
      (std::vector<int>::size_type)1,
      fileCloser.m_closedFds.size());
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check value of closed file-descriptor",
      sock,
      fileCloser.m_closedFds.front());
  }

  void testReleaseInitialRtcpdConnWithNoSet() {
    TraceableDummyFileCloser fileCloser;

    {
      BridgeSocketCatalogue catalogue(fileCloser);

      CPPUNIT_ASSERT_THROW(
        catalogue.releaseInitialRtcpdConn(),
        castor::exception::Exception);
    }

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "No file-descriptors should have been closed",
      true,
      fileCloser.m_closedFds.empty());
  }

  void testReleaseInitialRtcpdConn() {
    TraceableDummyFileCloser fileCloser;
    const int                sock = 12;

    {
      BridgeSocketCatalogue catalogue(fileCloser);
      int                   releasedSock = 0;

      CPPUNIT_ASSERT_NO_THROW(
        catalogue.addInitialRtcpdConn(sock));
      CPPUNIT_ASSERT_EQUAL(
        sock,
        catalogue.getInitialRtcpdConn());
      CPPUNIT_ASSERT_NO_THROW(
        releasedSock = catalogue.releaseInitialRtcpdConn());
      CPPUNIT_ASSERT_EQUAL(
        sock,
        releasedSock);
    }

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "No file-descriptors should have been closed",
      true,
      fileCloser.m_closedFds.empty());
  }

  void testReleaseInitialRtcpdConnTwice() {
    TraceableDummyFileCloser fileCloser;
    const int                sock = 12;

    {
      BridgeSocketCatalogue catalogue(fileCloser);
      int                   releasedSock = 0;

      CPPUNIT_ASSERT_NO_THROW(
        catalogue.addInitialRtcpdConn(sock));
      CPPUNIT_ASSERT_EQUAL(
        sock,
        catalogue.getInitialRtcpdConn());
      CPPUNIT_ASSERT_NO_THROW(
        releasedSock = catalogue.releaseInitialRtcpdConn());
      CPPUNIT_ASSERT_EQUAL(
        sock,
        releasedSock);
      CPPUNIT_ASSERT_THROW(
        catalogue.releaseInitialRtcpdConn(),
        castor::exception::Exception);
    }

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "No file-descriptors should have been closed",
      true,
      fileCloser.m_closedFds.empty());
  }

  void testAddRtcpdDiskTapeIOControlConnWithNegativeSock() {
    TraceableDummyFileCloser fileCloser;
    const int                sock = -1;

    {
      BridgeSocketCatalogue catalogue(fileCloser);

      CPPUNIT_ASSERT_THROW(
        catalogue.addRtcpdDiskTapeIOControlConn(sock),
        castor::exception::InvalidArgument);
      CPPUNIT_ASSERT_EQUAL(
        0,
        catalogue.getNbDiskTapeIOControlConns());
    }

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "No file-descriptors should have been closed",
      true,
      fileCloser.m_closedFds.empty());
  }

  void testAddRtcpdDiskTapeIOControlConnWithEqualToFD_SETSIZE() {
    TraceableDummyFileCloser fileCloser;
    const int                sock = FD_SETSIZE;

    {
      BridgeSocketCatalogue catalogue(fileCloser);

      CPPUNIT_ASSERT_THROW(
        catalogue.addRtcpdDiskTapeIOControlConn(sock),
        castor::exception::InvalidArgument);
      CPPUNIT_ASSERT_EQUAL(
        0,
        catalogue.getNbDiskTapeIOControlConns());
    }

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "No file-descriptors should have been closed",
      true,
      fileCloser.m_closedFds.empty());
  }

  void testAddRtcpdDiskTapeIOControlConnWithGreaterThanFD_SETSIZE() {
    TraceableDummyFileCloser fileCloser;
    const int                sock = FD_SETSIZE + 1;

    {
      BridgeSocketCatalogue catalogue(fileCloser);

      CPPUNIT_ASSERT_THROW(
        catalogue.addRtcpdDiskTapeIOControlConn(sock),
        castor::exception::InvalidArgument);
      CPPUNIT_ASSERT_EQUAL(
        0,
        catalogue.getNbDiskTapeIOControlConns());
    }

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "No file-descriptors should have been closed",
      true,
      fileCloser.m_closedFds.empty());
  }

  void testAddRtcpdDiskTapeIOControlConn() {
    TraceableDummyFileCloser fileCloser;
    const int                sock = 12;

    {
      BridgeSocketCatalogue catalogue(fileCloser);

      CPPUNIT_ASSERT_NO_THROW(
        catalogue.addRtcpdDiskTapeIOControlConn(sock));
      CPPUNIT_ASSERT_EQUAL(
        1,
        catalogue.getNbDiskTapeIOControlConns());
    }

    // The catalogue should act as a smart-pointer
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check catalogue closed file-descriptor",
      (std::vector<int>::size_type)1,
      fileCloser.m_closedFds.size());
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check value of closed file-descriptor",
      sock,
      fileCloser.m_closedFds.front());
  }

  void testAddRtcpdDiskTapeIOControlConnTwice() {
    TraceableDummyFileCloser fileCloser;
    const int                sock = 12;

    {
      BridgeSocketCatalogue catalogue(fileCloser);

      CPPUNIT_ASSERT_NO_THROW_MESSAGE(
        "Add once",
        catalogue.addRtcpdDiskTapeIOControlConn(sock));
      CPPUNIT_ASSERT_EQUAL(
        1,
        catalogue.getNbDiskTapeIOControlConns());
      CPPUNIT_ASSERT_THROW_MESSAGE(
        "Add twice",
        catalogue.addRtcpdDiskTapeIOControlConn(sock),
        castor::exception::Exception);
      CPPUNIT_ASSERT_EQUAL(
        1,
        catalogue.getNbDiskTapeIOControlConns());
    }

    // The catalogue should act as a smart-pointer
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check catalogue closed file-descriptor",
      (std::vector<int>::size_type)1,
      fileCloser.m_closedFds.size());
    CPPUNIT_ASSERT_EQUAL_MESSAGE( 
      "Check value of closed file-descriptor",
      sock,
      fileCloser.m_closedFds.front());
  }

  void testAddClientMigrationReportSockWithNegativeSock() {
    TraceableDummyFileCloser fileCloser;
    const int                sock = -1;

    {
      BridgeSocketCatalogue catalogue(fileCloser);
      const uint64_t aggregatorTransactionId = 1888;

      CPPUNIT_ASSERT_THROW(
        catalogue.addClientMigrationReportSock(sock, aggregatorTransactionId),
        castor::exception::InvalidArgument);
      CPPUNIT_ASSERT_EQUAL(
        false,
        catalogue.clientMigrationReportSockIsSet());
    }

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "No file-descriptors should have been closed",
      true,
      fileCloser.m_closedFds.empty());
  }

  void testAddClientMigrationReportSockWithEqualToFD_SETSIZE() {
    TraceableDummyFileCloser fileCloser;
    const int                sock = FD_SETSIZE;

    {
      BridgeSocketCatalogue catalogue(fileCloser);
      const uint64_t aggregatorTransactionId = 1888;

      CPPUNIT_ASSERT_THROW(
        catalogue.addClientMigrationReportSock(sock, aggregatorTransactionId),
        castor::exception::InvalidArgument);
      CPPUNIT_ASSERT_EQUAL(
        false,
        catalogue.clientMigrationReportSockIsSet());
    }

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "No file-descriptors should have been closed",
      true,
      fileCloser.m_closedFds.empty());
  }

  void testAddClientMigrationReportSockWithGreaterThanFD_SETSIZE() {
    TraceableDummyFileCloser fileCloser;
    const int                sock = FD_SETSIZE + 1;
  
    {
      BridgeSocketCatalogue catalogue(fileCloser);
      const uint64_t aggregatorTransactionId = 1888;
  
      CPPUNIT_ASSERT_THROW(
        catalogue.addClientMigrationReportSock(sock, aggregatorTransactionId),
        castor::exception::InvalidArgument);
      CPPUNIT_ASSERT_EQUAL(
        false,
        catalogue.clientMigrationReportSockIsSet());
    }

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "No file-descriptors should have been closed",
      true,
      fileCloser.m_closedFds.empty());
  }

  void testAddClientMigrationReportSock() {
    TraceableDummyFileCloser fileCloser;
    const int                sock = 12;

    {
      BridgeSocketCatalogue catalogue(fileCloser);
      const uint64_t aggregatorTransactionId = 2000;

      CPPUNIT_ASSERT_NO_THROW(
        catalogue.addClientMigrationReportSock(sock,
          aggregatorTransactionId));
      CPPUNIT_ASSERT_EQUAL(
        true,
        catalogue.clientMigrationReportSockIsSet());
    }

    // The catalogue should act as a smart-pointer
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check catalogue closed file-descriptor",
      (std::vector<int>::size_type)1,
      fileCloser.m_closedFds.size());
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check value of closed file-descriptor",
      sock,
      fileCloser.m_closedFds.front());
  }

  void testAddClientMigrationReportSockTwice() {
    TraceableDummyFileCloser fileCloser;
    const int                sock = 12;

    {
      BridgeSocketCatalogue catalogue(fileCloser);
      const uint64_t aggregatorTransactionId = 2222;

      CPPUNIT_ASSERT_NO_THROW_MESSAGE(
        "Add once",
        catalogue.addClientMigrationReportSock(sock, aggregatorTransactionId));
      CPPUNIT_ASSERT_EQUAL(
        true,
        catalogue.clientMigrationReportSockIsSet());
      CPPUNIT_ASSERT_THROW_MESSAGE(
        "Add twice",
        catalogue.addClientMigrationReportSock(sock, aggregatorTransactionId),
        castor::exception::Exception);
      CPPUNIT_ASSERT_EQUAL(
        true,
        catalogue.clientMigrationReportSockIsSet());
    }

    // The catalogue should act as a smart-pointer
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check catalogue closed file-descriptor",
      (std::vector<int>::size_type)1,
      fileCloser.m_closedFds.size());
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check value of closed file-descriptor",
      sock,
      fileCloser.m_closedFds.front());
  }

  void testReleaseClientMigrationReportSockWithNoSet() {
    TraceableDummyFileCloser fileCloser;

    {
      BridgeSocketCatalogue catalogue(fileCloser);
      uint64_t aggregatorTransactionId = 0;

      CPPUNIT_ASSERT_THROW(
        catalogue.releaseClientMigrationReportSock(aggregatorTransactionId),
        castor::exception::Exception);
      CPPUNIT_ASSERT_EQUAL(
        false,
        catalogue.clientMigrationReportSockIsSet());
    }

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "No file-descriptors should have been closed",
      true,
      fileCloser.m_closedFds.empty());
  }

  void testReleaseClientMigrationReportSock() {
    TraceableDummyFileCloser fileCloser;
    const int                sock = 12;

    {
      BridgeSocketCatalogue catalogue(fileCloser);
      const uint64_t        aggregatorTransactionId = 2333;
      int                   releasedSock = 0;
      uint64_t              releasedAggregatorTransactionId = 0;


      CPPUNIT_ASSERT_NO_THROW(
        catalogue.addClientMigrationReportSock(sock, aggregatorTransactionId));
      CPPUNIT_ASSERT_EQUAL(
        true,
        catalogue.clientMigrationReportSockIsSet());
      CPPUNIT_ASSERT_NO_THROW(
        releasedSock = catalogue.releaseClientMigrationReportSock(
          releasedAggregatorTransactionId));
      CPPUNIT_ASSERT_EQUAL(
        sock,
        releasedSock);
      CPPUNIT_ASSERT_EQUAL(
        aggregatorTransactionId,
        releasedAggregatorTransactionId);
      CPPUNIT_ASSERT_EQUAL(
        false,
        catalogue.clientMigrationReportSockIsSet());
    }

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "No file-descriptors should have been closed",
      true,
      fileCloser.m_closedFds.empty());
  }

  void testReleaseClientMigrationReportSockTwice() {
    TraceableDummyFileCloser fileCloser;
    const int                sock = 12;

    {
      BridgeSocketCatalogue catalogue(fileCloser);
      const uint64_t        aggregatorTransactionId = 2555;
      int                   releasedSock = 0;
      uint64_t              releasedAggregatorTransactionId = 0;

      CPPUNIT_ASSERT_NO_THROW(
        catalogue.addClientMigrationReportSock(sock, aggregatorTransactionId));
      CPPUNIT_ASSERT_EQUAL(
        true,
        catalogue.clientMigrationReportSockIsSet());
      CPPUNIT_ASSERT_NO_THROW(
        releasedSock = catalogue.releaseClientMigrationReportSock(
          releasedAggregatorTransactionId));
      CPPUNIT_ASSERT_EQUAL(
        sock,
        releasedSock);
      CPPUNIT_ASSERT_EQUAL(
        aggregatorTransactionId,
        releasedAggregatorTransactionId);
      CPPUNIT_ASSERT_EQUAL(
        false,
        catalogue.clientMigrationReportSockIsSet());
      CPPUNIT_ASSERT_THROW(
        catalogue.releaseClientMigrationReportSock(
          releasedAggregatorTransactionId),
        castor::exception::Exception);
      CPPUNIT_ASSERT_EQUAL(
        false,
        catalogue.clientMigrationReportSockIsSet());
    }

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "No file-descriptors should have been closed",
      true,
      fileCloser.m_closedFds.empty());
  }

  void testAddGetMoreWorkSockWithNegativeRtcpdSock() {
    TraceableDummyFileCloser fileCloser;
    const int                rtcpdSock  = -1;
    const int                clientSock =  1;

    {
      BridgeSocketCatalogue catalogue(fileCloser);
      const uint32_t    rtcpdReqMagic           = 2;
      const uint32_t    rtcpdReqType            = 3;
      const std::string rtcpdReqTapePath        = "";
      const uint64_t    aggregatorTransactionId = 1888;

      CPPUNIT_ASSERT_THROW(
        catalogue.addGetMoreWorkConnection(
          rtcpdSock,
          rtcpdReqMagic,
          rtcpdReqType,
          rtcpdReqTapePath,
          clientSock,
          aggregatorTransactionId),
        castor::exception::InvalidArgument);
      CPPUNIT_ASSERT_EQUAL(
        false,
        catalogue.getMoreWorkConnectionIsSet());
    }

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "No file-descriptors should have been closed",
      true,
      fileCloser.m_closedFds.empty());
  }

  void testAddGetMoreWorkSockWithRtcpdSockEqualToFD_SETSIZE() {
    TraceableDummyFileCloser fileCloser;
    const int                rtcpdSock  = FD_SETSIZE;
    const int                clientSock = 1;

    {
      BridgeSocketCatalogue catalogue(fileCloser);
      const uint32_t    rtcpdReqMagic           = 2;
      const uint32_t    rtcpdReqType            = 3;
      const std::string rtcpdReqTapePath        = "";
      const uint64_t    aggregatorTransactionId = 1888;

      CPPUNIT_ASSERT_THROW(
        catalogue.addGetMoreWorkConnection(
          rtcpdSock,
          rtcpdReqMagic,
          rtcpdReqType,
          rtcpdReqTapePath,
          clientSock,
          aggregatorTransactionId),
        castor::exception::InvalidArgument);
      CPPUNIT_ASSERT_EQUAL(
        false,
        catalogue.getMoreWorkConnectionIsSet());
    }

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "No file-descriptors should have been closed",
      true,
      fileCloser.m_closedFds.empty());
  }

  void testAddGetMoreWorkSockWithRtcpdSockGreaterThanFD_SETSIZE() {
    TraceableDummyFileCloser fileCloser;
    const int                rtcpdSock  = FD_SETSIZE + 1;
    const int                clientSock = 1;
  
    {
      BridgeSocketCatalogue catalogue(fileCloser);
      const uint32_t    rtcpdReqMagic           = 2;
      const uint32_t    rtcpdReqType            = 3;
      const std::string rtcpdReqTapePath        = "";
      const uint64_t    aggregatorTransactionId = 1888;

      CPPUNIT_ASSERT_THROW(
        catalogue.addGetMoreWorkConnection(
          rtcpdSock, 
          rtcpdReqMagic,
          rtcpdReqType,
          rtcpdReqTapePath,
          clientSock,
          aggregatorTransactionId),
        castor::exception::InvalidArgument);
      CPPUNIT_ASSERT_EQUAL(
        false,
        catalogue.getMoreWorkConnectionIsSet());
    }

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "No file-descriptors should have been closed",
      true,
      fileCloser.m_closedFds.empty());
  }

  void testAddGetMoreWorkSockWithNegativeClientSock() {
    TraceableDummyFileCloser fileCloser;
    const int                rtcpdSock  =  1;
    const int                clientSock = -1;

    {
      BridgeSocketCatalogue catalogue(fileCloser);
      const uint32_t rtcpdReqMagic = 2;
      const uint32_t rtcpdReqType  = 3;
      const char     rtcpdReqTapePath[CA_MAXPATHLEN+1] = "";
      const uint64_t aggregatorTransactionId = 1999;

      CPPUNIT_ASSERT_NO_THROW(
        catalogue.addRtcpdDiskTapeIOControlConn(rtcpdSock));

      CPPUNIT_ASSERT_THROW(
        catalogue.addGetMoreWorkConnection(
          rtcpdSock, 
          rtcpdReqMagic,
          rtcpdReqType,
          rtcpdReqTapePath,
          clientSock,
          aggregatorTransactionId),
        castor::exception::InvalidArgument);
      CPPUNIT_ASSERT_EQUAL(
        false,
        catalogue.getMoreWorkConnectionIsSet());
    }

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Catalogue should have closed only the rtcpd file-descriptor",
      (std::vector<int>::size_type)1,
      fileCloser.m_closedFds.size());
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Catalogue should have closed only the rtcpd file-descriptor",
      rtcpdSock,
      fileCloser.m_closedFds.front());
  }

  void testAddGetMoreWorkSockWithClientSockEqualToFD_SETSIZE() {
    TraceableDummyFileCloser fileCloser;
    const int                rtcpdSock  = 1;
    const int                clientSock = FD_SETSIZE;

    {
      BridgeSocketCatalogue catalogue(fileCloser);
      const uint32_t rtcpdReqMagic = 2;
      const uint32_t rtcpdReqType  = 3;
      const char     rtcpdReqTapePath[CA_MAXPATHLEN+1] = "";
      const uint64_t aggregatorTransactionId = 1999;

      CPPUNIT_ASSERT_NO_THROW(
        catalogue.addRtcpdDiskTapeIOControlConn(rtcpdSock));

      CPPUNIT_ASSERT_THROW(
        catalogue.addGetMoreWorkConnection(
          rtcpdSock,
          rtcpdReqMagic,
          rtcpdReqType,
          rtcpdReqTapePath,
          clientSock,
          aggregatorTransactionId),
        castor::exception::InvalidArgument);
      CPPUNIT_ASSERT_EQUAL(
        false,
        catalogue.getMoreWorkConnectionIsSet());
    }

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Catalogue should have closed only the rtcpd file-descriptor",
      (std::vector<int>::size_type)1,
      fileCloser.m_closedFds.size());
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Catalogue should have closed only the rtcpd file-descriptor",
      rtcpdSock,
      fileCloser.m_closedFds.front());
  }

  void testAddGetMoreWorkSockWithClientSockGreaterThanFD_SETSIZE() {
    TraceableDummyFileCloser fileCloser;
    const int                rtcpdSock  = 1;
    const int                clientSock = FD_SETSIZE + 1;

    {
      BridgeSocketCatalogue catalogue(fileCloser);
      const uint32_t rtcpdReqMagic = 2;
      const uint32_t rtcpdReqType  = 3;
      const char     rtcpdReqTapePath[CA_MAXPATHLEN+1] = "";
      const uint64_t aggregatorTransactionId = 1999;

      CPPUNIT_ASSERT_NO_THROW(
        catalogue.addRtcpdDiskTapeIOControlConn(rtcpdSock));

      CPPUNIT_ASSERT_THROW(
        catalogue.addGetMoreWorkConnection(
          rtcpdSock,
          rtcpdReqMagic,
          rtcpdReqType,
          rtcpdReqTapePath,
          clientSock,
          aggregatorTransactionId),
        castor::exception::InvalidArgument);
      CPPUNIT_ASSERT_EQUAL(
        false,
        catalogue.getMoreWorkConnectionIsSet());
    }

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Catalogue should have closed only the rtcpd file-descriptor",
      (std::vector<int>::size_type)1,
      fileCloser.m_closedFds.size());
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Catalogue should have closed only the rtcpd file-descriptor",
      rtcpdSock,
      fileCloser.m_closedFds.front());
  }

  void testAddGetMoreWorkSockWithUnknownRtcpdSock() {
    TraceableDummyFileCloser fileCloser;
    const int                rtcpdSock  = 12;
    const int                clientSock = 13;

    {
      BridgeSocketCatalogue catalogue(fileCloser);
      const uint32_t rtcpdReqMagic = 2;
      const uint32_t rtcpdReqType  = 3;
      const char     rtcpdReqTapePath[CA_MAXPATHLEN+1] = "";
      const uint64_t aggregatorTransactionId = 1999;

      CPPUNIT_ASSERT_THROW(
        catalogue.addGetMoreWorkConnection(
          rtcpdSock, 
          rtcpdReqMagic,
          rtcpdReqType,
          rtcpdReqTapePath,
          clientSock,
          aggregatorTransactionId),
        castor::exception::Exception);
      CPPUNIT_ASSERT_EQUAL(
        false,
        catalogue.getMoreWorkConnectionIsSet());
    }

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "No file-descriptors should have been closed",
      true,
      fileCloser.m_closedFds.empty());
  }

  void testAddGetMoreWorkSock() {
    TraceableDummyFileCloser fileCloser;
    const int                rtcpdSock  = 12;
    const int                clientSock = 13;

    {
      BridgeSocketCatalogue catalogue(fileCloser);
      const uint32_t rtcpdReqMagic = 2;
      const uint32_t rtcpdReqType  = 3;
      const char     rtcpdReqTapePath[CA_MAXPATHLEN+1] = "";
      const uint64_t aggregatorTransactionId = 2222;

      CPPUNIT_ASSERT_NO_THROW(
        catalogue.addRtcpdDiskTapeIOControlConn(rtcpdSock));

      CPPUNIT_ASSERT_NO_THROW(
        catalogue.addGetMoreWorkConnection(
          rtcpdSock,
          rtcpdReqMagic,
          rtcpdReqType,
          rtcpdReqTapePath,
          clientSock,
          aggregatorTransactionId));

      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "Check getMoreWorkConnectionIsSet() returns true",
        true,
        catalogue.getMoreWorkConnectionIsSet());

      GetMoreWorkConnection getMoreWorkConnection;
      CPPUNIT_ASSERT_NO_THROW_MESSAGE(
        "Get the get more work connection",
        getMoreWorkConnection = catalogue.getGetMoreWorkConnection());

      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "Check rtcpdSock of get more work connection",
        rtcpdSock,
        getMoreWorkConnection.rtcpdSock);

      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "Check childSock of get more work connection",
        clientSock,
        getMoreWorkConnection.clientSock);
    }

    // The catalogue should act as a smart-pointer
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check catalogue closed both file-descriptors",
      (std::vector<int>::size_type)2,
      fileCloser.m_closedFds.size());
    CPPUNIT_ASSERT_MESSAGE(
      "Check the values of both closed file-descriptors",
      ((rtcpdSock  == fileCloser.m_closedFds[0] &&
        clientSock == fileCloser.m_closedFds[1])  ||
       (clientSock == fileCloser.m_closedFds[0] &&
        rtcpdSock  == fileCloser.m_closedFds[1])));
  }

  void testAddGetMoreWorkSockTwice() {
    TraceableDummyFileCloser fileCloser;
    const int                rtcpdSock  = 12;
    const int                clientSock = 13;

    {
      BridgeSocketCatalogue catalogue(fileCloser);
      const uint32_t rtcpdReqMagic = 2;
      const uint32_t rtcpdReqType  = 3;
      const char     rtcpdReqTapePath[CA_MAXPATHLEN+1] = "";
      const uint64_t aggregatorTransactionId = 2555;

      CPPUNIT_ASSERT_NO_THROW(
        catalogue.addRtcpdDiskTapeIOControlConn(rtcpdSock));

      CPPUNIT_ASSERT_NO_THROW_MESSAGE(
        "Add once",
        catalogue.addGetMoreWorkConnection(
          rtcpdSock,
          rtcpdReqMagic,
          rtcpdReqType,
          rtcpdReqTapePath,
          clientSock,
          aggregatorTransactionId));

      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "Check getMoreWorkConnectionIsSet() returns true",
        true,
        catalogue.getMoreWorkConnectionIsSet());

      GetMoreWorkConnection getMoreWorkConnection;
      CPPUNIT_ASSERT_NO_THROW_MESSAGE(
        "Get the get more work connection",
        getMoreWorkConnection = catalogue.getGetMoreWorkConnection());

      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "Check rtcpdSock of get more work connection",
        rtcpdSock,
        getMoreWorkConnection.rtcpdSock);

      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "Check childSock of get more work connection",
        clientSock,
        getMoreWorkConnection.clientSock);

      CPPUNIT_ASSERT_THROW_MESSAGE(
        "Add twice",
        catalogue.addGetMoreWorkConnection(
          rtcpdSock,
          rtcpdReqMagic,
          rtcpdReqType,
          rtcpdReqTapePath,
          clientSock,
          aggregatorTransactionId),
        castor::exception::Exception);
      CPPUNIT_ASSERT_EQUAL(
        true,
        catalogue.getMoreWorkConnectionIsSet());
    }

    // The catalogue should act as a smart-pointer
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Check catalogue closed both file-descriptors",
      (std::vector<int>::size_type)2,
      fileCloser.m_closedFds.size());
    CPPUNIT_ASSERT_MESSAGE(
      "Check the values of both closed file-descriptors",
      ((rtcpdSock  == fileCloser.m_closedFds[0] &&
        clientSock == fileCloser.m_closedFds[1])  ||
       (clientSock == fileCloser.m_closedFds[0] &&
        rtcpdSock  == fileCloser.m_closedFds[1])));
  }

  void testReleaseClientGetMoreWorkSockWithNoSet() {
    TraceableDummyFileCloser fileCloser;

    {
      BridgeSocketCatalogue catalogue(fileCloser);

      CPPUNIT_ASSERT_THROW(
        catalogue.releaseGetMoreWorkClientSock(),
        castor::exception::Exception);
      CPPUNIT_ASSERT_EQUAL(
        false,
        catalogue.getMoreWorkConnectionIsSet());
    }

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "No file-descriptors should have been closed",
      true,
      fileCloser.m_closedFds.empty());
  }

  void testReleaseClientGetMoreWorkSock() {
    TraceableDummyFileCloser fileCloser;
    const int                rtcpdSock  = 12;
    const int                clientSock = 13;

    {
      BridgeSocketCatalogue catalogue(fileCloser);
      const uint32_t rtcpdReqMagic = 2;
      const uint32_t rtcpdReqType  = 3;
      const char     rtcpdReqTapePath[CA_MAXPATHLEN+1] = "";
      const uint64_t aggregatorTransactionId = 2888;
      int            releasedSock = 0;

      CPPUNIT_ASSERT_NO_THROW(
        catalogue.addRtcpdDiskTapeIOControlConn(rtcpdSock));

      CPPUNIT_ASSERT_NO_THROW(
        catalogue.addGetMoreWorkConnection(
          rtcpdSock,
          rtcpdReqMagic,
          rtcpdReqType,
          rtcpdReqTapePath,
          clientSock,
          aggregatorTransactionId));
      CPPUNIT_ASSERT_EQUAL(
        true,
        catalogue.getMoreWorkConnectionIsSet());
      CPPUNIT_ASSERT_NO_THROW(
        releasedSock = catalogue.releaseGetMoreWorkClientSock());
      CPPUNIT_ASSERT_EQUAL(
        clientSock,
        releasedSock);
      CPPUNIT_ASSERT_EQUAL(
        false,
        catalogue.getMoreWorkConnectionIsSet());
    }

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Catalogue should have closed only the rtcpd file-descriptor",
      (std::vector<int>::size_type)1,
      fileCloser.m_closedFds.size());
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Catalogue should have closed only the rtcpd file-descriptor",
      rtcpdSock,
      fileCloser.m_closedFds.front());
  }

  void testReleaseClientGetMoreWorkSockTwice() {
    TraceableDummyFileCloser fileCloser;
    const int                rtcpdSock  = 12;
    const int                clientSock = 13;

    {
      BridgeSocketCatalogue catalogue(fileCloser);
      const uint32_t rtcpdReqMagic = 2;
      const uint32_t rtcpdReqType  = 3;
      const char     rtcpdReqTapePath[CA_MAXPATHLEN+1] = "";
      const uint64_t aggregatorTransactionId = 3111;
      int            releasedSock = -1;

      CPPUNIT_ASSERT_NO_THROW(
        catalogue.addRtcpdDiskTapeIOControlConn(rtcpdSock));

      CPPUNIT_ASSERT_NO_THROW(
        catalogue.addGetMoreWorkConnection(
          rtcpdSock,
          rtcpdReqMagic,
          rtcpdReqType,
          rtcpdReqTapePath,
          clientSock,
          aggregatorTransactionId));
      CPPUNIT_ASSERT_EQUAL(
        true,
        catalogue.getMoreWorkConnectionIsSet());
      CPPUNIT_ASSERT_NO_THROW(
        releasedSock = catalogue.releaseGetMoreWorkClientSock());
      CPPUNIT_ASSERT_EQUAL(
        clientSock,
        releasedSock);
      CPPUNIT_ASSERT_EQUAL(
        false,
        catalogue.getMoreWorkConnectionIsSet());
      CPPUNIT_ASSERT_THROW(
        catalogue.releaseGetMoreWorkClientSock(),
        castor::exception::Exception);
      CPPUNIT_ASSERT_EQUAL(
        false,
        catalogue.getMoreWorkConnectionIsSet());
    }

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Catalogue should have closed only the rtcpd file-descriptor",
      (std::vector<int>::size_type)1,
      fileCloser.m_closedFds.size());
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Catalogue should have closed only the rtcpd file-descriptor",
      rtcpdSock,
      fileCloser.m_closedFds.front());
  }

  void testGetGetMoreWorkConnectionWithoutSet() {
    TraceableDummyFileCloser fileCloser;

    {
      BridgeSocketCatalogue catalogue(fileCloser);

      CPPUNIT_ASSERT_THROW(
        catalogue.getGetMoreWorkConnection(),
        castor::exception::NoValue);
    }

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "No file-descriptors should have been closed",
      true,
      fileCloser.m_closedFds.empty());
  }

  void testIfValidInsertFdIntoSetWithInvalidFd() {
    TraceableDummyFileCloser fileCloser;

    {
      const int fd           = -1;
      fd_set    fdSet;
      const int initialMaxFd = 12;
      int       maxFd        = initialMaxFd;

      FD_ZERO(&fdSet);

      TestingBridgeSocketCatalogue catalogue(fileCloser);

      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "Checking ifValidInsertFdIntoSet did not insert invalid fd",
        false,
        catalogue.ifValidInsertFdIntoSet(fd, fdSet, maxFd));

      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "Checking maxFd not changed after calling ifValidInsertFdIntoSet",
        initialMaxFd,
        maxFd);
    }

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "No file-descriptors should have been closed",
      true,
      fileCloser.m_closedFds.empty());

  }

  void testIfValidInsertFdIntoSetWithFdLessThanMax() {
    TraceableDummyFileCloser fileCloser;

    {
      const int fd           = 12;
      fd_set    fdSet;
      const int initialMaxFd = 13;
      int       maxFd        = initialMaxFd;

      FD_ZERO(&fdSet);

      TestingBridgeSocketCatalogue catalogue(fileCloser);

      CPPUNIT_ASSERT_MESSAGE(
        "Checking ifValidInsertFdIntoSet inserted valid fd",
        catalogue.ifValidInsertFdIntoSet(fd, fdSet, maxFd));

      CPPUNIT_ASSERT_MESSAGE(
        "Checking set contains fd after calling ifValidInsertFdIntoSet",
        FD_ISSET(fd, &fdSet));

      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "Checking maxFd not changed after calling ifValidInsertFdIntoSet",
        initialMaxFd,
        maxFd);
    }

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "No file-descriptors should have been closed",
      true,
      fileCloser.m_closedFds.empty());
  }

  void testIfValidInsertFdIntoSetWithFdGreaterThanMax() {
    TraceableDummyFileCloser fileCloser;

    {
      const int fd           = 12;
      fd_set    fdSet;
      const int initialMaxFd = 11;
      int       maxFd        = initialMaxFd;
      
      FD_ZERO(&fdSet);

      TestingBridgeSocketCatalogue catalogue(fileCloser);

      CPPUNIT_ASSERT_MESSAGE(
        "Checking ifValidInsertFdIntoSet inserted valid fd",
        catalogue.ifValidInsertFdIntoSet(fd, fdSet, maxFd));

      CPPUNIT_ASSERT_MESSAGE(
        "Checking set contains fd after calling ifValidInsertFdIntoSet",
        FD_ISSET(fd, &fdSet));

      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "Checking maxFd is equal to fd after calling ifValidInsertFdIntoSet",
        fd,
        maxFd);
    }

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "No file-descriptors should have been closed",
      true,
      fileCloser.m_closedFds.empty());
  }

  void testBuildReadFdSetWithNoFds() {
    TraceableDummyFileCloser fileCloser;

    {
      BridgeSocketCatalogue catalogue(fileCloser);
      fd_set readFdSet;
      int    maxFd = -1;
      FD_ZERO(&readFdSet);

      CPPUNIT_ASSERT_THROW(
        catalogue.buildReadFdSet(readFdSet, maxFd),
        castor::exception::NoValue);
    }

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "No file-descriptors should have been closed",
      true,
      fileCloser.m_closedFds.empty());
  }

  void testGetAPendingSockLISTEN() {
    TraceableDummyFileCloser fileCloser;
    const int                listenSock = 12;

    {
      BridgeSocketCatalogue catalogue(fileCloser);

      CPPUNIT_ASSERT_NO_THROW_MESSAGE(
        "Add listen socket to catalogue",
        catalogue.addListenSock(listenSock));

      // Create a set of file-descriptors where the listen socket is pending
      fd_set readFdSet;
      FD_ZERO(&readFdSet);
      FD_SET(listenSock, &readFdSet);

      BridgeSocketCatalogue::SocketType sockType =
        BridgeSocketCatalogue::UNKNOWN;
      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "Check getAPendingSock returns the listen socket",
        listenSock,
        catalogue.getAPendingSock(readFdSet, sockType));

      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "Check getAPendingSock correctly identifies the type of the listen"
        " socket",
        BridgeSocketCatalogue::LISTEN,
        sockType);
    }

    // The catalogue should NOT act as a smart-pointer for the listen socket
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "No file-descriptors should have been closed",
      true,
      fileCloser.m_closedFds.empty());
  }

  void testGetAPendingSockINITIAL_RTCPD() {
    TraceableDummyFileCloser fileCloser;
    const int                initialRtcpdSock = 12;

    {
      BridgeSocketCatalogue catalogue(fileCloser);

      CPPUNIT_ASSERT_NO_THROW_MESSAGE(
        "Add initial rtcpd socket to catalogue",
        catalogue.addInitialRtcpdConn(initialRtcpdSock));

      // Create a set of file-descriptors where the initial rtcpd socket is
      // pending
      fd_set readFdSet;
      FD_ZERO(&readFdSet);
      FD_SET(initialRtcpdSock, &readFdSet);

      BridgeSocketCatalogue::SocketType sockType =
        BridgeSocketCatalogue::UNKNOWN;
      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "Check getAPendingSock returns the initial rtcpd socket",
        initialRtcpdSock,
        catalogue.getAPendingSock(readFdSet, sockType));

      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "Check getAPendingSock correctly identifies the type of the intial"
        " rtcpd socket",
        BridgeSocketCatalogue::INITIAL_RTCPD,
        sockType);
    }

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Catalogue should have closed only the initial rtcpd socket",
      (std::vector<int>::size_type)1,
      fileCloser.m_closedFds.size());
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Catalogue should have closed only the initial rtcpd socket",
      initialRtcpdSock,
      fileCloser.m_closedFds.front());
  }

  void testGetAPendingSockRTCPD_DISK_TAPE_IO_CONTROL() {
    TraceableDummyFileCloser fileCloser;
    const int                ioControlSock = 12;

    {
      BridgeSocketCatalogue catalogue(fileCloser);

      CPPUNIT_ASSERT_NO_THROW_MESSAGE(
        "Add IO control socket to catalogue",
        catalogue.addRtcpdDiskTapeIOControlConn(ioControlSock));

      // Create a set of file-descriptors where the IO control socket is
      // pending
      fd_set readFdSet;
      FD_ZERO(&readFdSet);
      FD_SET(ioControlSock, &readFdSet);

      BridgeSocketCatalogue::SocketType sockType =
        BridgeSocketCatalogue::UNKNOWN;
      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "Check getAPendingSock returns the IO control socket",
        ioControlSock,
        catalogue.getAPendingSock(readFdSet, sockType));

      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "Check getAPendingSock correctly identifies the type of the IO control"
        " socket",
        BridgeSocketCatalogue::RTCPD_DISK_TAPE_IO_CONTROL,
        sockType);
    }

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Catalogue should have closed only the IO control socket",
      (std::vector<int>::size_type)1,
      fileCloser.m_closedFds.size());
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Catalogue should have closed only the IO control socket",
      ioControlSock,
      fileCloser.m_closedFds.front());
  }

  void testGetAPendingSockCLIENT_GET_MORE_WORK() {
    TraceableDummyFileCloser fileCloser;
    const int                rtcpdSock               = 12;
    const uint32_t           rtcpdReqMagic           = 13;
    const uint32_t           rtcpdReqType            = 14;
    const std::string        rtcpdReqTapePath        = "rtcpdReqTapePath";
    const int                clientSock              = 15;
    const uint64_t           aggregatorTransactionId = 16;

    {
      BridgeSocketCatalogue catalogue(fileCloser);
      CPPUNIT_ASSERT_NO_THROW_MESSAGE(
        "Add rtcpd sock to catalogue ready for adding get more work socket",
        catalogue.addRtcpdDiskTapeIOControlConn(rtcpdSock));

      CPPUNIT_ASSERT_NO_THROW_MESSAGE(
        "Add get more work socket to catalogue",
        catalogue.addGetMoreWorkConnection(
          rtcpdSock,
          rtcpdReqMagic,
          rtcpdReqType,
          rtcpdReqTapePath,
          clientSock,
          aggregatorTransactionId));

      // Create a set of file-descriptors where the get more work socket is
      // pending
      fd_set readFdSet;
      FD_ZERO(&readFdSet);
      FD_SET(clientSock, &readFdSet);

      BridgeSocketCatalogue::SocketType sockType =
        BridgeSocketCatalogue::UNKNOWN;
      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "Check getAPendingSock returns the get more work socket",
        clientSock,
        catalogue.getAPendingSock(readFdSet, sockType));

      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "Check getAPendingSock correctly identifies the type of the get more"
        " work socket",
        BridgeSocketCatalogue::CLIENT_GET_MORE_WORK,
        sockType);
    }

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Catalogue should have closed both the rtcpd and client sockets",
      (std::vector<int>::size_type)2,
      fileCloser.m_closedFds.size());
    CPPUNIT_ASSERT_MESSAGE(
      "Catalogue should have closed both the rtcpd and client sockets",
      (rtcpdSock == fileCloser.m_closedFds[0] &&
       clientSock == fileCloser.m_closedFds[1]) ||
      (rtcpdSock == fileCloser.m_closedFds[1] &&
       clientSock == fileCloser.m_closedFds[0]));
  }

  void testGetAPendingSockCLIENT_MIGRATION_REPORT() {
    TraceableDummyFileCloser fileCloser;
    const int                migrationReportSock     = 12;
    const uint64_t           aggregatorTransactionId = 13;

    {
      BridgeSocketCatalogue catalogue(fileCloser);

      CPPUNIT_ASSERT_NO_THROW_MESSAGE(
        "Add migration report socket to catalogue",
        catalogue.addClientMigrationReportSock(
          migrationReportSock,
          aggregatorTransactionId));

      // Create a set of file-descriptors where the IO control socket is
      // pending
      fd_set readFdSet;
      FD_ZERO(&readFdSet);
      FD_SET(migrationReportSock, &readFdSet);

      BridgeSocketCatalogue::SocketType sockType =
        BridgeSocketCatalogue::UNKNOWN;
      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "Check getAPendingSock returns the migration report socket",
        migrationReportSock,
        catalogue.getAPendingSock(readFdSet, sockType));

      CPPUNIT_ASSERT_EQUAL_MESSAGE(
        "Check getAPendingSock correctly identifies the type of the migration"
        " report socket",
        BridgeSocketCatalogue::CLIENT_MIGRATION_REPORT,
        sockType);
    }

    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Catalogue should have closed only the migration report socket",
      (std::vector<int>::size_type)1,
      fileCloser.m_closedFds.size());
    CPPUNIT_ASSERT_EQUAL_MESSAGE(
      "Catalogue should have closed only the migration report socket",
      migrationReportSock,
      fileCloser.m_closedFds.front());
  }

  CPPUNIT_TEST_SUITE(BridgeSocketCatalogueTest);

  CPPUNIT_TEST(testConstructor);

  CPPUNIT_TEST(testAddListenSockWithNegativeSock);
  CPPUNIT_TEST(testAddListenSockWithEqualToFD_SETSIZE);
  CPPUNIT_TEST(testAddListenSockWithGreaterThanFD_SETSIZE);
  CPPUNIT_TEST(testAddListenSock);
  CPPUNIT_TEST(testAddListenSockTwice);

  CPPUNIT_TEST(testAddInitialRtcpdConnWithNegativeSock);
  CPPUNIT_TEST(testAddInitialRtcpdConnWithEqualToFD_SETSIZE);
  CPPUNIT_TEST(testAddInitialRtcpdConnWithGreaterThanFD_SETSIZE);
  CPPUNIT_TEST(testAddInitialRtcpdConn);
  CPPUNIT_TEST(testAddInitialRtcpdConnTwice);

  CPPUNIT_TEST(testReleaseInitialRtcpdConnWithNoSet);
  CPPUNIT_TEST(testReleaseInitialRtcpdConn);
  CPPUNIT_TEST(testReleaseInitialRtcpdConnTwice);

  CPPUNIT_TEST(testAddRtcpdDiskTapeIOControlConnWithNegativeSock);
  CPPUNIT_TEST(testAddRtcpdDiskTapeIOControlConnWithEqualToFD_SETSIZE);
  CPPUNIT_TEST(testAddRtcpdDiskTapeIOControlConnWithGreaterThanFD_SETSIZE);
  CPPUNIT_TEST(testAddRtcpdDiskTapeIOControlConn);
  CPPUNIT_TEST(testAddRtcpdDiskTapeIOControlConnTwice);

  CPPUNIT_TEST(testAddClientMigrationReportSockWithNegativeSock);
  CPPUNIT_TEST(testAddClientMigrationReportSockWithEqualToFD_SETSIZE);
  CPPUNIT_TEST(testAddClientMigrationReportSockWithGreaterThanFD_SETSIZE);
  CPPUNIT_TEST(testAddClientMigrationReportSock);
  CPPUNIT_TEST(testAddClientMigrationReportSockTwice);

  CPPUNIT_TEST(testReleaseClientMigrationReportSockWithNoSet);
  CPPUNIT_TEST(testReleaseClientMigrationReportSock);
  CPPUNIT_TEST(testReleaseClientMigrationReportSockTwice);

  CPPUNIT_TEST(testAddGetMoreWorkSockWithNegativeRtcpdSock);
  CPPUNIT_TEST(testAddGetMoreWorkSockWithRtcpdSockEqualToFD_SETSIZE);
  CPPUNIT_TEST(testAddGetMoreWorkSockWithRtcpdSockGreaterThanFD_SETSIZE);
  CPPUNIT_TEST(testAddGetMoreWorkSockWithNegativeClientSock);
  CPPUNIT_TEST(testAddGetMoreWorkSockWithClientSockEqualToFD_SETSIZE);
  CPPUNIT_TEST(testAddGetMoreWorkSockWithClientSockGreaterThanFD_SETSIZE);
  CPPUNIT_TEST(testAddGetMoreWorkSockWithUnknownRtcpdSock);
  CPPUNIT_TEST(testAddGetMoreWorkSock);
  CPPUNIT_TEST(testAddGetMoreWorkSockTwice);

  CPPUNIT_TEST(testReleaseClientGetMoreWorkSockWithNoSet);
  CPPUNIT_TEST(testReleaseClientGetMoreWorkSock);
  CPPUNIT_TEST(testReleaseClientGetMoreWorkSockTwice);

  CPPUNIT_TEST(testGetGetMoreWorkConnectionWithoutSet);

  CPPUNIT_TEST(testIfValidInsertFdIntoSetWithInvalidFd);
  CPPUNIT_TEST(testIfValidInsertFdIntoSetWithFdLessThanMax);
  CPPUNIT_TEST(testIfValidInsertFdIntoSetWithFdGreaterThanMax);

  CPPUNIT_TEST(testBuildReadFdSetWithNoFds);

  CPPUNIT_TEST(testGetAPendingSockLISTEN);
  CPPUNIT_TEST(testGetAPendingSockINITIAL_RTCPD);
  CPPUNIT_TEST(testGetAPendingSockRTCPD_DISK_TAPE_IO_CONTROL);
  CPPUNIT_TEST(testGetAPendingSockCLIENT_GET_MORE_WORK);
  CPPUNIT_TEST(testGetAPendingSockCLIENT_MIGRATION_REPORT);

  CPPUNIT_TEST_SUITE_END();

}; // class BridgeSocketCatalogueTest

CPPUNIT_TEST_SUITE_REGISTRATION(BridgeSocketCatalogueTest);

} // namespace tapebridge
} // namespace tape
} // namespace castor

#endif // TEST_UNITTEST_CASTOR_TAPE_TAPEBRIDGE_BRIDGESOCKETCATALOGUETEST_HPP
