/******************************************************************************
 *                test/castor/tape/mighunter/MigrationPolicyTest.hpp
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

#ifndef TEST_CASTOR_TAPE_MIGHUNTER_MIGRATIONPOLICYTEST_HPP
#define TEST_CASTOR_TAPE_MIGHUNTER_MIGRATIONPOLICYTEST_HPP 1

// Include Python.h before any standard headers because Python.h may define
// some pre-processor definitions which affect the standard headers
#include "castor/tape/python/python.hpp"

#include "castor/tape/mighunter/MigrationPolicyElement.hpp"
#include "castor/tape/python/ScopedPythonLock.hpp"
#include "castor/tape/python/SmartPyObjectPtr.hpp"
#include "castor/tape/utils/utils.hpp"

#include <cppunit/extensions/HelperMacros.h>

#include <stdlib.h>
#include <string>

class test_exception: public std::exception {
private:
  const std::string m_msg;

  test_exception &operator=(const test_exception &tx) {
  }

public:
  test_exception(std::string msg): m_msg(msg) {
  }

  test_exception(const test_exception &tx) : m_msg(tx.m_msg) {
  }

  ~test_exception() throw() {
  }

  const char* what() const throw() {
    return m_msg.c_str();
  }
};

PyObject* importPythonModuleWithLock_stdException(
  const char *const moduleName) {
  try {
    return castor::tape::python::importPythonModuleWithLock(moduleName);
  } catch(castor::exception::Exception &ex) {
    test_exception te(ex.getMessage().str());

    throw te;
  }
}

void readFileIntoList_stdException(const char *const filename,
  std::list<std::string> &lines) {
  try {
    castor::tape::utils::readFileIntoList(filename, lines);
  } catch(castor::exception::Exception &ex) {
    test_exception te(ex.getMessage().str());

    throw te;
  }
}

void testMigrationPolicy_castorException() {
  const char *const pythonModuleName   = "migrationPolicyTestModule";
  const char *const pythonFunctionName = "migrationTestPolicyFunction";

  PyObject *pythonDict = NULL;
  CPPUNIT_ASSERT_NO_THROW_MESSAGE("import test module",
    pythonDict = importPythonModuleWithLock_stdException(pythonModuleName));

  CPPUNIT_ASSERT_MESSAGE("pythonDict not NULL",
    pythonDict != NULL);

  PyObject *migrationPolicyFunc = NULL;
  CPPUNIT_ASSERT_NO_THROW_MESSAGE("get python function",
    migrationPolicyFunc = castor::tape::python::getPythonFunctionWithLock(
      pythonDict, pythonFunctionName));

  {
    // Get a lock on the embedded Python-interpreter
    castor::tape::python::ScopedPythonLock scopedPythonLock;

    castor::tape::mighunter::MigrationPolicyElement elem;
    elem.tapePoolName   = "tapePoolName";
    elem.castorFileName = "castorFileName";
    elem.copyNb         = 0x0102030405060708;
    elem.fileId         = 0x1112131415161718;
    elem.fileSize       = 0x2122232425262728;
    elem.fileMode       = 0x31323334;
    elem.uid            = 0x4142434445464748;
    elem.gid            = 0x5152535455565758;
    elem.aTime          = 0x6162636465666768;
    elem.mTime          = 0x7172737475767778;
    elem.cTime          = 0x8182838485868788;
    elem.fileClass      = 0x31323334;

    // Create the input tuple for the migration-policy Python-function
    //
    // python-Bugs-1308740  Py_BuildValue (C/API): "K" format
    // K must be used for unsigned (feature not documented at all but
    // available)
    castor::tape::python::SmartPyObjectPtr inputObj;
    CPPUNIT_ASSERT_NO_THROW_MESSAGE("Py_BuildValue",
      inputObj.reset(Py_BuildValue(
      (char*)"(s,s,K,K,K,i,K,K,K,K,K,i)",
      (elem.tapePoolName).c_str(),
      (elem.castorFileName).c_str(),
      elem.copyNb,
      elem.fileId,
      elem.fileSize,
      elem.fileMode,
      elem.uid,
      elem.gid,
      elem.aTime,
      elem.mTime,
      elem.cTime,
      elem.fileClass)));

    castor::tape::python::SmartPyObjectPtr resultObj(PyObject_CallObject(
      migrationPolicyFunc, inputObj.get()));

    CPPUNIT_ASSERT_MESSAGE("result not NULL", resultObj.get() != NULL);

    const int expectedResult = 1;
    const int actualResult = PyInt_AsLong(resultObj.get());
    if(resultObj.get() != NULL) {
      CPPUNIT_ASSERT_EQUAL_MESSAGE("result", expectedResult, actualResult);
    }
  }
} // testMigrationPolicy_castorException()

void testMigrationPolicy_stdException() {
  try {
    testMigrationPolicy_castorException();
  } catch(castor::exception::Exception &ex) {
    test_exception te(ex.getMessage().str());

    throw te;
  }
}

class MigrationPolicyTest: public CppUnit::TestFixture {
public:

  void setUp() {
  }

  void tearDown() {
  }

  void testMigrationPolicy() {
    const char *const policyOutputFilename =
      "migrationTestPolicyFunctionOutput.txt";
    std::list<std::string> policyOutputLines;

    CPPUNIT_ASSERT_NO_THROW_MESSAGE("testMigrationPolicy_stdException",
      testMigrationPolicy_stdException());

    CPPUNIT_ASSERT_NO_THROW_MESSAGE("readFileIntoList_stdException",
      readFileIntoList_stdException(policyOutputFilename, policyOutputLines));

    const size_t expectedNbOutputLines = 12;
    CPPUNIT_ASSERT_EQUAL_MESSAGE("number of output lines",
      expectedNbOutputLines, policyOutputLines.size());

    std::list<std::string>::iterator itor = policyOutputLines.begin();
    CPPUNIT_ASSERT_EQUAL_MESSAGE("tapePoolName"  ,
      std::string("tapepool       = \"tapePoolName\""   ), *(itor++));
    CPPUNIT_ASSERT_EQUAL_MESSAGE("castorFileName",
      std::string("castorfilename = \"castorFileName\"" ), *(itor++));
    CPPUNIT_ASSERT_EQUAL_MESSAGE("copyNb"        ,
      std::string("copynb         = 0x102030405060708L" ), *(itor++));
    CPPUNIT_ASSERT_EQUAL_MESSAGE("fileId"        ,
      std::string("fileId         = 0x1112131415161718L"), *(itor++));
    CPPUNIT_ASSERT_EQUAL_MESSAGE("fileSize"      ,
      std::string("fileSize       = 0x2122232425262728L"), *(itor++));
    CPPUNIT_ASSERT_EQUAL_MESSAGE("fileMode"      ,
      std::string("fileMode       = 0x31323334"         ), *(itor++));
    CPPUNIT_ASSERT_EQUAL_MESSAGE("uid"           ,
      std::string("uid            = 0x4142434445464748L"), *(itor++));
    CPPUNIT_ASSERT_EQUAL_MESSAGE("gid"           ,
      std::string("gid            = 0x5152535455565758L"), *(itor++));
    CPPUNIT_ASSERT_EQUAL_MESSAGE("aTime"         ,
      std::string("aTime          = 0x6162636465666768L"), *(itor++));
    CPPUNIT_ASSERT_EQUAL_MESSAGE("mTime"         ,
      std::string("mTime          = 0x7172737475767778L"), *(itor++));
    CPPUNIT_ASSERT_EQUAL_MESSAGE("cTime"         ,
      std::string("cTime          = 0x8182838485868788L"), *(itor++));
    CPPUNIT_ASSERT_EQUAL_MESSAGE("fileClass"     ,
      std::string("fileClass      = 0x31323334"         ), *(itor++));
    CPPUNIT_ASSERT_MESSAGE("No more lines" , itor == policyOutputLines.end());
  } // testMigrationPolicy()

  CPPUNIT_TEST_SUITE(MigrationPolicyTest);
  CPPUNIT_TEST(testMigrationPolicy);
  CPPUNIT_TEST_SUITE_END();
};

#endif // TEST_CASTOR_TAPE_MIGHUNTER_MIGRATIONPOLICYTEST_HPP
