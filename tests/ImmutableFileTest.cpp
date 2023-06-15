/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#include "common/exception/CommandLineNotParsed.hpp"
#include "common/exception/Exception.hpp"
#include "tests/ImmutableFileTest.hpp"
#include "tests/ImmutableFileTestCmdLineArgs.hpp"

#include <iostream>
#include <list>
#include <sstream>
#include <utility>
#include <XrdCl/XrdClFile.hh>
#include <XrdVersion.hh>

namespace cta {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
ImmutableFileTest::ImmutableFileTest(std::istream& inStream, std::ostream& outStream, std::ostream& errStream) :
m_in(inStream),
m_out(outStream),
m_err(errStream) {}

//------------------------------------------------------------------------------
// main
//------------------------------------------------------------------------------
int ImmutableFileTest::main(const int argc, char* const* const argv) {
  bool cmdLineNotParsed = false;
  std::string errorMessage;

  try {
    return exceptionThrowingMain(argc, argv);
  }
  catch (exception::CommandLineNotParsed& ue) {
    errorMessage = ue.getMessage().str();
    cmdLineNotParsed = true;
  }
  catch (exception::Exception& ex) {
    errorMessage = ex.getMessage().str();
  }
  catch (std::exception& se) {
    errorMessage = se.what();
  }
  catch (...) {
    errorMessage = "An unknown exception was thrown";
  }

  // Reaching this point means the command has failed, an exception was throw
  // and errorMessage has been set accordingly

  m_out << errorMessage << std::endl;
  if (cmdLineNotParsed) {
    m_out << std::endl;
    ImmutableFileTestCmdLineArgs::printUsage(m_out);
  }
  return 1;
}

//------------------------------------------------------------------------------
// exceptionThrowingMain
//------------------------------------------------------------------------------
int ImmutableFileTest::exceptionThrowingMain(const int argc, char* const* const argv) {
  const ImmutableFileTestCmdLineArgs cmdLine(argc, argv);

  if (cmdLine.help) {
    ImmutableFileTestCmdLineArgs::printUsage(m_out);
    return 0;
  }

  const bool runTests = userConfirmsDestroyFile(cmdLine.fileUrl.GetURL());

  m_out << std::endl;
  if (runTests) {
    m_out << "Starting destructive test on " << cmdLine.fileUrl.GetURL() << std::endl;
  }
  else {
    m_out << "Aborting" << std::endl;
    return 0;
  }

  if (cmdLine.fileUrl.IsLocalFile()) {
    exception::Exception ex;
    ex.getMessage() << cmdLine.fileUrl.GetURL() << " is local";
    throw ex;
  }

  m_out << std::endl;
  if (fileExists(cmdLine.fileUrl)) {
    m_out << cmdLine.fileUrl.GetURL() << " already exists" << std::endl;
    m_out << "About to delete " << cmdLine.fileUrl.GetURL() << std::endl;
    deleteFile(cmdLine.fileUrl);
    m_out << "Deleted " << cmdLine.fileUrl.GetURL() << std::endl;
  }
  else {
    m_out << cmdLine.fileUrl.GetURL() << " does not exist yet" << std::endl;
  }

  bool oneOrMoreTestsFailed = true;  // Guilty until proven innocent
  try {
    // Create the file
    testFile(cmdLine.fileUrl, XrdCl::OpenFlags::New, XrdCl::Access::UR, "CONTENTS1", 0);

    // Re-create the file
    testFile(cmdLine.fileUrl, XrdCl::OpenFlags::Delete | XrdCl::OpenFlags::Write, XrdCl::Access::UR, "CONTENTS2", 0);

    // Try to open the file for modification
    testFile(cmdLine.fileUrl, XrdCl::OpenFlags::Write, XrdCl::Access::None, "", kXR_NotAuthorized);
    testFile(cmdLine.fileUrl, XrdCl::OpenFlags::Update, XrdCl::Access::None, "", kXR_NotAuthorized);
    oneOrMoreTestsFailed = false;
  }
  catch (exception::Exception& ex) {
    m_out << ex.getMessage().str() << std::endl;
  }
  catch (std::exception& se) {
    m_out << se.what() << std::endl;
  }
  catch (...) {
    m_out << "Caught an unknown exception" << std::endl;
  }

  m_out << std::endl;
  if (fileExists(cmdLine.fileUrl)) {
    m_out << "Test file still exists after the tests" << std::endl;
    try {
      deleteFile(cmdLine.fileUrl);
      m_out << "Successfully deleted test file" << std::endl;
    }
    catch (exception::Exception& ex) {
      m_out << "Failed to delete test file: " << ex.getMessage().str() << std::endl;
    }
    catch (std::exception& se) {
      m_out << "Failed to delete test file: " << se.what() << std::endl;
    }
    catch (...) {
      m_out << "Failed to delete test file: Caught an unknown exception" << std::endl;
    }
  }
  else {
    m_out << cmdLine.fileUrl.GetURL() << " does not exist after the tests" << std::endl;
  }

  return oneOrMoreTestsFailed ? 1 : 0;
}

//------------------------------------------------------------------------------
// userConfirmsDestroyFile
//------------------------------------------------------------------------------
bool ImmutableFileTest::userConfirmsDestroyFile(const std::string& fileUrl) const {
  m_out << "WARNING" << std::endl;
  m_out << "You are about to destroy the file with URL " << fileUrl << std::endl;
  m_out << "Are you sure you want to continue?" << std::endl;

  std::string userResponse;
  while (userResponse != "yes" && userResponse != "no") {
    m_out << "Please type either \"yes\" or \"no\" > ";
    std::getline(m_in, userResponse);
  }

  return userResponse == "yes";
}

//------------------------------------------------------------------------------
// fileExists
//------------------------------------------------------------------------------
bool ImmutableFileTest::fileExists(const XrdCl::URL& url) {
  XrdCl::FileSystem fs(url);
  XrdCl::StatInfo* info = 0;
  const XrdCl::XRootDStatus statStatus = fs.Stat(url.GetPath(), info);
  return statStatus.IsOK();
}

//------------------------------------------------------------------------------
// deleteFile
//------------------------------------------------------------------------------
void ImmutableFileTest::deleteFile(const XrdCl::URL& url) {
  XrdCl::FileSystem fs(url);
  const XrdCl::XRootDStatus rmStatus = fs.Rm(url.GetPath());

  if (!rmStatus.IsOK()) {
    exception::Exception ex;
    ex.getMessage() << "Failed to rm file: URL=" << url.GetURL() << " :" << rmStatus.ToStr();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// testFile
//------------------------------------------------------------------------------
void ImmutableFileTest::testFile(const XrdCl::URL& url,
                                 const XrdCl::OpenFlags::Flags openFlags,
                                 const XrdCl::Access::Mode openMode,
                                 const std::string& contents,
                                 const uint32_t expectedOpenErrNo) {
  bool testPassed = true;  // Innocent until proven guilty
  const bool expectedSuccess = 0 == expectedOpenErrNo;
  XrdCl::File file;

  m_out << std::endl;
  m_out << "START OF TEST" << std::endl;
  m_out << "Opening file with flags \"" << openFlagsToString(openFlags) << "\""
        << " and mode \"" << openModeToString(openMode) << "\" expecting " << xErrorCodeToString(expectedOpenErrNo)
        << std::endl;

  const XrdCl::XRootDStatus openStatus = file.Open(url.GetURL(), openFlags, openMode);
  {
    if (expectedSuccess) {
      if (openStatus.IsOK()) {
        m_out << "Succeeded to open file as expected" << std::endl;
      }
      else {
        testPassed = false;
        m_out << "Failure when success was expected: Failed to open file: " << openStatus.ToStr() << std::endl;
      }
    }
    else {
      if (openStatus.IsOK()) {
        testPassed = false;
        m_out << "Success when failure was expected: Successfully opened file" << std::endl;
      }
      else {
        if (expectedOpenErrNo == openStatus.errNo) {
          m_out << "Successfully got " << xErrorCodeToString(expectedOpenErrNo) << std::endl;
        }
        else {
          testPassed = false;
          m_out << "Unexpectedly got " << xErrorCodeToString(openStatus.errNo) << std::endl;
        }
      }
    }
  }

  if (testPassed && !contents.empty()) {
    const uint64_t offset = 0;
    const uint16_t timeout = 0;
    const XrdCl::XRootDStatus writeStatus = file.Write(offset, contents.size(), contents.c_str(), timeout);

    if (writeStatus.IsOK()) {
      m_out << "Successfully wrote \"" << contents << "\" to the beginning of the file" << std::endl;
    }
    else {
      testPassed = false;
      m_out << "Failed to write to file: " << writeStatus.ToStr() << std::endl;
    }
  }

  if (openStatus.IsOK()) {
    const XrdCl::XRootDStatus closeStatus = file.Close();
    if (!closeStatus.IsOK()) {
      testPassed = false;
      m_out << "Failed to close file:" << closeStatus.ToStr() << std::endl;
    }
  }

  if (testPassed) {
    m_out << "TEST PASSED" << std::endl;
  }
  else {
    exception::Exception ex;
    ex.getMessage() << "TEST FAILED";
    throw ex;
  }
}

//------------------------------------------------------------------------------
// openFlagsToString
//------------------------------------------------------------------------------
std::string ImmutableFileTest::openFlagsToString(XrdCl::OpenFlags::Flags flags) {
  if (XrdCl::OpenFlags::None == flags) {
    return "None";
  }

  typedef std::pair<XrdCl::OpenFlags::Flags, std::string> FlagAndName;
  std::list<FlagAndName> allFlags;
  allFlags.emplace_back(XrdCl::OpenFlags::Compress, "Compress");
  allFlags.emplace_back(XrdCl::OpenFlags::Delete, "Delete");
  allFlags.emplace_back(XrdCl::OpenFlags::Force, "Force");
  allFlags.emplace_back(XrdCl::OpenFlags::MakePath, "MakePath");
  allFlags.emplace_back(XrdCl::OpenFlags::New, "New");
  allFlags.emplace_back(XrdCl::OpenFlags::NoWait, "NoWait");
#if XrdMajorVNUM(XrdVNUMBER) < 5
  allFlags.emplace_back(XrdCl::OpenFlags::Append, "Append");
#endif
  allFlags.emplace_back(XrdCl::OpenFlags::Read, "Read");
  allFlags.emplace_back(XrdCl::OpenFlags::Update, "Update");
  allFlags.emplace_back(XrdCl::OpenFlags::Write, "Write");
  allFlags.emplace_back(XrdCl::OpenFlags::POSC, "POSC");
  allFlags.emplace_back(XrdCl::OpenFlags::Refresh, "Refresh");
  allFlags.emplace_back(XrdCl::OpenFlags::Replica, "Replica");
  allFlags.emplace_back(XrdCl::OpenFlags::SeqIO, "SeqIO");
  allFlags.emplace_back(XrdCl::OpenFlags::PrefName, "PrefName");

  std::ostringstream result;
  for (const auto& flagAndName : allFlags) {
    const XrdCl::OpenFlags::Flags flag = flagAndName.first;
    const std::string& name = flagAndName.second;

    if (flags & flag) {
      if (!result.str().empty()) {
        result << " | ";
      }
      result << name;
      flags &= ~flag;
    }
  }

  if (0 != flags) {
    if (!result.str().empty()) {
      result << " | ";
    }
    result << "ONE OR MORE UNKNOWN FLAGS";
  }

  return result.str();
}

//------------------------------------------------------------------------------
// openModeToString
//------------------------------------------------------------------------------
std::string ImmutableFileTest::openModeToString(XrdCl::Access::Mode mode) {
  if (XrdCl::Access::None == mode) {
    return "None";
  }

  typedef std::pair<XrdCl::Access::Mode, std::string> ModeAndName;
  std::list<ModeAndName> allModes;
  allModes.emplace_back(XrdCl::Access::UR, "UR");
  allModes.emplace_back(XrdCl::Access::UW, "UW");
  allModes.emplace_back(XrdCl::Access::UX, "UX");
  allModes.emplace_back(XrdCl::Access::GR, "GR");
  allModes.emplace_back(XrdCl::Access::GW, "GW");
  allModes.emplace_back(XrdCl::Access::GX, "GX");
  allModes.emplace_back(XrdCl::Access::OR, "OR");
  allModes.emplace_back(XrdCl::Access::OW, "OW");
  allModes.emplace_back(XrdCl::Access::OX, "OX");

  std::ostringstream result;
  for (const auto& modeAndName : allModes) {
    const XrdCl::Access::Mode modeToSearchFor = modeAndName.first;
    const std::string& name = modeAndName.second;

    if (mode & modeToSearchFor) {
      if (!result.str().empty()) {
        result << " | ";
      }
      result << name;
      mode &= ~modeToSearchFor;
    }
  }

  if (0 != mode) {
    if (!result.str().empty()) {
      result << " | ";
    }
    result << "ONE OR MORE UNKNOWN MODE PERMISSIONS";
  }

  return result.str();
}

//------------------------------------------------------------------------------
// xErrorCodeToString
//------------------------------------------------------------------------------
std::string ImmutableFileTest::xErrorCodeToString(const uint32_t code) {
  switch (code) {
    case 0:
      return "SUCCESS";
    case kXR_ArgInvalid:
      return "kXR_ArgInvalid";
    case kXR_ArgMissing:
      return "kXR_ArgMissing";
    case kXR_ArgTooLong:
      return "kXR_ArgTooLong";
    case kXR_FileLocked:
      return "kXR_FileLocked";
    case kXR_FileNotOpen:
      return "kXR_FileNotOpen";
    case kXR_FSError:
      return "kXR_FSError";
    case kXR_InvalidRequest:
      return "kXR_InvalidRequest";
    case kXR_IOError:
      return "kXR_IOError";
    case kXR_NoMemory:
      return "kXR_NoMemory";
    case kXR_NoSpace:
      return "kXR_NoSpace";
    case kXR_NotAuthorized:
      return "kXR_NotAuthorized";
    case kXR_NotFound:
      return "kXR_NotFound";
    case kXR_ServerError:
      return "kXR_ServerError";
    case kXR_Unsupported:
      return "kXR_Unsupported";
    case kXR_noserver:
      return "kXR_noserver";
    case kXR_NotFile:
      return "kXR_NotFile";
    case kXR_isDirectory:
      return "kXR_isDirectory";
    case kXR_Cancelled:
      return "kXR_Cancelled";
#if XrdMajorVNUM(XrdVNUMBER) < 5
    case kXR_ChkLenErr:
      return "kXR_ChkLenErr";
#endif
    case kXR_ChkSumErr:
      return "kXR_ChkSumErr";
    case kXR_inProgress:
      return "kXR_inProgress";
    case kXR_overQuota:
      return "kXR_overQuota";
    case kXR_SigVerErr:
      return "kXR_SigVerErr";
    case kXR_DecryptErr:
      return "kXR_DecryptErr";
    case kXR_Overloaded:
      return "kXR_Overloaded";
    default:
      return "UNKNOWN";
  }
}

}  // namespace cta
