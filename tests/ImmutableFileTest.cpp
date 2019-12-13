/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2019  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
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

namespace cta {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
ImmutableFileTest::ImmutableFileTest(
  std::istream &inStream,
  std::ostream &outStream,
  std::ostream &errStream):
  m_in(inStream),
  m_out(outStream),
  m_err(errStream) {
}

//------------------------------------------------------------------------------
// main
//------------------------------------------------------------------------------
int ImmutableFileTest::main(const int argc, char *const *const argv) {
  bool cmdLineNotParsed = false;
  std::string errorMessage;

  try {
    return exceptionThrowingMain(argc, argv);
  } catch(exception::CommandLineNotParsed &ue) {
    errorMessage = ue.getMessage().str();
    cmdLineNotParsed = true;
  } catch(exception::Exception &ex) {
    errorMessage = ex.getMessage().str();
  } catch(std::exception &se) {
    errorMessage = se.what();
  } catch(...) {
    errorMessage = "An unknown exception was thrown";
  }

  // Reaching this point means the command has failed, an exception was throw
  // and errorMessage has been set accordingly

  m_err << errorMessage << std::endl;
  if(cmdLineNotParsed) {
    m_err << std::endl;
    ImmutableFileTestCmdLineArgs::printUsage(m_err);
  }
  return 1;
}

//------------------------------------------------------------------------------
// exceptionThrowingMain
//------------------------------------------------------------------------------
int ImmutableFileTest::exceptionThrowingMain(const int argc, char *const *const argv) {
  const ImmutableFileTestCmdLineArgs cmdLine(argc, argv);

  if(cmdLine.help) {
    ImmutableFileTestCmdLineArgs::printUsage(m_out);
    return 0;
  }

  if(cmdLine.fileUrl.IsLocalFile()) {
    exception::Exception ex;
    ex.getMessage() << cmdLine.fileUrl.GetURL() << " is local";
    throw ex;
  }
  if(fileExists(cmdLine.fileUrl)) {
    m_out << cmdLine.fileUrl.GetURL() << " already exists" << std::endl;
    deleteFile(cmdLine.fileUrl);
    m_out << "Deleted " << cmdLine.fileUrl.GetURL() << std::endl;
  } else {
    m_out << cmdLine.fileUrl.GetURL() << " does not exist yet" << std::endl;
  }

  m_out << "Ready to create " << cmdLine.fileUrl.GetURL()  << std::endl;
  m_out << std::endl;
  try {
    testOpenCloseFile(cmdLine.fileUrl, XrdCl::OpenFlags::New, 0);
    testOpenCloseFile(cmdLine.fileUrl, XrdCl::OpenFlags::Delete | XrdCl::OpenFlags::Write, 0);
    testOpenCloseFile(cmdLine.fileUrl, XrdCl::OpenFlags::Write, kXR_NotAuthorized);
    testOpenCloseFile(cmdLine.fileUrl, XrdCl::OpenFlags::Update, kXR_NotAuthorized);
    testOpenCloseFile(cmdLine.fileUrl, XrdCl::OpenFlags::Append, kXR_NotAuthorized);
  } catch(...) {
    try {
      deleteFile(cmdLine.fileUrl);
    } catch(...) {
      // Do nothing
    }
    throw;
  }

  return 0;
}

//------------------------------------------------------------------------------
// fileExists
//------------------------------------------------------------------------------
bool ImmutableFileTest::fileExists(const XrdCl::URL &url) {
  XrdCl::FileSystem fs(url);
  XrdCl::StatInfo *info = 0;
  const XrdCl::XRootDStatus statStatus = fs.Stat(url.GetPath(), info);
  if(!statStatus.IsOK()) m_out << statStatus.ToStr() << std::endl;
  return statStatus.IsOK();
}

//------------------------------------------------------------------------------
// deleteFile
//------------------------------------------------------------------------------
void ImmutableFileTest::deleteFile(const XrdCl::URL &url) {
  XrdCl::FileSystem fs(url);
  const XrdCl::XRootDStatus rmStatus = fs.Rm(url.GetPath());

  if (!rmStatus.IsOK()) {
    exception::Exception ex;
    ex.getMessage() << "Failed to rm file: URL=" << url.GetURL() << " :" << rmStatus.ToStr();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// testOpenCloseFile
//------------------------------------------------------------------------------
void ImmutableFileTest::testOpenCloseFile(const XrdCl::URL &url,
  const XrdCl::OpenFlags::Flags openFlags, const uint32_t expectedOpenErrNo) {
  bool testPassed = false;
  std::ostringstream msg;
  const bool expectedSuccess = 0 == expectedOpenErrNo;
  XrdCl::File file;

  m_out << "START OF TEST: Opening " << url.GetURL() << " with flags \"" << openFlagsToString(openFlags) << "\"" <<
    " expecting " << xErrorCodeToString(expectedOpenErrNo) << std::endl;

  const XrdCl::Access::Mode openMode = XrdCl::Access::UR | XrdCl::Access::UW;
  const XrdCl::XRootDStatus openStatus = file.Open(url.GetURL(), openFlags, openMode);
  {
    if (expectedSuccess) {
      if(openStatus.IsOK()) {
        testPassed = true;
        msg << "Succeeded to open file as expected";
      } else {
        testPassed = false;
        msg << "Failure when success was expected: Failed to open file: " << openStatus.ToStr();
      }
    } else {
      if (openStatus.IsOK()) {
        testPassed = false;
        msg << "Success when failure was expected: Successfully opened file";
      } else {
        if (expectedOpenErrNo == openStatus.errNo) {
          testPassed = true;
          msg << "Successfully got " << xErrorCodeToString(expectedOpenErrNo);
        } else {
          testPassed = false;
          msg << "Unexpectedly got " << xErrorCodeToString(openStatus.errNo);
        }
      }
    }
  }

  if (openStatus.IsOK()){
    const XrdCl::XRootDStatus closeStatus = file.Close();
    if (!closeStatus.IsOK()) {
      exception::Exception ex;
      ex.getMessage() << "Failed to close \"" << url.GetURL() << "\" :" << closeStatus.ToStr();
      throw ex;
    }
  }

  if(testPassed) {
    m_out << "END   OF TEST: PASSED: " << msg.str() << std::endl;
  } else {
    exception::Exception ex;
    ex.getMessage() << "END   OF TEST: FAILED: " << msg.str();
    throw ex;
  }
}

//------------------------------------------------------------------------------
// openFlagsToString
//------------------------------------------------------------------------------
std::string ImmutableFileTest::openFlagsToString(XrdCl::OpenFlags::Flags flags) {
  typedef std::pair<XrdCl::OpenFlags::Flags, std::string> FlagAndName;
  std::list<FlagAndName> allFlags;
  allFlags.emplace_back(XrdCl::OpenFlags::Compress, "Compress");
  allFlags.emplace_back(XrdCl::OpenFlags::Delete, "Delete");
  allFlags.emplace_back(XrdCl::OpenFlags::Force, "Force");
  allFlags.emplace_back(XrdCl::OpenFlags::MakePath, "MakePath");
  allFlags.emplace_back(XrdCl::OpenFlags::New, "New");
  allFlags.emplace_back(XrdCl::OpenFlags::NoWait, "NoWait");
  allFlags.emplace_back(XrdCl::OpenFlags::Append, "Append");
  allFlags.emplace_back(XrdCl::OpenFlags::Read, "Read");
  allFlags.emplace_back(XrdCl::OpenFlags::Update, "Update");
  allFlags.emplace_back(XrdCl::OpenFlags::Write, "Write");
  allFlags.emplace_back(XrdCl::OpenFlags::POSC, "POSC");
  allFlags.emplace_back(XrdCl::OpenFlags::Refresh, "Refresh");
  allFlags.emplace_back(XrdCl::OpenFlags::Replica, "Replica");
  allFlags.emplace_back(XrdCl::OpenFlags::SeqIO, "SeqIO");
  allFlags.emplace_back(XrdCl::OpenFlags::PrefName, "PrefName");

  std::ostringstream result;
  for(const auto &flagAndName : allFlags) {
    const XrdCl::OpenFlags::Flags flag = flagAndName.first;
    const std::string &name = flagAndName.second;

    if(flags & flag) {
      if(!result.str().empty()) {
        result << " | ";
      }
      result << name;
      flags &= ~flag;
    }
  }

  if(0 != flags) {
    if(!result.str().empty()) {
      result << " | ";
    }
    result << "ONE OR MORE UNKNOWN FLAGS";
  }

  return result.str();
}

//------------------------------------------------------------------------------
// xErrorCodeToString
//------------------------------------------------------------------------------
std::string ImmutableFileTest::xErrorCodeToString(const uint32_t code) {
  switch(code) {
  case 0: return "SUCCESS";
  case kXR_ArgInvalid: return "kXR_ArgInvalid";
  case kXR_ArgMissing: return "kXR_ArgMissing";
  case kXR_ArgTooLong: return "kXR_ArgTooLong";
  case kXR_FileLocked: return "kXR_FileLocked";
  case kXR_FileNotOpen: return "kXR_FileNotOpen";
  case kXR_FSError: return "kXR_FSError";
  case kXR_InvalidRequest: return "kXR_InvalidRequest";
  case kXR_IOError: return "kXR_IOError";
  case kXR_NoMemory: return "kXR_NoMemory";
  case kXR_NoSpace: return "kXR_NoSpace";
  case kXR_NotAuthorized: return "kXR_NotAuthorized";
  case kXR_NotFound: return "kXR_NotFound";
  case kXR_ServerError: return "kXR_ServerError";
  case kXR_Unsupported: return "kXR_Unsupported";
  case kXR_noserver: return "kXR_noserver";
  case kXR_NotFile: return "kXR_NotFile";
  case kXR_isDirectory: return "kXR_isDirectory";
  case kXR_Cancelled: return "kXR_Cancelled";
  case kXR_ChkLenErr: return "kXR_ChkLenErr";
  case kXR_ChkSumErr: return "kXR_ChkSumErr";
  case kXR_inProgress: return "kXR_inProgress";
  case kXR_overQuota: return "kXR_overQuota";
  case kXR_SigVerErr: return "kXR_SigVerErr";
  case kXR_DecryptErr: return "kXR_DecryptErr";
  case kXR_Overloaded: return "kXR_Overloaded";
  default: return "UNKNOWN";
  }
}

} // namespace cta
