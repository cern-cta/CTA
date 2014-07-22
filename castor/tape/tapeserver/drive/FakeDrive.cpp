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

#include "castor/tape/tapeserver/drive/FakeDrive.hpp"

namespace {
const long unsigned int max_fake_drive_record_length = 1000;
const char filemark[] = "";
}

castor::tape::tapeserver::drives::FakeDrive::FakeDrive() throw() : m_current_position(0) {
  m_tape.reserve(max_fake_drive_record_length);
}
castor::tape::tapeserver::drives::compressionStats castor::tape::tapeserver::drives::FakeDrive::getCompression()  {
  throw Exception("FakeDrive::getCompression Not implemented");
}
void castor::tape::tapeserver::drives::FakeDrive::clearCompressionStats()  {
  throw Exception("FakeDrive::clearCompressionStats Not implemented");
}
castor::tape::tapeserver::drives::deviceInfo castor::tape::tapeserver::drives::FakeDrive::getDeviceInfo()  {
  deviceInfo devInfo;
  devInfo.product = "Fake Drv";
  devInfo.productRevisionLevel = "0.1";
  devInfo.vendor = "ACME Ind";
  devInfo.serialNumber = "123456";
  return devInfo;
}
std::string castor::tape::tapeserver::drives::FakeDrive::getSerialNumber()  {
  throw Exception("FakeDrive::getSerialNumber Not implemented");
}
void castor::tape::tapeserver::drives::FakeDrive::positionToLogicalObject(uint32_t blockId)  {
  m_current_position = blockId;
}
castor::tape::tapeserver::drives::positionInfo castor::tape::tapeserver::drives::FakeDrive::getPositionInfo()  {
  positionInfo pos;
  pos.currentPosition = m_current_position;
  pos.dirtyBytesCount = 0;
  pos.dirtyObjectsCount = 0;
  pos.oldestDirtyObject = 0;
  return pos;
}
std::vector<std::string> castor::tape::tapeserver::drives::FakeDrive::getTapeAlerts()  {
  throw Exception("FakeDrive::getTapeAlerts Not implemented");
}
void castor::tape::tapeserver::drives::FakeDrive::setDensityAndCompression(bool compression, unsigned char densityCode)  {
  throw Exception("FakeDrive::setDensityAndCompression Not implemented");
}
castor::tape::tapeserver::drives::driveStatus castor::tape::tapeserver::drives::FakeDrive::getDriveStatus()  {
  throw Exception("FakeDrive::getDriveStatus Not implemented");
}
castor::tape::tapeserver::drives::tapeError castor::tape::tapeserver::drives::FakeDrive::getTapeError()  {
  throw Exception("FakeDrive::getTapeError Not implemented");
}
void castor::tape::tapeserver::drives::FakeDrive::setSTBufferWrite(bool bufWrite)  {
  throw Exception("FakeDrive::setSTBufferWrite Not implemented");
}
void castor::tape::tapeserver::drives::FakeDrive::fastSpaceToEOM(void)  {
  m_current_position = m_tape.size()-1;
}
void castor::tape::tapeserver::drives::FakeDrive::rewind(void)  {
  m_current_position = 0;
}
void castor::tape::tapeserver::drives::FakeDrive::spaceToEOM(void)  {
  m_current_position = m_tape.size()-1;
}
void castor::tape::tapeserver::drives::FakeDrive::spaceFileMarksBackwards(size_t count)  {
  if(!count) return;
  size_t countdown = count;
  std::vector<std::string>::size_type i=0;
  for(i=m_current_position; i!=(std::vector<std::string>::size_type)-1 and countdown!=0; i--) {
    if(!m_tape[i].compare(filemark)) countdown--;
  }
  if(countdown) {
    throw Exception("FakeDrive::spaceFileMarksBackwards");
  }  
  m_current_position = i-1; //BOT side of the filemark
}
void castor::tape::tapeserver::drives::FakeDrive::spaceFileMarksForward(size_t count)  {
  if(!count) return;
  size_t countdown = count;
  std::vector<std::string>::size_type i=0;
  for(i=m_current_position; i!=m_tape.size() and countdown!=0; i++) {
    if(!m_tape[i].compare(filemark)) countdown--;
  }
  if(countdown) {
    throw castor::exception::Errnum(EIO, "Failed FakeDrive::spaceFileMarksForward");
  }
  m_current_position = i; //EOT side of the filemark
}
void castor::tape::tapeserver::drives::FakeDrive::unloadTape(void)  {
}
void castor::tape::tapeserver::drives::FakeDrive::flush(void)  {
  //already flushing
}
void castor::tape::tapeserver::drives::FakeDrive::writeSyncFileMarks(size_t count)  {
  if(count==0) return;  
  m_tape.resize(m_current_position+count);
  for(size_t i=0; i<count; ++i) {
    if(m_current_position<m_tape.size()) {
      m_tape[m_current_position] = filemark;
    }
    else {
      m_tape.push_back(filemark);
    }
    m_current_position++;
  }
}
void castor::tape::tapeserver::drives::FakeDrive::writeImmediateFileMarks(size_t count)  {
  writeSyncFileMarks(count);
}
void castor::tape::tapeserver::drives::FakeDrive::writeBlock(const void * data, size_t count)  {  
  m_tape.resize(m_current_position+1);
  m_tape[m_current_position].assign((const char *)data, count);
  m_current_position++;
}
ssize_t castor::tape::tapeserver::drives::FakeDrive::readBlock(void *data, size_t count)  {
  if(count < m_tape[m_current_position].size()) {
    throw Exception("Block size too small in FakeDrive::readBlock");
  }
  size_t bytes_copied = m_tape[m_current_position].copy((char *)data, m_tape[m_current_position].size());
  m_current_position++;
  return bytes_copied;
}
std::string castor::tape::tapeserver::drives::FakeDrive::contentToString() throw() {
  std::stringstream exc;
  exc << std::endl;
  exc << "Tape position: " << m_current_position << std::endl;
  exc << std::endl;
  exc << "Tape contents:" << std::endl;
  for(unsigned int i=0; i<m_tape.size(); i++) {
    exc << i << ": " << m_tape[i] << std::endl;
  }
  exc << std::endl;
  return exc.str();
}
void castor::tape::tapeserver::drives::FakeDrive::readExactBlock(void *data, size_t count, std::string context)  {
  if(count != m_tape[m_current_position].size()) {
    std::stringstream exc;
    exc << "Wrong block size in FakeDrive::readExactBlock. Expected: " << count << " Found: " << m_tape[m_current_position].size() << " Position: " << m_current_position << " String: " << m_tape[m_current_position] << std::endl;
    exc << contentToString();
    throw Exception(exc.str());
  }
  if(count != m_tape[m_current_position].copy((char *)data, count)) {
    throw Exception("Failed FakeDrive::readExactBlock");
  }
  m_current_position++;
}
void castor::tape::tapeserver::drives::FakeDrive::readFileMark(std::string context)  {
  if(m_tape[m_current_position].compare(filemark)) {
    throw Exception("Failed FakeDrive::readFileMark");
  }
  m_current_position++;  
}
bool castor::tape::tapeserver::drives::FakeDrive::waitUntilReady(int timeoutSecond)  {
  return true;
}  
bool castor::tape::tapeserver::drives::FakeDrive::isWriteProtected()  {
  return false;
}
bool castor::tape::tapeserver::drives::FakeDrive::isAtBOT()  {
  return m_current_position==0;
}
bool castor::tape::tapeserver::drives::FakeDrive::isAtEOD()  {
  return m_current_position==m_tape.size()-1;
}

bool castor::tape::tapeserver::drives::FakeDrive::isTapeBlank() {
  return m_tape.empty();
}

bool castor::tape::tapeserver::drives::FakeDrive::hasTapeInPlace() {
  return true;
}
