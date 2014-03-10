/******************************************************************************
 *                      MockTapeGateway.hpp
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

#include "castor/tape/tapeserver/daemon/MigrationJob.hpp"
#include "castor/tape/tapeserver/daemon/RecallJob.hpp"
#include <list>

class MockTapeGateway {
public:
  MockTapeGateway(int filesCount, int modulo): m_fileId(1000), m_fSeq(0), 
          m_filesCount(filesCount), m_modulo(modulo) {}
  /* Get more work for recall */
  std::list<RecallJob> getMoreWork(int maxFiles, int maxBlocks) {
    std::list<RecallJob> ret;
    int files, size;
    for(files=0, size=0; files <= maxFiles && size <= maxBlocks && m_filesCount > 0;) {
      int s = 1+(m_fSeq % m_modulo);
      size += s;
      files++;
      ret.push_back(RecallJob(m_fileId++, m_fSeq++, s));
      m_filesCount--;
    }
    return ret;
  }
  /* Report migration done. */
  void reportMigratedFiles(std::list<MigrationJob> jobsDone) {
    printf("Got %zd migration done report(s): ", jobsDone.size());
    for(std::list<MigrationJob>::iterator i = jobsDone.begin(); i != jobsDone.end(); i++) {
      printf("fSeq=%d ",i->fSeq);
    }
    printf("\n");
  }
  /* Report end of session */
  void reportEndOfSession(bool error) {
    if (error) {
      printf("Got end of session: error\n");
    } else {
      printf("Got end of session: success\n");
    }
  }
private:
  int m_fileId;
  int m_fSeq;
  int m_filesCount;
  int m_modulo;
};
