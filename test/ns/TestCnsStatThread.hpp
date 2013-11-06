/******************************************************************************
 *                      TestCnsStatThread.hpp
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
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#include <sys/time.h>
#include <vector>
#include "occi.h"

#include "castor/BaseObject.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/server/IThread.hpp"
#include "castor/server/Mutex.hpp"

class TestCnsStatThread : public castor::server::IThread,
                          public castor::BaseObject {

  public:
      
    TestCnsStatThread();
    
    virtual void run(void *param);
    
    virtual void init() throw() {};
    
    virtual void stop() throw() {};

    ~TestCnsStatThread() throw() {};
    
  private:
    
    u_signed64 m_procTime, m_wallTime, m_reqCount;
    timeval m_timeStart;
    unsigned int m_nbThreads;
    
    std::vector<std::string> m_files;
    
    castor::server::Mutex* m;

    u_signed64 cnsStat(oracle::occi::Statement* m_cnsStatStatement, std::string filepath)
      throw (castor::exception::Exception);
    
};
