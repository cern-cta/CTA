/******************************************************************************
 *                      RrTest.hpp
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
 * @(#)$RCSfile: RrTest.hpp,v $ $Revision: 1.1 $ $Release$ $Date: 2004/08/19 10:06:23 $ $Author: bcouturi $
 *
 * 
 *
 * @author Benjamin Couturier
 *****************************************************************************/


#ifndef CASTOR_RRTEST_HPP 
#define CASTOR_RRTEST_HPP 1

#include "castor/BaseObject.hpp"

namespace test {

  class RrTest: public castor::BaseObject  {
    
  public:
    RrTest();
    int start();

  }; // class RrTest

  class RrTestClient  {
    
  public:
    RrTestClient(char *srvIp);
    int start();
    ~RrTestClient();

  protected:
    int listen();
    int mListenPort;
    int mSocket;
    int mChallenge;
    char *mSrvIp;

  }; // class RrTest



} // Namespace test
#endif // CASTOR_RRTEST_HPP
