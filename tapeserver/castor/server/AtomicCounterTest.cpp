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
#include <gtest/gtest.h>
#include "castor/server/Threading.hpp"
#include "castor/server/AtomicCounter.hpp"
namespace unitTests {
  struct ThreadPlus : public castor::server::Thread{
    ThreadPlus( castor::server::AtomicCounter<int>& c):count(c){}
  protected:
    castor::server::AtomicCounter<int>& count;
    virtual void run (){
      for(int i=0;i<100;++i)
        ++count;
    }
  };
  struct ThreadMinus : public castor::server::Thread{
    ThreadMinus( castor::server::AtomicCounter<int>& c):count(c){}
  protected:
    castor::server::AtomicCounter<int>& count;
    virtual void run (){
      for(int i=0;i<100;++i)
        --count;
    }
  };
  TEST(castor_tape_threading, AtomicCounterTest) {
    castor::server::AtomicCounter<int> c(42);
    
    ThreadPlus t(c);
    ThreadPlus t1(c);
    ThreadMinus t2(c);
    t.start();
    t1.start();
    t2.start();
    
    t.wait();
    t1.wait();
    t2.wait();
    int i = c;
    ASSERT_EQ(142,i);
  }
}
