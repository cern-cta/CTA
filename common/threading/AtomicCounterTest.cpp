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
#include <gtest/gtest.h>
#include "common/threading/Thread.hpp"
#include "common/threading/AtomicCounter.hpp"
namespace unitTests {
  struct ThreadPlus : public cta::threading::Thread{
    ThreadPlus( cta::threading::AtomicCounter<int>& c):count(c){}
  protected:
    cta::threading::AtomicCounter<int>& count;
    virtual void run (){
      for(int i=0;i<100;++i)
        ++count;
    }
  };
  struct ThreadMinus : public cta::threading::Thread{
    ThreadMinus( cta::threading::AtomicCounter<int>& c):count(c){}
  protected:
    cta::threading::AtomicCounter<int>& count;
    virtual void run (){
      for(int i=0;i<100;++i)
        --count;
    }
  };
  TEST(castor_tape_threading, AtomicCounterTest) {
    cta::threading::AtomicCounter<int> c(42);
    
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
