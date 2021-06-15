/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2015-2021 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
