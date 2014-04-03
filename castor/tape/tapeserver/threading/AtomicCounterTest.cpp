#include <gtest/gtest.h>
#include "castor/tape/tapeserver/threading/Threading.hpp"
#include "castor/tape/tapeserver/threading/AtomicCounter.hpp"
namespace unitTest {
  struct ThreadPlus : public castor::tape::threading::Thread{
    ThreadPlus( castor::tape::threading::AtomicCounter<int>& c):count(c){}
  protected:
    castor::tape::threading::AtomicCounter<int>& count;
    virtual void run (){
      for(int i=0;i<100;++i)
        ++count;
    }
  };
  struct ThreadMinus : public castor::tape::threading::Thread{
    ThreadMinus( castor::tape::threading::AtomicCounter<int>& c):count(c){}
  protected:
    castor::tape::threading::AtomicCounter<int>& count;
    virtual void run (){
      for(int i=0;i<100;++i)
        --count;
    }
  };
  TEST(castor_tape_threading, AtomicCOunterTest) {
    castor::tape::threading::AtomicCounter<int> c(42);
    
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
