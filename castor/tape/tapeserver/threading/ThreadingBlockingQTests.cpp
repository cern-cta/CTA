#include "BlockingQueue.hpp"
#include <gtest/gtest.h>

namespace threadedUnitTests {

    typedef castor::tape::threading::BlockingQueue<int> QueueType;

    const int numberOfElements = 10000;
    class PingThread : public castor::tape::threading::Thread {
        QueueType& queue;
    protected :
        virtual void run()  //@override 
        {
            
            for(int i=0;i<numberOfElements;++i)
            {
                int n = queue.pop();
                queue.push(n+1);
            }
        }
    public:
        PingThread(QueueType& q):queue(q){

        }
        

    };
    class PongThread : public castor::tape::threading::Thread {
        QueueType& queue;
    protected:
         virtual void run()  //@override 
        {
            for(int i=0;i<numberOfElements;++i)
            {
                int n = queue.pop();
                queue.push(n+1);
            }
        }
    public:
        PongThread(QueueType& q):queue(q){}
       
    };
    
    TEST(castor_tape_threading, BlockingQ_properly_working_on_helgrind) {
        QueueType sharedQueue;
        
        PingThread ping(sharedQueue);
        PongThread pong(sharedQueue);

        pong.start();
        sharedQueue.push(0);
        sharedQueue.push(-100);
        
        ping.start();
        
        sharedQueue.push(42);
        sharedQueue.push(21);
     
        
        pong.wait();
        ping.wait();
        
        ASSERT_EQ(4,sharedQueue.size());
  }

}


