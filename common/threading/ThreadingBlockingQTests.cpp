/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2021 CERN
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

#include "common/threading/BlockingQueue.hpp"
#include <gtest/gtest.h>

namespace threadedUnitTests {

    typedef cta::threading::BlockingQueue<int> QueueType;

    const int numberOfElements = 10000;
    class PingThread : public cta::threading::Thread {
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
    class PongThread : public cta::threading::Thread {
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
    
    TEST(cta_threading, BlockingQ_properly_working_on_helgrind) {
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
        
        ASSERT_EQ(4U,sharedQueue.size());
  }

}


