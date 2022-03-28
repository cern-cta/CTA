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

#ifndef ASYNC_HPP
#define ASYNC_HPP

#include "Thread.hpp"

namespace cta {
  
  /**
   * This class holds the necessary to reimplement the std::async function.
   * Indeed, helgrind does not like the way how std::async execute the std::function passed in parameter
   * (usage of a shared_ptr : https://stackoverflow.com/questions/8393777/current-state-of-drd-and-helgrind-support-for-stdthread/8458482#8458482)
   * 
   * How to use this class :
   * auto future = cta::Async::async([]{
   *   //DO SOME ACTIONS TO EXECUTE ASYNCHRONOUSLY
   * });
   */
  class Async{
    public:
      /**
       * This method allows to execute asynchronously the callable passed in parameter
       * @param callable the callable to execute asynchronously
       * @return the future associated to the execution of the callable. If an exception is thrown during the execution
       * of the callable, the future.get() will throw this exception
       */
      static std::future<void> async(std::function<void()> callable);
    private:
       class ThreadWrapper: public threading::Thread{
	 friend Async;
	public:
	  ThreadWrapper(std::function<void()> callable);
	  void run();
	private:
	  std::function<void()> m_callable;
	  std::promise<void> m_promise;
      };
    
  };
}

#endif /* ASYNC_HPP */

