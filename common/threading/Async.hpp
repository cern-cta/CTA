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

