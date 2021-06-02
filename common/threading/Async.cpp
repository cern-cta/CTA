
#include <future>
#include "Async.hpp"
#include "Thread.hpp"

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
namespace cta { 
  Async::ThreadWrapper::ThreadWrapper(std::function<void()> callable):m_callable(callable){}

  void Async::ThreadWrapper::run(){
    try{
      m_callable();
      m_promise.set_value();
    } catch (...){
      m_promise.set_exception(std::current_exception());
    }
  }
  
  std::future<void> Async::async(std::function<void()> callable){
    std::future<void> ret;
    Async::ThreadWrapper threadCallable(callable);
    ret = threadCallable.m_promise.get_future();
    threadCallable.run();
    return ret;
  }
  
}