/******************************************************************************
 *                   PipeSocket.hpp
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
 * A dedicated socket on top of standard file descriptors to be used
 * as communication channel between a parent and its forked children process
 *
 * @author Giuseppe Lo Presti
 *****************************************************************************/

#ifndef CASTOR_IO_PIPESOCKET_HPP
#define CASTOR_IO_PIPESOCKET_HPP 1

// Include Files
#include "castor/io/AbstractSocket.hpp"

namespace castor {

  namespace io {
    
    const int PIPE_READ = 1;
    const int PIPE_WRITE = 2;
    const int PIPE_RW = 3;

    /**
     * Pipe version of the abstract socket class, able
     * to handle sending and receiving of IObjects over standard
     * file descriptors
     */
    class PipeSocket : public AbstractSocket {

    public:

      /**
       * Default constructor: creates a standard Unix pipe and builds a socket
       * on its file descriptors.
       * The socket is considered bidirectional at the beginning
       * but it must be closed on one direction: typically, a forked child
       * keeps one end and the parent keeps the other end.
       * @throw exception if the pipe cannot be created
       */
      PipeSocket()
        throw (castor::exception::Exception);

      /**
       * Constructor building a socket on the given file descriptors.
       * The socket is considered readable/writable according to the passed mode
       * @param fdIn the fd for reading
       * @param fdOut the fd for writing
       * @param mode one of PIPE_READ, PIPE_WRITE, PIPE_RW
       */
      PipeSocket(const int fdIn, const int fdOut, const int mode)
        throw (castor::exception::Exception);

      /**
       * Default destructor. Closes the pipe.
       */
      virtual ~PipeSocket() throw();
      
      /**
       * Closes the socket for reading
       */
      void closeRead();
      
      /**
       * Closes the socket for writing
       */
      void closeWrite();

      /**
       * Returns the fd for reading
       */
      int getFdRead() const { return m_fdIn; };

      /**
       * Returns the fd for writing
       */
      int getFdWrite() const { return m_fdOut; };
      
    protected:

      /**
       * Internal method to create the inner socket
       * not needed for this implementation
       */
      virtual void createSocket() throw (castor::exception::Exception) {};

      /**
       * Internal method to send the content of a buffer
       * over the socket.
       * @param magic the magic number to be used as first
       * 4 bytes of the data sent
       * @param buf the data to send
       * @param n the length of buf
       */
      virtual void sendBuffer(const unsigned int magic,
                              const char* buf,
                              const int n)
        throw (castor::exception::Exception);

      /**
       * Internal method to read from a socket into a buffer.
       * @param magic the magic number expected as the first 4 bytes read.
       * An exception is sent if the correct magic number is not found
       * @param buf allocated by the method, it contains the data read.
       * Note that the deallocation of this buffer is the responsability
       * of the caller
       * @param n the length of the allocated buffer
       */
      virtual void readBuffer(const unsigned int magic,
                              char** buf,
                              int& n)
        throw (castor::exception::Exception);
        
    private:
    
       /// file descriptors used for the I/O
       int m_fdIn, m_fdOut;

       /// operation mode
       int m_mode;
       
    };

  } // end of namespace io

} // end of namespace castor

#endif // CASTOR_IO_PIPESOCKET_HPP

