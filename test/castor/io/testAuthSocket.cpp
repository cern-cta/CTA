/******************************************************************************
 *                      testSocket.cpp
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
 * @(#)$RCSfile: testAuthSocket.cpp,v $ $Revision: 1.1 $ $Release$ $Date: 2004/07/19 15:12:43 $ $Author: bcouturi $
 *
 * 
 *
 * @author AuthtSocket.cpp
 *****************************************************************************/

#include <castor/io/AuthServerSocket.hpp>
#include <castor/io/AuthClientSocket.hpp>
#include "castor/rh/File.hpp"
#include "castor/exception/Exception.hpp"
#include <iostream>
#include <sys/types.h>
#include <unistd.h>
#include <string>

int main (int argc, char** argv) {

  if (argc > 1) {
    try {
      if (argc < 3) {
	std::cerr << "Usage: " << argv[0] << " machineName" << " fileOjectName" << std::endl; \
	exit(1);
      }

      std::string *cn = new std::string(argv[1]);
      std::cout << "CLIENT Creating socket" << std::endl;
      castor::io::AuthClientSocket *client = new castor::io::AuthClientSocket(7777, *cn);
      std::cout << "CLIENT socket created" << std::endl;
      client->connect();
      
      castor::rh::File* f2 = new castor::rh::File();
      f2->setName(argv[2]);
      
      client->sendObject(*f2);
      std::cout << "CLIENT File Object sent" << std::endl;

      delete client;

    } catch(castor::exception::Exception ex) {
      std::cerr << "CLIENT ERROR:" << ex.getMessage().str() << std::endl;
      exit(1);
    }

  } else {
    try {
      std::cout << "SERVER Creating socket" << std::endl;
      castor::io::AuthServerSocket *server = new castor::io::AuthServerSocket(7777, false);
      server->listen();
      std::cout << "SERVER Going to accept" << std::endl;
      castor::io::ServerSocket *cs = server->accept(); 
      std::cout << "SERVER Accept done" << std::endl;
      
      castor::IObject* cobj = cs->readObject();
      castor::rh::File* f = dynamic_cast<castor::rh::File*>(cobj);
      std::cout << "Received Object: " << f->name() << std::endl; 

      
    } catch(castor::exception::Exception ex) {
      std::cerr << "SERVER ERROR:" << ex.getMessage().str() << std::endl;
      exit(1);
    }
    
  }
}

