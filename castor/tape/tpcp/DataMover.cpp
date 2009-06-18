/******************************************************************************
 *                 castor/tape/tpcp/DataMover.cpp
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
 *
 * @author Nicola.Bessone@cern.ch Steven.Murray@cern.ch
 *****************************************************************************/
 
#include "castor/tape/tpcp/DataMover.hpp"


//------------------------------------------------------------------------------
// run
//------------------------------------------------------------------------------
void castor::tape::tpcp::DataMover::run(
  castor::tape::tpcp::ParsedCommandLine &parsedCommandLine, const char *dgn,
  castor::io::ServerSocket &callbackSocket)
  throw(castor::exception::Exception) {

/*
    // Get the port range to be used by the aggregator callback socket
    int   lowPort  = LOW_CLIENT_PORT_RANGE;
    int   highPort = HIGH_CLIENT_PORT_RANGE;
    char* sport;
    if((sport = getconfent((char *)CLIENT_CONF,(char *)LOWPORT_CONF,0)) != 0) {
      lowPort = castor::System::porttoi(sport);
    }
    if((sport = getconfent((char *)CLIENT_CONF,(char *)HIGHPORT_CONF,0)) != 0) {
      highPort = castor::System::porttoi(sport);
    }

    // Bind the aggregator callback socket
    m_callbackSocket.bind(lowPort, highPort);
    m_callbackSocket.listen();

    unsigned short port = 0;
    unsigned long  ip   = 0;
    m_callbackSocket.getPortIp(port, ip);

    int VolReqID = 0;
    // Send the request to read a tape
    std::cerr<<"vdqm_SendAggregatorVolReq (NULL, " << VolReqID << ", " 
      << m_parsedCommandLine.vid <<", "<< m_parsedCommandLine.dgn
      <<", NULL, NULL, "
      << actionToString(m_parsedCommandLine.action) <<", "
      <<port<< ")"<<std::endl;
    // *nv, *reqID, *VID, *dgn, *server, *unit, mode, client_port
    int rc = 0;
    rc = vdqm_SendAggregatorVolReq(NULL, &VolReqID,  m_parsedCommandLine.vid, 
       m_parsedCommandLine.dgn, NULL, NULL, 
       actionToString(m_parsedCommandLine.action), port);

    if ( rc == -1 ) {
      char codeStr[STRERRORBUFLEN];
      strerror_r(errno, codeStr, sizeof(codeStr));
      TAPE_THROW_EX(castor::exception::Internal,
        ": vdqm_SendAggregatorVolReq()"
        ": Error=" << codeStr);
    }
  
    fprintf(stdout,
      "Volume request successfully submitted. Volume request ID:%d\n", VolReqID);

    //.........................................................................
    // Wait for the Aggreator messege 
    std::auto_ptr<castor::io::ServerSocket> socket(
      (castor::io::ServerSocket*)waitForCallBack());

    std::auto_ptr<castor::IObject>  obj(socket->readObject());
    std::cerr<<"Obj type: " << obj->type()<<std::endl;
    // OBJ_VolumeRequest = 168

    //.........................................................................
    // Create and send the Volume Request message to the Aggregator 
    castor::tape::tapegateway::Volume volumeMsg; 

    volumeMsg.setVid(vid);
    volumeMsg.setMode(mode);
    volumeMsg.setLabel("aul\0");
    volumeMsg.setTransactionId(VolReqID);
    volumeMsg.setDensity("1000GC\0");

    socket->sendObject(volumeMsg);

    // close the socket!!
    //delete(socket);  //
    delete(socket.release());

    //.........................................................................
    // Wait for the Aggreator messege 
    socket.reset((castor::io::ServerSocket*)waitForCallBack());

    std::auto_ptr<castor::IObject>  obj2(socket->readObject());
    std::cerr<<"Obj type: " << obj2->type()<<std::endl;
    // OBJ_FileToRecallRequest = 166


    // Loop over all the files to recall

    {
      //.........................................................................
      // Send a FileToRecall message to the Aggregator
      castor::tape::tapegateway::FileToRecall fileToRecallMsg;
  
      fileToRecallMsg.setTransactionId(VolReqID);
      fileToRecallMsg.setNshost("castorns\0");
      fileToRecallMsg.setFileid(320723286);
      fileToRecallMsg.setFseq(5);
      fileToRecallMsg.setPositionCommandCode(
        castor::tape::tapegateway::PositionCommandCode(3));
    
      fileToRecallMsg.setPath("lxc2disk15.cern.ch:/tmp/volume_I02000_file_n5");
      fileToRecallMsg.setBlockId0(0);
      fileToRecallMsg.setBlockId1(0);
      fileToRecallMsg.setBlockId2(0);
      fileToRecallMsg.setBlockId3(41);
    
      socket->sendObject(fileToRecallMsg);

      // close the socket!!
      delete(socket.release());


      //.........................................................................
      // Wait for the Aggreator messege 
      socket.reset((castor::io::ServerSocket*)waitForCallBack());

      std::auto_ptr<castor::IObject>  obj3(socket->readObject());
      std::cerr<<"Obj type: " << obj3->type()<<std::endl;
      // OBJ_FileToRecallRequest = 166
    }


    //.........................................................................
    // Sent a NoMoreFile message
    castor::tape::tapegateway::NoMoreFiles noMore;
    noMore.setTransactionId(VolReqID);

    socket->sendObject(noMore);

    delete(socket.release());


    //.........................................................................
    // Wait for the Aggreator messege 
    socket.reset((castor::io::ServerSocket*)waitForCallBack());

    std::auto_ptr<castor::IObject>  obj4(socket->readObject());
    std::cerr<<"Obj type: " << obj4->type()<<std::endl;
    //OBJ_FileRecalledNotification = 163

    //.........................................................................
    // Notification Acknowledge 
    castor::tape::tapegateway::NotificationAcknowledge notificationAcknowledge;
    notificationAcknowledge.setTransactionId(VolReqID);
    
    socket->sendObject(notificationAcknowledge);

    delete(socket.release());

    //.........................................................................
    // Wait for the Aggreator messege 
    socket.reset((castor::io::ServerSocket*)waitForCallBack());

    std::auto_ptr<castor::IObject>  obj5(socket->readObject());
    std::cerr<<"Obj type: " << obj5->type()<<std::endl;
    // OBJ_EndNotification = 172

    
    //.........................................................................
    // Notification Acknowledge
    socket->sendObject(notificationAcknowledge);

    delete(socket.release());

    //.........................................................................

    std::cerr<<"We have probably recalled a file (lxc2disk15.cern.ch:/tmp/volume_I02000_file_n5)"<<std::endl;

*/
}
