
#include "castor/exception/Exception.hpp"
#include "castor/tape/format/ALB0100Unmarshaller.hpp"

#include <sys/stat.h>
#include <fstream>
#include <iostream>
#include <cstdlib>  // for exit function
#include <cstdlib>  // for exit function

using std::ifstream;
using std::ofstream;
using std::cerr;
using std::cout;
using std::endl;
using std::ios;

/* Program that read a file containing haeder data+file data and 
 * return different separate files (as they were before the marshall) 
 */
int main(int argc, char** argv) {
  
  using namespace castor::tape::format;
  //-------------------------------------------------------------------------------  
  ifstream file;   
  ofstream fileout;
  char     *buffer;       // pointer 32 or 64 bit
  int      buffLength, 
    payloadOffset, 
    file_name_length,
    payloadSize;          // int -> 2 Bytes (or 4)
 
  ALB0100Unmarshaller                       unmarshallObj;
  const ALB0100Unmarshaller::Header        *myHeader;
  const ALB0100Unmarshaller::ALB0100Header *myALB0100Header;

  //-------------------------------------------------------------------------------  
  buffLength =       BLOCK_SIZE; 	     // 256 KiB
  payloadOffset =    HEADER_SIZE;     	     // 1 KiB
  buffer =           new char[buffLength];   // Header + payload 256 KiB
  file_name_length = 1000*sizeof(char);
  payloadSize =      BLOCK_SIZE-HEADER_SIZE; // 255 KiB
    
  //-------------------------------------------------------------------------------  
  file.open("./TXTfiles/fileout.txt"); // open as text file
  if(!file) { // file couldn't be opened
    cerr << "Error: FILES OUT's file could not be opened" << endl;
    exit(1);
  }    

  file.read(buffer, buffLength);
  unmarshallObj.readVersion(buffer);
  bool first = true; // to specify 'first block of a file'

  while(! file.eof() ){
     
    try{ 
      myHeader = unmarshallObj.unmarshallHeader(buffer, first);
    }catch(castor::exception::Exception ex ) {
      ex.getMessage() << "Failed to call 'unmarshallObj.unmarshallHeader'";
      throw ex;
    }
    try{
      myALB0100Header = dynamic_cast<const ALB0100Unmarshaller::ALB0100Header*>(myHeader);
    }catch(castor::exception::Exception ex ) {
      ex.getMessage() << "Failed to call 'unmarshallObj.unmarshallHeader'";
      throw ex;
    }

    if(myALB0100Header->file_block_count == 0){// is the first file block!! open a new file.
      char str[alb0100::FILE_NAME_LEN+1];
      strcpy(str, myALB0100Header->file_name);
      strcat(str, ".NEW");// Recreate the file in the same source location, whit the same name + '.NEW'
      fileout.open(str, ios::binary | ios::out); // open as text file
      if(!fileout) { // file couldn't be opened
	cerr << "Error: FILES OUT's file could not be opened" << endl;
	exit(1);
      }  
    }
      
    if(myALB0100Header->file_block_count+payloadSize >= myALB0100Header->file_size){// Is the last block of the unmarshalling file?
	
      fileout.write(buffer+payloadOffset, myALB0100Header->file_size - myALB0100Header->file_block_count);
      fileout.close();
      if(fileout.fail()){fileout.clear();} 
      first = true;	  
    }
    else{// Is not the last block of the unmarshalling file

      first= false;
      fileout.write(buffer+payloadOffset, payloadSize);
    }
   

    file.read(buffer, buffLength);
    
  }// end while file


  file.close();
  if(file.fail()){file.clear();} 

  delete[] buffer;
    
  return (EXIT_SUCCESS);
}
