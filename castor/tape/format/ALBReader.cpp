
#include "castor/exception/Exception.hpp"
#include "castor/tape/format/ALB0100Unmarshaller.hpp"

#include <sys/stat.h>
#include <fstream>
#include <iostream>
#include <errno.h>
#include <cstdlib>  // for exit function
#include <typeinfo>

using std::ifstream;
using std::ofstream;
using std::cerr;
using std::cout;
using std::endl;
using std::ios;


/**
 * Implementation of a simple auto pointer for a char array.
 */
//==============================================================================
template <typename T>
class Array_auto_ptr
{
public:
  Array_auto_ptr(T *p) : p_(p) { }
  ~Array_auto_ptr()            { delete [] p_; }
  T* get()                     { return (p_); }
private:
  T* p_;
};


/**
 * Program that read a file containing haeder data+file data and 
 * return different separate files (as they were before the marshall) 
 */
//==============================================================================
int main(int, char**) {
  
  using namespace castor::tape::format;
  //-------------------------------------------------------------------------------  
  ifstream file;   
  ofstream fileout;
  int      payloadSize;
  bool     first;
 
  ALB0100Unmarshaller                       unmarshallObj;
  const ALB0100Unmarshaller::Header        *myHeader;
  const ALB0100Unmarshaller::ALB0100Header *myALB0100Header;

  //-------------------------------------------------------------------------------  
  Array_auto_ptr<char> buffer(new char[BLOCK_SIZE]);
  payloadSize =        BLOCK_SIZE-HEADER_SIZE;
  first =              true; // to specify 'first block of a file'
    
  //-------------------------------------------------------------------------------  
  file.open("./TXTfiles/fileout.txt"); // open as text file
  if(!file) { // file couldn't be opened
    cerr << "Error: FILES OUT's file could not be opened" << endl;
    exit(1);
  }    

  file.read(buffer.get(), BLOCK_SIZE);
  unmarshallObj.readVersion(buffer.get());

  while(! file.eof() ){
     
    try{ 
      myHeader = unmarshallObj.unmarshallHeader(buffer.get(), first);
    }catch(castor::exception::Exception ex ) {
      std::cout << ex.getMessage() << "Failed to call 'unmarshallObj.unmarshallHeader'"<< std::endl;
      break;
    }
    try{
      myALB0100Header = dynamic_cast<const ALB0100Unmarshaller::ALB0100Header*>(myHeader);
    }catch(std::bad_cast e ) {
      std::cout<<e.what() << "Failed to dynamic_cast in call 'unmarshallObj.unmarshallHeader'"<<std::endl;
      break;
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
	
      fileout.write(buffer.get()+HEADER_SIZE, myALB0100Header->file_size - myALB0100Header->file_block_count);
      fileout.close();
      if(fileout.fail()){fileout.clear();} 
      first = true;	  
    }
    else{// Is not the last block of the unmarshalling file

      first = false;
      fileout.write(buffer.get()+HEADER_SIZE, payloadSize);
    }
   
    file.read(buffer.get(), BLOCK_SIZE);
    
  }// end while file

  file.close();
  if(file.fail()){file.clear();} 
  
  return (EXIT_SUCCESS);
}
