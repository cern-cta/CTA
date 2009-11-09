
#include "castor/tape/format/ALB0100Marshaller.hpp"
#include <string.h>

#include <sys/stat.h>
#include <fstream>
#include <iostream>
#include <errno.h>
#include <stdlib.h>
#include <memory>

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
 * Function to copy a strig (char*) to another strig (char*)
 * of maximun lenght <= dstLen.
 */
//==============================================================================
void copyString(const char *src, char *dst, size_t dstLen)
  throw(castor::exception::Exception) {
  if(src == NULL) {
    castor::exception::Exception ex(EINVAL);
    ex.getMessage() << "Source variable = NULL";
    throw ex;
  }

  if(dstLen < strlen(src)) {
    castor::exception::Exception ex(EINVAL);
    ex.getMessage() << "Destination variable too small";
    throw ex;
  }

  strcpy(dst, src);
}

/**
 * Template function to copy a strig (char*) to a char array.
 * Automatic trasformation from array to pointer ("decay").
 */
//==============================================================================
template< int n > 
void copyString(const char *src, char (&dst)[n])
  throw(castor::exception::Exception) {
  copyString(src, dst, n);
}


/**
 * The program open a file containing a list of files delimitated by "\n".
 * Read each of them as binary file per block (256Bytes) and copy the blocks
 * into a single output file.
 */
//==============================================================================
int main(int argc, char** argv) {

  using namespace castor::tape::format;    

  ifstream    indata, filelist;
  ofstream    fout;  
  int         payloadSize;    
  long        size;
  struct stat results;         // Need for 'stat' command

  ALB0100Marshaller             blockObject;
  ALB0100Marshaller::MigHeader  mig;
  ALB0100Marshaller::FileHeader file;
  
  Array_auto_ptr<char> buffer(new char[BLOCK_SIZE]); 
  payloadSize =       BLOCK_SIZE-HEADER_SIZE;
    
  //=== define migration datails ==================================================  
  mig.header_size =     HEADER_SIZE;
  mig.tape_mark_count = 777;
  mig.block_size =      262144;
  mig.block_count =     0;
  copyString("X----------c2cmsstager.cern.ch", mig.stager_host);
  copyString("X-------------tpsrv250.cern.ch", mig.drive_host);
  strcpy(mig.version_number,     "2.99");
  strcpy(mig.checksum_algorithm, "Adler-32");
  strcpy(mig.stager_version,     "2.7.1.18");
  strcpy(mig.drive_name,         "3592028");
  strcpy(mig.drive_serial,       "465000001642");
  strcpy(mig.drive_firmware,     "D3I0_C90");
  strcpy(mig.vol_density,        "700.00GB");
  strcpy(mig.vol_id,             "T02694");
  strcpy(mig.vol_serial,         "T02694");
  strcpy(mig.device_group_name,  "3592B1");
  //==============================================================================

  filelist.open("./TXTfiles/filelist.txt"); // open as text file
  if(!filelist) { // file couldn't be opened
    cerr << "Error: FILES LIST's file could not be opened" << endl;
    exit(1);
  }
    
  fout.open("./TXTfiles/fileout.txt", ios::binary | ios::out);  // create/open output file as binary file
  if(!fout) { // file couldn't be opened
    cerr << "Error: OUTPUT file could not be opened" << endl;
    filelist.close();
    exit(1);
  }

  blockObject.startMigration(mig);    // Start migration file
    
  while( !filelist.eof() ){

    filelist.getline(file.file_name, sizeof(file.file_name));      // Read file name form filelist

    if(filelist.eof() ) { break;}	
    if((filelist.rdstate() & ifstream::failbit ) != 0 ){
      if((uint)filelist.gcount() == sizeof(file.file_name)-1){
	cerr<< "ERROR: File_name readed from filelist.txt longer then "<<sizeof(file.file_name)<<" characters."<<endl
	    << "Line exceded: "<< file.file_name<<"..."<<endl;
      }
      else{
	cout<< "ERROR: Error during reading LILELIST.TXT."<<endl;
      }
      break;
    }

    stat(file.file_name, &results);
    file.file_size = results.st_size;   // The size of the file in bytes is in results.st_size
                 	    
    indata.open(file.file_name, ios::binary | ios::in); // open file to read as a binary file
    if(!indata) { // file couldn't be opened
      cerr << "Error: INPUT file could not be opened. file: "<< file.file_name << endl;
      //exit(1);
      break;
    }
            
    //==fill up the file specific data====================
    {
      FILE *fp;// code to generate Adler32 checksum runtime
      char s[alb0100::FILE_NAME_LEN]="adler32 ";
      strcat (s,file.file_name);
      strcat (s," 2>&1");
      fp = popen(s, "r");// 2>&1 reditect output from stder  to  stdio
      char  strA[100];
      fscanf (fp, "%s %s %10ud",strA, strA ,&file.file_checksum); // How to get the Third 'string'
      pclose(fp);
      copyString("castorns.cern.ch", file.file_ns_host);
      file.file_ns_id = 226994274;
    }
    //====================================================

    blockObject.startFile(file);
	    
    size = file.file_size;
   
    while ( !indata.eof() ){
                
      if (size < payloadSize-1){                  // !!! LAST FILE BLOCK !!!
	memset(buffer.get()+HEADER_SIZE, '0', payloadSize-1);
	memset(buffer.get()+HEADER_SIZE+payloadSize-1, '\n', 1);
	indata.read(buffer.get()+HEADER_SIZE, size+1);  // remove to have 256 blocks
	blockObject.marshall(buffer.get()); 	    // compute the header
	fout.write(buffer.get(), BLOCK_SIZE);           // write the output on a file
      }
      else {
	size -= payloadSize;
	indata.read(buffer.get()+HEADER_SIZE, payloadSize);
	blockObject.marshall(buffer.get()); 	    // compute the header
	fout.write(buffer.get(),BLOCK_SIZE );	    // write the output on a file
      }
    }// end of while ( !indata.eof() )
    indata.close();
    if(indata.fail()){ indata.clear();} 	    // in case of .close() failure it clear the variable
                
  }// end while( !filelist.eof() )
    
  fout.close();
  if(fout.fail()){ fout.clear();}    	            // in case of .close() failure it clear the variable

  filelist.close();
  if(filelist.fail()){ filelist.clear();} 	    // in case of .close() failure it clear the variable
    
  return (EXIT_SUCCESS);
}
