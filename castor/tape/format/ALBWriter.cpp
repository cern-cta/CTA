
#include "castor/tape/format/ALB0100Marshaller.hpp"

#include <sys/stat.h>
#include <fstream>
#include <iostream>

using std::ifstream;
using std::ofstream;
using std::cerr;
using std::cout;
using std::endl;
using std::ios;


const size_t MAX_FILE_NAME_LEN =         1024;



// The program open a file containing a list of files delimitated by "\n".
// Read each of them as binary file per block (256Bytes) and copy the blocks
// into a single output file.
int main(int argc, char** argv) {

  using namespace castor::tape::format;    

  ifstream    indata, filelist;
  ofstream    fout;  
  char       *buffer;
  int         payloadSize;    
  long        size;
  struct stat results;         // Need for 'stat' command

  ALB0100Marshaller             blockObject;
  ALB0100Marshaller::MigHeader  mig;
  ALB0100Marshaller::FileHeader file;
    
  //-------------------------------------------------------------------------------
  file.file_name =   new char[MAX_FILE_NAME_LEN];
  buffer =           new char[BLOCK_SIZE];  // Header + payload
  payloadSize =      BLOCK_SIZE-HEADER_SIZE;// 255 KiB
    
  //=== define migration datails ==================================================  
  mig.header_size =     HEADER_SIZE;
  mig.tape_mark_count = 777;
  mig.block_size =      262144;
  mig.block_count =     0;
  mig.stager_host=      "----------------------------X----------c2cmsstager.cern.ch";
  mig.drive_host=       "------------X-------------tpsrv250.cern.ch";
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

    filelist.getline(file.file_name, MAX_FILE_NAME_LEN );      // Read file name form filelist

    if(filelist.eof() ) { break;}	
    if((filelist.rdstate() & ifstream::failbit ) != 0 ){
      if((uint)filelist.gcount() == MAX_FILE_NAME_LEN-1){
	cerr<< "ERROR: File_name readed from filelist.txt longer then "<<MAX_FILE_NAME_LEN<<" characters."<<endl
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
            
    // fill up the file specific data
    //====================================================
    {
      FILE *fp;// code to generate Adler32 checksum runtime
      char s[alb0100::FILE_NAME_LEN]="adler32 ";
      strcat (s,file.file_name);
      strcat (s," 2>&1");
      fp = popen(s, "r");// 2>&1 reditect output from stder  to  stdio
      char  strA[100];
      fscanf (fp, "%s %s %10d",strA, strA ,&file.file_checksum); // How to get the Third 'string'
      pclose(fp);
      file.file_ns_host= "castorns.cern.ch";
      file.file_ns_id = 226994274;
    }

    blockObject.startFile(file);
	    
    size = file.file_size;
   
    while ( !indata.eof() ){
                
      if (size < payloadSize-1){                  // !!! LAST FILE BLOCK !!!
	memset(buffer+HEADER_SIZE, '0', payloadSize-1);
	memset(buffer+HEADER_SIZE+payloadSize-1, '\n', 1);
	indata.read(buffer+HEADER_SIZE, size+1);  // remove to have 256 blocks
	blockObject.marshall(buffer); 	    // compute the header
	fout.write(buffer, BLOCK_SIZE);           // write the output on a file
      }
      else {
	size -= payloadSize;
	indata.read(buffer+HEADER_SIZE, payloadSize);
	blockObject.marshall(buffer); 	    // compute the header
	fout.write(buffer,BLOCK_SIZE );	    // write the output on a file
      }
    }// end of while ( !indata.eof() )
    indata.close();
    if(indata.fail()){ indata.clear();} 	    // in case of .close() failure it clear the variable
                
  }// end while( !filelist.eof() )
    
  fout.close();
  if(fout.fail()){ fout.clear();}    	            // in case of .close() failure it clear the variable

  filelist.close();
  if(filelist.fail()){ filelist.clear();} 	    // in case of .close() failure it clear the variable
 
  delete[] buffer;
  delete[] file.file_name;
    
  return (EXIT_SUCCESS);
}
