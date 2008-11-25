
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


// The program open a file containing a list of files delimitated by "\n".
// Read each of them as binary file per block (256Bytes) and copy the blocks
// into a single output file.
int main(int argc, char** argv) {

    using namespace castor::tape::format;    

    ifstream indata, filelist;
    ofstream fout;  
    char    *buffer;
    int      buffLength, 
      payloadOffset, 
      file_name_length, 
      filename_size, 
      payloadSize;    
    long     size;
    struct stat results;         // Need for 'stat' command
    char    *ch;

    ALB0100Marshaller blockObject;

    ALB0100Marshaller::MigHeader  mig;
    ALB0100Marshaller::FileHeader file;
    
    //-------------------------------------------------------------------------------
    buffLength =       BLOCK_SIZE;            // 256 KiB
    payloadOffset =    HEADER_SIZE;           // 1 KiB
    buffer =           new char[buffLength];  // Header + payload 256 KiB
    file_name_length = 1000*sizeof(char);
    payloadSize =      BLOCK_SIZE-HEADER_SIZE;// 255 KiB
    
    //=== define migration datails =========================  
    strcpy(mig.version_number, "2.99");
    mig.header_size = 1024;
    strcpy(mig.checksum_algorithm, "Adler-32");
    mig.tape_mark_count = 777;
    mig.block_size = 262144;
    mig.block_count = 0;
    strcpy(mig.stager_version, "2.7.1.18");
    ch =  "----------------------------X----------c2cmsstager.cern.ch";
    mig.stager_host = ch; //(char *)
    strcpy(mig.drive_name, "3592028");
    strcpy(mig.drive_serial, "465000001642");
    strcpy(mig.drive_firmware, "D3I0_C90");
    ch = "------------X-------------tpsrv250.cern.ch";
    mig.drive_host = ch; //(char *)
    strcpy(mig.vol_density, "700.00GB");
    strcpy(mig.vol_id, "T02694");
    strcpy(mig.vol_serial, "T02694");
    strcpy(mig.device_group_name, "3592B1");
    //=======================================================
    
    filelist.open("./TXTfiles/filelist.txt"); // open as text file
    if(!filelist) { // file couldn't be opened
        cerr << "Error: FILES LIST's file could not be opened" << endl;
        exit(1);
    }
    
    // create/open output file
    fout.open("./TXTfiles/fileout.txt", ios::binary | ios::out); // open as binary file
    if(!fout) { // file couldn't be opened
        cerr << "Error: OUTPUT file could not be opened" << endl;
        exit(1);
    }
    
    // start migration file
    blockObject.startMigration(mig);
    
    while(! filelist.eof() ){
        // Read file name form filelist
        memset(file.file_name, 0 , file_name_length);
        filelist.getline(file.file_name, 1000 );// delimited defaults to '\n'
	filename_size = strlen(file.file_name);
        if (filename_size  > 10){// && (stat(filename, &results) != 0)){
	    
	    stat(file.file_name, &results);
            file.file_size = results.st_size;   // The size of the file in bytes is in results.st_size
	                        	    
            indata.open(file.file_name, ios::binary | ios::in); // open file to read as a binary file
            if(!indata) { // file couldn't be opened
                cerr << "Error: INPUT file could not be opened. file: "<< file.file_name << endl;
                exit(1);
            }
            
            // fill up the file specific data
	    //====================================================

	    FILE *fp;// code to generate Adler32 checksum runtime
	    char s[alb0100::FILE_NAME_LEN]="adler32 ";
	    strcat (s,file.file_name);
	    strcat (s," 2>&1");
	    fp = popen(s, "r");// 2>&1 reditect output from stder  to  stdio
	    char  strA[100];
	    fscanf (fp, "%s %s %10d",strA, strA ,&file.file_checksum); // How to get the Third 'string'
	    pclose(fp);
	    
            file.file_ns_host = "castorns.cern.ch";
            file.file_ns_id = 226994274;
	    blockObject.startFile(file);
	    
	    size = file.file_size;
   
            while (! indata.eof() ){
                
                if (size < payloadSize-1){ // (-1) is to count the efo character
                    //padding the buffer with '0' + '\n'  !!! LAST FILE BLOCK !!!
                    memset(buffer+payloadOffset, '0', payloadSize-1);
		    memset(buffer+payloadOffset+payloadSize-1, '\n', 1);
                    indata.read(buffer+payloadOffset, size+1);  // remove to have 256 blocks
                    blockObject.marshall(buffer); 		// compute the header
                    fout.write(buffer, buffLength);       	// write the output on a file

                }
                else {
                    size -= payloadSize;
                    indata.read(buffer+payloadOffset, payloadSize);
                    blockObject.marshall(buffer); 		// compute the header
                    fout.write(buffer, buffLength);		// write the output on a file

               }
            }// end of while
        indata.close();
	if(indata.fail()){ indata.clear();} 	// in case of .close() failure it clear the variable
        }
                
    } // end while filelist
    
    fout.close();
    filelist.close();
    delete[] buffer;
    
    return (EXIT_SUCCESS);
}

