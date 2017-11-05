#include "pfm.h"
#include<iostream>
#include<stdio.h>
#include<unistd.h>


PagedFileManager* PagedFileManager::_pf_manager = 0;

PagedFileManager* PagedFileManager::instance()
{
    if(!_pf_manager)
        _pf_manager = new PagedFileManager();

    return _pf_manager;
}


PagedFileManager::PagedFileManager()
{

}


PagedFileManager::~PagedFileManager()
{
}


RC PagedFileManager::createFile(const string &fileName)
{
	//see if this file already exists
	if(exists(fileName))
	{
		return -1;
	}

	//if this file not exist, create this file
	FILE *file = fopen(fileName.c_str(),"wb");
	if(file==NULL){
		cout << "Fail to create file: " << fileName << endl;
		return -1;
	}
	//close this file to avoid writing error
	int pos = fclose(file);
		if(pos)
		{
			cout << "fail to close file" << endl;
			return -1;
		}
	return 0;
}


RC PagedFileManager::destroyFile(const string &fileName)
{
	//see if this file already exists
	if(!exists(fileName))
	{
		return -1;
	}
	remove(fileName.c_str());
	if(exists(fileName))
	{
		cout << "fail to remove file :" << fileName << endl;
		return -1;
	}

    return 0;
}


RC PagedFileManager::openFile(const string &fileName, FileHandle &fileHandle)
{
	//see if this file exist
	if(!exists(fileName))
	{
		perror("file do not exists");
		return -1;
	}

//	//Nov added
//	if(access(fileName.c_str(),R_OK) != -1){
//		perror("this file already open");
//		return -1;
//	}

	// open file and then check
	fileHandle.file = fopen(fileName.c_str(),"rb+wb");
	//check the authority of the open file

	if(fileHandle.file == NULL){
		cout << "fail to open file: " << fileName << endl;
		return -1;
	}

	//check if the feedback fileHandle is already opened

	// check if the feedback fileHandle bind with other file
	fileHandle.setfileName(fileName);

	//get the page Counter
	fseek(fileHandle.file,0,SEEK_SET);
	fread(&(fileHandle.readPageCounter),sizeof(int),1,fileHandle.file);
	fseek(fileHandle.file,4,SEEK_SET);
	fread(&(fileHandle.writePageCounter),sizeof(int),1,fileHandle.file);
	fseek(fileHandle.file,8,SEEK_SET);
	fread(&(fileHandle.appendPageCounter),sizeof(int),1,fileHandle.file);
    return 0;
}


RC PagedFileManager::closeFile(FileHandle &fileHandle)
{
	//close the file by FileHandle::getfilePtr and then check
	string fileName = fileHandle.getfileName();
	//update file operation counter
	fseek(fileHandle.file,0,SEEK_SET);
	fwrite(&(fileHandle.readPageCounter),sizeof(int),1,fileHandle.file);
	fseek(fileHandle.file,4,SEEK_SET);
	fwrite(&(fileHandle.writePageCounter),sizeof(int),1,fileHandle.file);
	fseek(fileHandle.file,8,SEEK_SET);
	fwrite(&(fileHandle.appendPageCounter),sizeof(int),1,fileHandle.file);

	int pos = fclose(fileHandle.file);
	if(pos)
	{
		cout << "fail to close file" << endl;
		return -1;
	}
	//undo the connection between file and fileHandle
//	fileHandle.setfilePtr(NULL);

    return 0;
}


FileHandle::FileHandle()
{
    readPageCounter = 0;
    writePageCounter = 0;
    appendPageCounter = 0;
}


FileHandle::~FileHandle()
{
}


RC FileHandle::readPage(PageNum pageNum, void *data)
{
	//check if this page exist
	PageNum maxPage = getNumberOfPages();
	if(pageNum >= maxPage){
		perror("This page do not exist readPage");
		return -1;
	}

	//set file position to the start position of this page
	int pos = fseek(this->file,pageNum*PAGE_SIZE+12,SEEK_SET);
	if(pos){
				cout << "seek failed! read part" <<endl;
				return -1;
			}

	//use fread to read data from this page
	fread(data,sizeof(byte),PAGE_SIZE,this->file);
	if(ferror(this->file)){
			cout << "read failed!" <<endl;
			return -1;
		}

	//update readPageCounter
	readPageCounter ++;
    return 0;
}


RC FileHandle::writePage(PageNum pageNum, const void *data)
{
	//check if this page exist
	PageNum maxPage = getNumberOfPages();
	if(pageNum+1 > maxPage)
	{
		perror("this page do not exists");
		return -1;
	}

	//set file position to the start position of this page
	int pos = fseek(this->file,pageNum*PAGE_SIZE+12,SEEK_SET);
	if(pos){
				cout << "seek failed! write part" <<endl;
				return -1;
			}
	//use fwrite to write data to this page
	fwrite(data,sizeof(byte),PAGE_SIZE,this->file);
	if(ferror(this->file)){
		cout << "write failed!" <<endl;
		return -1;
	}

//	fflush(file);
	writePageCounter ++;
    return 0;
}


RC FileHandle::appendPage(const void *data)
{
	if(this->file == NULL){
		cout << "fail to get file! append part" << endl;
		return -1;
	}
	//check if this is the first page
	PageNum pageNum = getNumberOfPages();
	if(pageNum==0){
		//set file position to the start
		fseek(this->file,0,SEEK_SET);
		if(ferror(this->file)){
			cout << "seek failed! append part" <<endl;
			return -1;
		}
		//initial page counter number
		fseek(this->file,0,SEEK_SET);
		fwrite(&readPageCounter,sizeof(int),1,this->file);
		fseek(this->file,4,SEEK_SET);
		fwrite(&writePageCounter,sizeof(int),1,this->file);
		fseek(this->file,8,SEEK_SET);
		fwrite(&appendPageCounter,sizeof(int),1,this->file);
	}
	//set file position to the end
	fseek(this->file,0,SEEK_END);
//	cout << "position is :" << ftell(file) << endl;
	if(ferror(this->file)){
			cout << "seek failed! append part" <<endl;
			return -1;
	}
	//write the data to the new page
	fwrite(data,sizeof(byte),PAGE_SIZE,this->file) ;
//	fflush(file);
	if(ferror(this->file)){
			cout << "append failed!" <<endl;
			return -1;
	}

	//get appendPageNumber and update
	appendPageCounter++;
    return 0;

}


unsigned FileHandle::getNumberOfPages()
{
	//use fseek put the file position to the end
	fseek(this->file,0,SEEK_END);

	//use ftell to get the file position
	long endpos = ftell(this->file);
	if(endpos==0) return 0;

	//caculate the page num by endpos-offset / pagesize
	PageNum pageNum = (endpos-12) / PAGE_SIZE;

    return pageNum;
}


RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
	readPageCount = readPageCounter;
	writePageCount = writePageCounter;
	appendPageCount = appendPageCounter;
    return 0;
}

string FileHandle::getfileName()
{
	return fileName;
}
RC FileHandle::setfileName(const string &fileName)
{
	FileHandle::fileName = fileName;
	return 0;
}

inline bool exists(const string &fileName)
{
	return (access(fileName.c_str(),F_OK) != -1);
}
