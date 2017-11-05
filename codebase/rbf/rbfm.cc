#include "rbfm.h"
#include "pfm.h"
#include<iostream>
#include<stdio.h>
#include<unistd.h>
#include<math.h>
#include <stdexcept>
#include<cstring>

const int success = 0;

RecordBasedFileManager* RecordBasedFileManager::_rbf_manager = 0;
PagedFileManager* RecordBasedFileManager::_pf_manager = PagedFileManager::instance();


//Record first come with RID(8 bytes), then take 4 bytes store field number, and take ceil((float)n/8) bytes to store nullsIndicator
//next store offset Info(4 * n bytes), finally according to the attribute store fields Info.
//skip the RID part

unsigned getRecord(const vector<Attribute> &recordDescriptor,const void *data ,const void* buffer){
	int offset=8;
	unsigned fieldNum = 0;
//	RID rid;
	unsigned recordSize = 0;

	int bytesForField;

	//get and store field number offset = 8 , then offset + 4 =12 ;
	fieldNum = recordDescriptor.size();
	memcpy((char *)buffer+offset,&fieldNum,sizeof(unsigned));
	offset += sizeof(unsigned);

	//offset indicator for fields
	int offsetIndicator[fieldNum];
	offset += fieldNum * sizeof(int);
	//caculate bytes needed to store nullsFieldIndicator
	bytesForField = ceil((double)fieldNum/8);

	//copy nullFieldsIndicator and offset + bytesForField
	memcpy((char *)buffer + offset, data, bytesForField);
	offset += bytesForField;

	//create nullsIndicator to help check loop
	unsigned char *nullsIndicator = (unsigned char *) malloc(bytesForField);
	memset(nullsIndicator,0,bytesForField);
	memcpy(nullsIndicator,data,bytesForField);

	//reserve space for offsetIndicator
//	offset += fieldNum * 4;

	int dataOffset = bytesForField;
	//according to recordDescriptor, get data and write to buffer
	int round;
	bool nullBit = false;
	for(int i=0; i < fieldNum ; i++)
	{
		round = i / 8;
		nullBit = nullsIndicator[round]&(1<<(7-(i%8)));
		if(nullBit){
			//version 2
			offsetIndicator[i] = -1;
		}
		else{
			switch (recordDescriptor[i].type){
				case TypeInt :{
					memcpy((char *)buffer+offset,(char *)data+dataOffset,sizeof(int));
					offsetIndicator[i] = offset;
					offset += recordDescriptor[i].length;
					dataOffset += recordDescriptor[i].length;
					break;
				}
				case TypeReal:{
					memcpy((char *)buffer+offset,(char *)data+dataOffset,recordDescriptor[i].length);
					offsetIndicator[i] = offset;
					offset += recordDescriptor[i].length;
					dataOffset += recordDescriptor[i].length;
					break;
				}
				case TypeVarChar:{
					memcpy((char *)buffer+offset,(char *)data+dataOffset,sizeof(int));
					int nameLength;
					memcpy(&nameLength,(char *)data+dataOffset,sizeof(int));
					offsetIndicator[i] = offset;
					offset += sizeof(int);
					dataOffset += sizeof(int);
					memcpy((char *)buffer+offset,(char *)data+dataOffset,nameLength);

					offset += nameLength;
					dataOffset+=nameLength;
					break;
				}
			}
		}
	}

	memcpy((char*)buffer+3*sizeof(unsigned),(char*)offsetIndicator,fieldNum*sizeof(int));
	recordSize = offset;
	free(nullsIndicator);
	return recordSize;
}

RecordBasedFileManager* RecordBasedFileManager::instance()
{
    if(!_rbf_manager)
        _rbf_manager = new RecordBasedFileManager();

    return _rbf_manager;
}

RecordBasedFileManager::RecordBasedFileManager()
{
}

RecordBasedFileManager::~RecordBasedFileManager()
{
}

RC RecordBasedFileManager::createFile(const string &fileName) {
	//use PagedFileManager method createFile to create file
	if(_pf_manager->createFile(fileName) != success){
		perror("Fail to create file!");
		return -1;
	}
    return 0;
}

RC RecordBasedFileManager::destroyFile(const string &fileName) {
	//use PagedfileManager method to destroy file
	if(_pf_manager->destroyFile(fileName) != success){
			perror("Fail to destroy file!");
			return -1;
		}
	return 0;
}

RC RecordBasedFileManager::openFile(const string &fileName, FileHandle &fileHandle) {

	//use PagedfileManager method to open file
	if(_pf_manager->openFile(fileName,fileHandle) != success){
			perror("Fail to open file!");
			return -1;
		}
	return 0;
}

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
	//use PagedfileManager method to close file
	if(_pf_manager->closeFile(fileHandle) != success){
			perror("Fail to close file!");
			return -1;
		}
	return 0;
}

RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid) {
	//create record ; note:malloc memory for buffer
	char *buffer = (char *)malloc(PAGE_SIZE);
	memset(buffer,0,PAGE_SIZE);
	if(!buffer){
		cout << "fail to malloc buffer , line 160" << buffer << endl;
	}
	unsigned recordSize = getRecord(recordDescriptor,data,buffer);

	//create a buffer for a page to temporary store until write into file
	char* pageBuffer =(char *) malloc(PAGE_SIZE);
	memset(pageBuffer,0,PAGE_SIZE);
	if(!pageBuffer){
		cout << "fail to malloc pageBuffer , line 171" << buffer << endl;
	}

	//insert package record
	insertPackagedRecord(fileHandle,pageBuffer,buffer,rid,recordSize,INSERT);

	//free all the space
	free(buffer);
	free(pageBuffer);

    return 0;
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {
	unsigned pageNum = fileHandle.getNumberOfPages();

	if(rid.pageNum>=pageNum)
	{
		cout << "record not found. line 185 " << endl;
		return 0;
	}

	char *pageBuffer = (char *)malloc(PAGE_SIZE);

	fileHandle.readPage(rid.pageNum,pageBuffer);

	//get slotNum
	unsigned slotNum;
	memcpy(&slotNum,(char *)pageBuffer+PAGE_SIZE-sizeof(unsigned)*2 ,sizeof(unsigned));

	if(rid.slotNum>slotNum){
		cout << "record not found. line 198" << endl;
		free(pageBuffer);
		return -1;
	}


	//check if this page is the record current location,if not, find the current location
	//prepare the variable for get the current location
	Slot slot;
	RID currentPointer=rid;
	RID nextPointer = {0,0} ;
	//testInfo
	//cout<<rid.slotNum<<endl;
	//using while loop to get the current location and load it into pageBuffer
	while(true){
		slot = getRIDInfo(pageBuffer,currentPointer);
		if(slot.offset==-1){
			free(pageBuffer);
			return -1;
		}
		if(slot.length!=8) break;
		memcpy(&nextPointer,(char*)pageBuffer+slot.offset,slot.length);
		currentPointer = nextPointer;
		fileHandle.readPage(currentPointer.pageNum,pageBuffer);
	};



	//get field number
	unsigned fieldNum;
	memcpy(&fieldNum,(char *)pageBuffer+slot.offset+2*sizeof(unsigned),sizeof(unsigned));

	memcpy(data,(char *)pageBuffer+slot.offset+sizeof(unsigned)*3+sizeof(int)*fieldNum,slot.length-sizeof(unsigned)*3-sizeof(int)*fieldNum);

	free(pageBuffer);
    return 0;
}

RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data) {
	//get field Number
	unsigned fieldNum;
	fieldNum = recordDescriptor.size();
	int offset = 0;
	//get nullsIndicator
	int bytesForField = ceil((double)fieldNum/8);
	unsigned char *nullsIndicator = (unsigned char *) malloc(bytesForField);
	if(!nullsIndicator){
		cout << "fail to malloc nullsIndicator Line 321" << endl;
	}
	memset(nullsIndicator,0,bytesForField);
	memcpy(nullsIndicator,(char *)data+offset,bytesForField);
	offset += bytesForField;
	int round;
	bool nullBit=false ;
	int value;
	float fvalue;
	int nameLength;
	for(int i=0; i < fieldNum ; i++)
	{
		round = i / 8;
		nullBit = nullsIndicator[round]&(1<<(7-(i%8)));
		if(nullBit){
			cout << recordDescriptor[i].name <<'\t'<< ":" << "Null" << '\t' ;
		}
		else{
			switch (recordDescriptor[i].type){
				case TypeInt :{

					memcpy(&value,(char *)data+offset,recordDescriptor[i].length);
					offset += sizeof(int);
					cout << recordDescriptor[i].name << ":" << value << '\t';
					break;
				}
				case TypeReal:{

					memcpy(&fvalue,(char *)data+offset,recordDescriptor[i].length);
					offset += sizeof(float);
					cout << recordDescriptor[i].name << ":" << fvalue << '\t';
					break;
				}
				case TypeVarChar:{

					memcpy(&nameLength,(char *)data+offset,sizeof(int));
					offset += sizeof(int);
					char name[nameLength+1];

					memcpy(name , (char *)data+offset,nameLength);
					name[nameLength] = '\0';
					string str(name);
					offset += nameLength;
					cout << recordDescriptor[i].name << ":" << str <<'\t';
					break;
				}
			}
		}
	}
	free(nullsIndicator);
    return 0;
}

PageNum RecordBasedFileManager::searchInsertPage(const unsigned recordSize,FileHandle &fileHandle)
{
	//search for a page has enough space ,if find return pageNum,if not return
	PageNum maxPage = fileHandle.getNumberOfPages();
	unsigned freeSpace;
	char *dataBuffer = (char *)malloc(PAGE_SIZE);
	for(PageNum i=0;i<maxPage;i++){
		fileHandle.readPage(i,dataBuffer);
		memcpy(&freeSpace,(char *)dataBuffer+PAGE_SIZE-sizeof(unsigned),sizeof(unsigned));
		if(freeSpace>=recordSize+sizeof(Slot)){
			free(dataBuffer);
			return i;
		}
	}
	free(dataBuffer);
//	cout << "need a new page" << endl;
	return maxPage;
}

RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid){
    //search orderly and store the next slot, then delete the last slot

	//create record ; note:malloc memory for buffer
	char *pageBuffer = (char *)malloc(PAGE_SIZE);
	if(fileHandle.readPage(rid.pageNum,pageBuffer)){
		perror("read page failed in delete record");
		return -1;
	}
	//prepare variable
	Slot slot;
	RID currentPointer = rid;
	RID nextPointer;
	while(true){
		slot = getRIDInfo(pageBuffer,currentPointer);
		if(slot.offset==-1) {
			perror( "This record do not exist! delete record failed");
			free(pageBuffer);
			return -1;
		}
		if(slot.length!=8) {
			recordMoveByOffset(pageBuffer,currentPointer,0,DELETE);
			fileHandle.writePage(currentPointer.pageNum,pageBuffer);
			free(pageBuffer);

			//testInfo
//			cout<<endl<<"before return value in delete record"<<endl;
//			cout<<endl<<"before return value slot offset is======= "<<slot.offset<<endl;
//			cout<<endl<<"before return value slot length is======= "<<slot.length<<endl;
//			cout<<endl<<"before return value current slot number is======= "<<currentPointer.slotNum<<endl;


			return 0;
		}
		else {
			//get next Pointer by check the slot
			memcpy(&nextPointer,(char*)pageBuffer+slot.offset,sizeof(RID));
			//delete current pointer info
			recordMoveByOffset(pageBuffer,currentPointer,0,DELETE);
			fileHandle.writePage(currentPointer.pageNum,pageBuffer);
			fileHandle.readPage(nextPointer.pageNum,pageBuffer);
			currentPointer = nextPointer;
		}
	}
}

RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, const RID &rid){

	//package the record by using function getRecord
	//create a buffer to get the new record
	char* buffer = (char*)malloc(PAGE_SIZE);
	memset(buffer,0,PAGE_SIZE);
	if(!buffer){
		cout << "fail to malloc buffer , line 445" << buffer << endl;
		return -1;
	}
	//using getRecord function to get the new record
	unsigned recordSize = getRecord(recordDescriptor,data,buffer);
	//write RID info into buffer
	memcpy((char *)buffer,&(rid),2*sizeof(unsigned));

	//search to get the current location by RID
	//create a page buffer to store read page info
	char* pageBuffer = (char *)malloc(PAGE_SIZE);
	fileHandle.readPage(rid.pageNum,pageBuffer);
	//prepare the variable for get the current location
	Slot slot;
	RID currentPointer=rid;
	RID nextPointer = {0,0} ;

	//using while loop to get the current location and load it into pageBuffer
	while(true){
		slot = getRIDInfo(pageBuffer,currentPointer);
		if(slot.offset==-1){
			cout << "This record do not exist! " << endl;
			//free all allocated space
			free(buffer);
			free(pageBuffer);

			return -1;
		}
		if(slot.length!=8) break;
		memcpy(&nextPointer,(char*)pageBuffer+slot.offset,slot.length);
		currentPointer = nextPointer;
		fileHandle.readPage(currentPointer.pageNum,pageBuffer);
	};

	//check if this page have enough freespace
	unsigned freespace = getPageInfo(pageBuffer,FREESPACE);

	//get the change between new record size and old record size
	int spaceDiff = recordSize - slot.length;

	if( spaceDiff>freespace ){
		//if current free space is not enough to store the new record
		//search for page which have enough free space then store the new record
		PageNum insertPageNum = searchInsertPage(recordSize,fileHandle);


		char* nextPageBuffer = (char *)malloc(PAGE_SIZE);
		memset(nextPageBuffer,0,PAGE_SIZE);
		insertPackagedRecord(fileHandle,nextPageBuffer,buffer,nextPointer,recordSize,UPDATE);
		fileHandle.writePage(nextPointer.pageNum,nextPageBuffer);

		//update last page and then point to newest location
		recordMoveByOffset(pageBuffer,currentPointer,sizeof(RID),UPDATE);
		//store the newest record location
		memcpy((char*)pageBuffer+slot.offset,&(nextPointer),sizeof(RID));
		fileHandle.writePage(currentPointer.pageNum,pageBuffer);
		free(nextPageBuffer);
	}
	else{
		//if freespace is enough, insert new record into current page
		recordMoveByOffset(pageBuffer,currentPointer,recordSize,UPDATE);
		memcpy((char*)pageBuffer+slot.offset,buffer,recordSize);
		fileHandle.writePage(currentPointer.pageNum,pageBuffer);
	}

	//free all allocated space
	free(buffer);
	free(pageBuffer);

    return 0;
}


//get rest after the rid record
int RecordBasedFileManager::getRestRecordLengthByRID(void* pageBuffer,const RID &rid){
	const unsigned slotNum = getPageInfo(pageBuffer,SLOTNUM);
	Slot slotDir[slotNum];
	Slot slot = getRIDInfo(pageBuffer,rid);
	memcpy(slotDir,(char*)pageBuffer+PAGE_SIZE-2*sizeof(unsigned)-slotNum*sizeof(Slot),slotNum*sizeof(Slot));

	int len=0;
	for(int i=0;i<slotNum;i++){
		if(slotDir[i].offset>slot.offset) {
			len+=slotDir[i].length;
		}
	}
	return len;
}

//move the record by offset.
//when delete offset is 0, when mode is update offset should be the new record size
RC RecordBasedFileManager::recordMoveByOffset(void* pageBuffer,const RID &rid,const unsigned offset,const int mode){
	unsigned slotNum = getPageInfo(pageBuffer,SLOTNUM);
	Slot slotDir[slotNum];
	memcpy(slotDir,(char*)pageBuffer+PAGE_SIZE-2*sizeof(unsigned)-slotNum*sizeof(Slot),slotNum*sizeof(Slot));
	//get rest record length
	int len = getRestRecordLengthByRID(pageBuffer,rid);

	//get RID corresponding slot
	Slot slot = getRIDInfo(pageBuffer,rid);

	//copy the next space by offset
	memcpy((char*)pageBuffer+slot.offset+offset,(char*)pageBuffer+slot.offset+slot.length,len);


	//if mode is delete
	if(mode==DELETE){
		//update freespace
		unsigned fs = getPageInfo(pageBuffer,FREESPACE);
		fs = fs + slot.length;
		memcpy((char*)pageBuffer+PAGE_SIZE-sizeof(unsigned),&fs,sizeof(unsigned));

		//update slot Directory
		for(int i=0;i<slotNum;i++){
			if(slotDir[i].offset>slot.offset) slotDir[i].offset -= slot.length;
		}
		memcpy((char*)pageBuffer+PAGE_SIZE-2*sizeof(unsigned)-slotNum*sizeof(Slot),slotDir,slotNum*sizeof(Slot));
		//after update the slotDir then update the slot.offset
		slot.offset = -1;
		memcpy((char*)pageBuffer+PAGE_SIZE-2*sizeof(unsigned)-rid.slotNum*sizeof(Slot),&slot,sizeof(Slot));
	}
	//if mode is update
	if(mode==UPDATE){
		//update freespace
		unsigned fs = getPageInfo(pageBuffer,FREESPACE);
		if(offset>slot.length){
			fs = fs - (offset - slot.length);

			//update slot Directory
			for(int i=0;i<slotNum;i++){
				if(slotDir[i].offset>slot.offset) {
					slotDir[i].offset += (offset-slot.length);
				}
			}

		}
		else{
			fs = fs + (slot.length - offset);

			//update slot Directory
			for(int i=0;i<slotNum;i++){
				if(slotDir[i].offset>slot.offset) {
					slotDir[i].offset -= (slot.length - offset);
				}
			}

		}

		memcpy((char*)pageBuffer+PAGE_SIZE-sizeof(unsigned),&fs,sizeof(unsigned));


		memcpy((char*)pageBuffer+PAGE_SIZE-2*sizeof(unsigned)-slotNum*sizeof(Slot),slotDir,slotNum*sizeof(Slot));
		//after update slotDir then update the slot.length
		slot.length = offset;
		memcpy((char *)pageBuffer+PAGE_SIZE-2*sizeof(unsigned)-rid.slotNum*sizeof(Slot),&slot,sizeof(Slot));
	}

	return 0;

}

//get Page information
inline unsigned RecordBasedFileManager::getPageInfo(void* pageBuffer,int choice){
	unsigned info=0;
	memcpy(&info,(char*)pageBuffer+PAGE_SIZE-choice*sizeof(unsigned),sizeof(unsigned));
	return info;
}

//through RID and current page buffer to get slot
inline Slot RecordBasedFileManager::getRIDInfo(void* pageBuffer,const RID &rid){
	Slot slot;
	unsigned slotNum = rid.slotNum;
	memcpy(&slot ,(char*)pageBuffer+PAGE_SIZE-slotNum*sizeof(Slot) - 2*sizeof(unsigned) ,sizeof(Slot));
	return slot;
}

//insert packaged record
RC RecordBasedFileManager::insertPackagedRecord(FileHandle &fileHandle,const void* pageBuffer,const void* buffer,RID &rid,const unsigned recordSize,const int mode){
	//get the current page number
	PageNum pageNum = fileHandle.getNumberOfPages();
	//search for insert page index
	PageNum insertPage = searchInsertPage(recordSize,fileHandle);
	//prepare variable
	unsigned reverseOffset = PAGE_SIZE;

	//excute insert operation
	//1, check if this page already exists
	//2, if not, append a new page; else set pointer to insert page
	//3, find the right position to insert data and update slot directory
	if(insertPage == pageNum){
		//set RID into record RID: 0,1
		unsigned slotNum = 1;
		if(mode==2){
			memcpy((char*)buffer,&insertPage,sizeof(unsigned));
			memcpy((char *)buffer+sizeof(unsigned),&slotNum,sizeof(unsigned));
		}

		memcpy(&(rid.pageNum),&insertPage,sizeof(unsigned));
		memcpy(&(rid.slotNum),&slotNum,sizeof(unsigned));

		//store record Info into pageBuffer
		memcpy((char*)pageBuffer,(char*)buffer,recordSize);

		//store free space Info into pageBuffer
		unsigned freeSpace = PAGE_SIZE - recordSize - sizeof(Slot) - sizeof(unsigned) * 2;
		reverseOffset -= sizeof(unsigned);
		memcpy((char *)pageBuffer+reverseOffset,&freeSpace,sizeof(unsigned));
//		cout << "free space is : " << freeSpace << endl;

		//store Slot Number into pageBuffer
		unsigned slotNumber = 1;
		reverseOffset -= sizeof(unsigned);
		memcpy((char *)pageBuffer+reverseOffset,&slotNumber,sizeof(unsigned));


		//create slotPtr
		Slot slotPtr[0];
		slotPtr[0].offset = 0;
		slotPtr[0].length = recordSize;
		//store Slot Dir pageBuffer
		reverseOffset -= sizeof(Slot);
		memcpy((char *)pageBuffer+reverseOffset,slotPtr,sizeof(Slot));

		//write pageBuffer into file
		fileHandle.appendPage(pageBuffer);

	}
	else{
		//get page data and temporary store in pageBuffer
		fileHandle.readPage(insertPage,(void*)pageBuffer);

		//get slotNumber
		unsigned slotNum ;
		reverseOffset -= 2 * sizeof(unsigned);
		memcpy(&slotNum,(char *) pageBuffer+reverseOffset,sizeof(unsigned));

		//create slotDir 1 - slotNum
		Slot slotDir[slotNum];
		reverseOffset -= sizeof(Slot)*slotNum;
		memcpy(slotDir,(char *)pageBuffer+reverseOffset,slotNum*sizeof(Slot));

		//find the right place by check the offset
		unsigned freespace = getPageInfo((void*)pageBuffer,FREESPACE);
		short maxOffset=PAGE_SIZE-(slotNum+2)*sizeof(unsigned)-freespace;
		unsigned slotIndex=0;
		for(int i=slotNum-1;i>=0;i--){
			if(slotDir[i].offset==-1){
				slotIndex = (unsigned)slotNum-i;

				break;
			}
		}

		//copy the record into the page
		memcpy((char *)pageBuffer+maxOffset,buffer,recordSize);
		//check if needed to add slot number
		if(slotIndex==0){
			slotNum++;
			slotIndex=slotNum;
			reverseOffset -= sizeof(Slot);
			//update slotNum
			memcpy((char *)pageBuffer+PAGE_SIZE-2*sizeof(unsigned),&slotNum,sizeof(unsigned));
		}
		//update slotDir
		Slot slot;
		slot.offset = maxOffset;
		slot.length = recordSize;
		memcpy((char *)pageBuffer+PAGE_SIZE-2*sizeof(unsigned)-slotIndex*sizeof(Slot),&slot,sizeof(Slot));

		//update free space
		freespace = PAGE_SIZE - maxOffset-recordSize- 4*(slotNum+2);
		memcpy((char *)pageBuffer+PAGE_SIZE-sizeof(unsigned),&freespace,sizeof(unsigned));

		//set RID
		if(mode==2){
			memcpy((char *)pageBuffer+maxOffset,&insertPage,sizeof(unsigned));
			memcpy((char *)pageBuffer+maxOffset+sizeof(unsigned),&slotIndex,sizeof(unsigned));
		}
		memcpy(&(rid.pageNum),&insertPage,sizeof(unsigned));
		memcpy(&(rid.slotNum),&slotIndex,sizeof(unsigned));
		//write pageBuffer into file
		fileHandle.writePage(insertPage,pageBuffer);
	}
	return 0;
}

unsigned RecordBasedFileManager::readFullSizeRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {
	unsigned pageNum = fileHandle.getNumberOfPages();

	if(rid.pageNum>=pageNum)
	{
		cout << "record not found. line 663 " << endl;
		return 0;
	}

	char *pageBuffer = (char *)malloc(PAGE_SIZE);

	fileHandle.readPage(rid.pageNum,pageBuffer);

	//get slotNum
	unsigned slotNum;
	memcpy(&slotNum,(char *)pageBuffer+PAGE_SIZE-sizeof(unsigned)*2 ,sizeof(unsigned));

	if(rid.slotNum>slotNum){
		cout << "record not found. line 198" << endl;
		free(pageBuffer);
		return -1;
	}


	//check if this page is the record current location,if not, find the current location
	//prepare the variable for get the current location
	Slot slot;
	RID currentPointer=rid;
	RID nextPointer = {0,0} ;
	//testInfo
	//cout<<rid.slotNum<<endl;
	//using while loop to get the current location and load it into pageBuffer
	while(true){
		slot = getRIDInfo(pageBuffer,currentPointer);
		if(slot.offset==-1){
			free(pageBuffer);
			return -1;
		}
		if(slot.length!=8) break;
		memcpy(&nextPointer,(char*)pageBuffer+slot.offset,slot.length);
		currentPointer = nextPointer;
		fileHandle.readPage(currentPointer.pageNum,pageBuffer);
	};

	memcpy(data,(char *)pageBuffer+slot.offset,slot.length);

	free(pageBuffer);
	return (unsigned)slot.length;
}









RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, const string &attributeName, void *data) {
	int attrPosition = -1;
	AttrType thisType;
	bool nullBit = false;
	int fieldOffset = 0;
	char nullIndicator;

	//Find the position of the specfic attributeName in the record
	for(int i = 0; i < recordDescriptor.size(); i++) {
		if(recordDescriptor[i].name == attributeName) {
			attrPosition = i;
			thisType = recordDescriptor[i].type;
			break;
		}
	}

	if(attrPosition == -1)
    		return -1;

    //create a buffer to store the page
    void *dataBuffer = malloc(PAGE_SIZE);
    if(readRecord(fileHandle,recordDescriptor,rid,dataBuffer)){
    		perror("failed to read record in read Attribute");
    		free(dataBuffer);
    		return -1;
    }
    char* recordBuffer = (char*)malloc(PAGE_SIZE);
    getRecord(recordDescriptor,dataBuffer,recordBuffer);
    //get nulls bit
//    int bytesForField = ceil((double)recordDescriptor.size()/8);
//    unsigned char *nullsIndicator = (unsigned char*)malloc(bytesForField);
//    memcpy(nullsIndicator, (char*)dataBuffer + (3 + recordDescriptor.size()) * sizeof(unsigned), bytesForField);
//    int round = attrPosition / 8;
//    nullBit = nullsIndicator[round]&(1<<(7-(attrPosition%8)));
    memcpy(&nullIndicator,(char*)recordBuffer+(3+recordDescriptor.size())*sizeof(unsigned)+attrPosition/8,sizeof(char));
    nullBit = nullIndicator & (1<<(7-(attrPosition%8)));



    memcpy(&fieldOffset,(char*)recordBuffer+(3+attrPosition)*sizeof(unsigned),sizeof(int));

	char null = 0;
    if(nullBit) {
		null = 128;
		memcpy((char*)data,&null,sizeof(char));
		free(recordBuffer);
		free(dataBuffer);
		return 0;
    }

	switch(thisType){
		case TypeInt:{
			memcpy((char*)data,&null, sizeof(char));
			memcpy((char*)data+ sizeof(char) ,(char*)recordBuffer + fieldOffset , sizeof(int));
			break;
		}
		case TypeReal:{
			memcpy((char*)data,&null, sizeof(char));
			memcpy((char*)data+ sizeof(char) ,(char*)recordBuffer + fieldOffset , sizeof(float));
			break;
		}
		case TypeVarChar:{
			int nameLength = 0;
			memcpy((char*)data,&null, sizeof(char));
			memcpy(&nameLength ,(char*)recordBuffer + fieldOffset , sizeof(int));
			memcpy((char*)data + sizeof(char) ,(char*)recordBuffer + fieldOffset , sizeof(int)+nameLength);
			break;
		}
	}

	free(recordBuffer);
	free(dataBuffer);
	return 0;
}

RC RecordBasedFileManager::scan(FileHandle &fileHandle,
 const vector<Attribute> &recordDescriptor,
 const string &conditionAttribute,
 const CompOp compOp,
 const void *value,
 const vector<string> &attributeNames,
 RBFM_ScanIterator &rbfm_ScanIterator) {

    //import info to the scan iterator
    int RC_Scan = rbfm_ScanIterator.importInfoToScanIterator(fileHandle, recordDescriptor, conditionAttribute, compOp, value, attributeNames);
    if(RC_Scan != 0)
     return -1;

    return 0;

}

RC RBFM_ScanIterator::importInfoToScanIterator(FileHandle &fileHandle,
	const vector<Attribute> &recordDescriptor,
	const string &conditionAttribute,
	const CompOp compOp,
	const void *value,
	const vector<string> &attributeNames) {

	//import basic information to the Scan Iterator
	this->recordDescriptor = recordDescriptor;
	this->fileHandle = fileHandle;
	this->compOp = compOp;
	this->value = value;
	this->attributeNames = attributeNames;



	//testInfo
//	cout<<endl<<"FileHandle name is "<<fileHandle.getfileName()<<endl;
//	cout<<endl<<"comp op is "<<this->compOp<<endl;
//	cout<<endl<<"value is "<< *(int*)value<<endl;
//	cout<<endl<<"attribute name is "<<attributeNames[0]<<endl;
//	cout<<endl<<"record descriptor size is "<<recordDescriptor.size()<<endl;


    //set conditionAttribute for the sacn iterator
    if(compOp == NO_OP)
    		this->conditionAttribute = conditionAttribute;
    else {
    		for(int i = 0; i < recordDescriptor.size(); i++) {

			if(recordDescriptor[i].name == conditionAttribute) {
				this->conditionAttribute = recordDescriptor[i].name;
				this->compType = recordDescriptor[i].type;
				break;
			}
    		}
    }

    //testInfo
//    cout<<endl<<"condition attribute is " << this->conditionAttribute << endl;
//    cout << endl << "compType: " << this->compType << endl;

    //innitialize the basic information of the scan iterator
    unsigned LastPage = this->fileHandle.getNumberOfPages();


    //string fileName = this->fileHandle.getfileName();
    if(LastPage > 0) {

     //initialize the scan iterator
		void *dataBuffer = malloc(PAGE_SIZE);
		this->finalPageNum = LastPage - 1;
		this->fileHandle.readPage(this->finalPageNum, (char*)dataBuffer);
		this->finalSlotNum = RecordBasedFileManager::instance()->getPageInfo(dataBuffer, SLOTNUM);
		this->currentRID.pageNum = 0;
		this->currentRID.slotNum = 1;

        free(dataBuffer);
    }
    else {

     //file is empty
     this->finalPageNum = 0;
     this->finalSlotNum = 0;
     this->currentRID.pageNum = 0;
     this->currentRID.slotNum = 0;
    }

 return 0;
}

bool RBFM_ScanIterator::hasRecord(const char* pageBuffer) {

	//judge if the file is empty
	if(this->finalPageNum == 0 && this->finalSlotNum == 0){
		return false;
	}

	while(this->currentRID.pageNum <= this->finalPageNum) {

		//load current page to the dataBuffer
		if(this->fileHandle.readPage(this->currentRID.pageNum,(void*)pageBuffer)){
			perror("read page is not right");
		}

		//get the current Slot information of the currentRID
		this->cur_slot = RecordBasedFileManager::instance()->getRIDInfo((void*)pageBuffer, this->currentRID);
		//get current page's slot number
		this->currentPageSlotNum = RecordBasedFileManager::instance()->getPageInfo((void*)pageBuffer,SLOTNUM);

		//testInfo
//		cout<<endl<<"````fileHandle name`````"<< this->fileHandle.getfileName() << endl;
//		cout<<endl<<"````current slot````````"<< this->currentRID.slotNum << endl;
//		cout<<endl<<"````record descriptor```"<< this->recordDescriptor[0].name << endl;
//		cout<<endl<<"````conditionAttribute``"<< this->conditionAttribute << endl;
//		cout<<endl<<"````compOp``````````````"<< this->compOp << endl;
//		cout<<endl<<"````attributeNames``````"<< this->attributeNames[0] << endl;
//		cout<<endl<<"````finalPageNum````````"<< this->finalPageNum << endl;
//		cout<<endl<<"````finalSlotNum````````"<< this->finalSlotNum << endl;
//		cout<<endl<<"````currentPageSlotNum``"<< this->currentPageSlotNum << endl;
//		cout<<endl<<"````cur_slot.length`````"<< this->cur_slot.length << endl;



		if(this->currentRID.slotNum > this->currentPageSlotNum){
			this->currentRID.pageNum++;
			this->currentRID.slotNum = 1;
			continue;
		}

		//still remains slot in the current page
		while(this->currentRID.slotNum <= this->currentPageSlotNum) {

			//get the current Slot information of the currentRID
			this->cur_slot = RecordBasedFileManager::instance()->getRIDInfo((void*)pageBuffer, this->currentRID);

			//record which links to this slot has already been deleted
			if(this->cur_slot.offset < 0) {
				this->currentRID.slotNum++;

				continue;
			}
			//cur_slot >= 0
			//current record which links to this slot is a pointer
			if(this->cur_slot.length == 8) {
			 this->currentRID.slotNum++;
				continue;
			}
            return true;
        }
	}

 return false;
}

bool RBFM_ScanIterator::dataCompare(const char* pageBuffer) {
 //rbfm->readAttribute(this->fileHandle, this->recordDescriptor, this->currentRID, this->conditionAttribute, currentRecord)

 //get the record for the current RID
    Slot slotForRecord = this->cur_slot;
    void *currentRecord = malloc(slotForRecord.length);
    memcpy((char*)currentRecord, (char*)pageBuffer + slotForRecord.offset, slotForRecord.length);

    unsigned testPageNum, testSlotNum, testfieldNum, testOffSet1;
    memcpy(&testPageNum, (char*)currentRecord, sizeof(unsigned));
    memcpy(&testSlotNum, (char*)currentRecord + sizeof(unsigned), sizeof(unsigned));
    memcpy(&testfieldNum, (char*)currentRecord + 2 * sizeof(unsigned), sizeof(unsigned));
    memcpy(&testOffSet1, (char*)currentRecord + 3 * sizeof(unsigned), sizeof(unsigned));

	if(this->compOp == NO_OP) {
		free(currentRecord);
		return true;
	}

	//get fieldNum of the current record
	unsigned cur_fieldNum;
	memcpy(&cur_fieldNum, (char*)currentRecord + 2 * sizeof(unsigned), sizeof(unsigned));

	//get nullsIndicator of the current record
	unsigned bytesForField = ceil((double)cur_fieldNum / 8);
	unsigned char *nullsIndicator = (unsigned char *) malloc(bytesForField);
	memcpy(nullsIndicator, (char*)currentRecord + (3 + recordDescriptor.size()) * sizeof(unsigned), bytesForField);

	//judge if the given conditionAttribute is null
	unsigned fieldPos = findFieldNum(this->conditionAttribute, this->recordDescriptor);
	unsigned round = fieldPos / 8;
	bool nullBit = false;
	nullBit = nullsIndicator[round]&(1<<(7-(fieldPos%8)));

	free(nullsIndicator);

	if(nullBit) {
		free(currentRecord);
		return false;
	}

    int fieldOffset;
    memcpy(&fieldOffset, (char*)currentRecord + (3 + fieldPos) * sizeof(unsigned), sizeof(int));

    //prepare the data for the comparison
	switch(this->compType) {
		case TypeInt: {
			int leftData;
			memcpy(&leftData, (char*)currentRecord + fieldOffset, sizeof(int));
			int rightData = *(int*) this->value;
			free(currentRecord);
			//do the comparison
			if(compOp == EQ_OP){
				return leftData == rightData;
			}

			if(compOp == LT_OP)
				return leftData < rightData;
			if(compOp == LE_OP)
				return leftData <= rightData;
			if(compOp == GT_OP) {

			  return leftData > rightData;
			}
			if(compOp == GE_OP)
				return leftData >= rightData;
			if(compOp == NE_OP)
				return leftData != rightData;
			break;
			}
     case TypeReal: {
      float leftData;
      memcpy(&leftData, (char*)currentRecord + fieldOffset, sizeof(float));
      float rightData = *(float*) this->value;
      free(currentRecord);
      //do the comparison
      if(compOp == EQ_OP)
       return leftData == rightData;
      if(compOp == LT_OP)
       return leftData < rightData;
      if(compOp == LE_OP)
       return leftData <= rightData;
      if(compOp == GT_OP)
          return leftData > rightData;
         if(compOp == GE_OP)
          return leftData >= rightData;
         if(compOp == NE_OP)
          return leftData != rightData;
      break;
     }
     case TypeVarChar: {
		int varLength;
		memcpy(&varLength, (char*)currentRecord + fieldOffset, sizeof(int));
		char *temp_varchar = (char*)malloc(varLength);
		memcpy(temp_varchar, (char*)currentRecord + fieldOffset+ sizeof(int), varLength);
		string leftData(temp_varchar);
		string rightData((char*)(this->value));
		free(temp_varchar);
		free(currentRecord);
		//do the comparison
		if(compOp == EQ_OP){
	    	  //testInfo
//		cout << endl << "EQ_OP begin" << endl;
//		cout <<endl<< "leftData: " << leftData << endl;
//		cout <<endl<< "rightData: " << rightData << endl;

			return leftData == rightData;
		}
		if(compOp == LT_OP)
			return leftData < rightData;
		if(compOp == LE_OP)
			return leftData <= rightData;
		if(compOp == GT_OP)
			return leftData > rightData;
		if(compOp == GE_OP)
			return leftData >= rightData;
		if(compOp == NE_OP)
			return leftData != rightData;
		break;
     }
    }

	cout << endl << "compare fail" << endl;
	free(currentRecord);
	return false;
}

//get the Attribute of the given attributeName
Attribute RBFM_ScanIterator::findField(const string &attributeName, const vector<Attribute> &recordDescriptor) {
 Attribute field;
 for(unsigned i = 0; i < recordDescriptor.size(); i++) {
  if(recordDescriptor[i].name == attributeName)
   field = recordDescriptor[i];
 }

 return field;
}

//find the fieldNum of the attributeName in the recordDescriptor
RC RBFM_ScanIterator::findFieldNum(const string &attributeName, const vector<Attribute> &recordDescriptor) {
 unsigned fieldNum;
 for(unsigned i = 0; i < recordDescriptor.size(); i++) {
  if(recordDescriptor[i].name == attributeName)
   fieldNum = i;
 }

 return fieldNum;
}

RC RBFM_ScanIterator::retrieveData(const RID &rid, void *data) {

	//prepare for the retrieve process
	unsigned offset = 0;

	//get the record for the given record
	char* buffer = (char*)malloc(PAGE_SIZE);
	unsigned fullRecordSize= RecordBasedFileManager::instance()->readFullSizeRecord(this->fileHandle,this->recordDescriptor,rid,(void*)buffer);
	void *dataBuffer = malloc(fullRecordSize);
	memcpy((char*)dataBuffer, (char*)buffer, fullRecordSize);
	free(buffer);

	//get the nullsIndicator for the original data
	int bytesForField_1 = ceil((double)this->recordDescriptor.size() / 8);
	unsigned char *nullsIndicator_1 = (unsigned char*)malloc(bytesForField_1);
	memset(nullsIndicator_1, 0, bytesForField_1);
	memcpy(nullsIndicator_1, (char*)dataBuffer, bytesForField_1);

	//initialize the nullsIndicator for the new data
	int bytesForField_2 = ceil((double)this->attributeNames.size() / 8);
	unsigned char *nullsIndicator_2 = (unsigned char*)malloc(bytesForField_2);
	memset(nullsIndicator_2, 0, bytesForField_2);

//set the value for the new nullsIndicator
	for(int i = 0; i < this->attributeNames.size(); i++) {
		unsigned fieldNum = findFieldNum(this->attributeNames[i], this->recordDescriptor);
		int index = fieldNum / 8;
		int nullBit = 0;
		nullBit = nullsIndicator_1[index]&(1<<(7-(fieldNum%8)));
		if(nullBit) {
			nullsIndicator_2[i/8]=nullsIndicator_2[i/8]|(1<<(7-(i%8)));
		}
	}

	//import the new nullsindicator to the datas
	memcpy((char*)data, nullsIndicator_2, bytesForField_2);
	offset += bytesForField_2;

	for(int i = 0; i < this->attributeNames.size(); i++) {

	//judge if the new field is null
	int round = i / 8;
	bool nullbit = false;
	nullbit = nullsIndicator_2[round] & (1<<(7-(i%8)));
	if(nullbit)
	continue;

	//get the field of given attributeName
	Attribute field = findField(this->attributeNames[i], this->recordDescriptor);
	unsigned fieldNum = findFieldNum(this->attributeNames[i], this->recordDescriptor);
	unsigned fieldOffset;
	memcpy(&fieldOffset, (char*)dataBuffer + (3 + fieldNum) * sizeof(unsigned), sizeof(unsigned));

	//import the field to the new data
		switch(field.type) {
			case TypeInt: {
				memcpy((char*)data + offset, (char*)dataBuffer + fieldOffset, sizeof(int));
				offset += sizeof(int);
				break;
			}
			case TypeReal: {
				memcpy((char*)data + offset, (char*)dataBuffer + fieldOffset, sizeof(float));
				offset += sizeof(float);
				break;
			}
			case TypeVarChar: {
				int varLength;
				memcpy(&varLength, (char*)dataBuffer + fieldOffset, sizeof(unsigned));
				memcpy((char*)data + offset, (char*)dataBuffer + fieldOffset, sizeof(unsigned) + varLength);
				offset += (sizeof(unsigned) + varLength);
				break;
			}
		}
	}
	free(nullsIndicator_1);
	free(nullsIndicator_2);
	free(dataBuffer);
	return 0;
}

RC RBFM_ScanIterator::getNextRecord(RID &rid, void *data) {

	char *pageBuffer = (char*)malloc(PAGE_SIZE);
	memset(pageBuffer,0,PAGE_SIZE);


	while(true) {
		bool hasNextRecord = hasRecord(pageBuffer);
		if(!hasNextRecord) break;


		if(dataCompare(pageBuffer)) {
			//get the current record

			Slot slotForRecord = RecordBasedFileManager::instance()->getRIDInfo(pageBuffer, this->currentRID);

			char *cur_record =(char*) malloc(slotForRecord.length);
			memcpy((char*)cur_record, (char*)pageBuffer + slotForRecord.offset, slotForRecord.length);

			//get the aim rid
			memcpy(&rid.pageNum, (char*)cur_record, sizeof(unsigned));
			memcpy(&rid.slotNum, (char*)cur_record + sizeof(unsigned), sizeof(unsigned));

			free(cur_record);

			if(retrieveData(rid, data) != 0){
				perror("retrieve data wrong!");
				free(pageBuffer);
				return -1;
			}

			//update current RID info
			this->currentRID.slotNum++;
			free(pageBuffer);
			return 0;
		}
		else {

			this->currentRID.slotNum++;

			continue;
		}
	}

	free(pageBuffer);

	//testInfo
//	cout<<endl<<"xunhuan jieshu"<<endl;

	return RBFM_EOF;
}

RC RBFM_ScanIterator::close() {
	RecordBasedFileManager::instance()->closeFile(fileHandle);
    return 0;
}
