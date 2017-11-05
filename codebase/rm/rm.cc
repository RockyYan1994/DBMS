
#include "rm.h"
#include<math.h>
#include<iostream>
#include <string>
RelationManager* RelationManager::instance()
{
    static RelationManager _rm;
    return &_rm;
}

RelationManager::RelationManager()
{
}

RelationManager::~RelationManager()
{
}

RC RelationManager::createCatalog()
{

	//create two tables to store system tables:Table table and Columns table
	if(tableVec.size()==0){
		prepareTableVec();
		prepareColumnVec();
	}


	//testInfo
//	cout<<"tableVec size is "<<tableVec.size()<<endl;


	//increate table number
	increaseTableIDForAll();
	//allocate a table-id for this table
	int tableNum = getTableIDForAll();

	//create two rbf corresponding to this table
	RecordBasedFileManager::instance()->createFile("Table");
	RecordBasedFileManager::instance()->createFile("Column");

	//according to the attrs and insert tuples into Table table and Column table
	char* tuple = (char*)malloc(SYS_TUPLE_SIZE);
	memset(tuple,0,SYS_TUPLE_SIZE);

	//insert Table table info into Table table
	prepareTableTable((void*)tuple,tableVec,tableNum,"Table","Table" );
	prepareColumnTable((void*)tuple,tableVec,tableNum);

	//increate table number
	increaseTableIDForAll();
	tableNum = getTableIDForAll();
	//insert Column table info into Column table
	prepareTableTable((void*)tuple,columnVec,tableNum,"Column","Column");
	prepareColumnTable((void*)tuple,columnVec,tableNum);

	//free tuple
	free(tuple);

	//set table access variable to 0
	setTableAccess(1);
    return 0;
}

RC RelationManager::deleteCatalog()
{
	if(exists("Table")==false) {
		cout<<endl<<"Table do not exist"<< endl;
		return 0;
	}


	//set the table access to 1 and set tableIDForAll to 0
	setTableAccess(0);
	resetTableIDForAll();

	//retrive all the table name
	RID rid;
	char* buffer = (char*)malloc(50);
	int nameSize = 0;
	int value =2;
	vector<string> vec;
	vec.push_back("table-name");
	RM_ScanIterator iterator;

	//
	scan("Table","table-id",GT_OP,(void*)&value,vec,iterator);
	while(iterator.getNextTuple(rid,buffer) != -1){

		memcpy(&nameSize,(char*)buffer+sizeof(char),sizeof(int));
		char* nameBuffer = (char*)malloc(nameSize);
		memcpy(nameBuffer,(char*)buffer+sizeof(char)+sizeof(int),nameSize);
		string tableName(nameBuffer);
		RecordBasedFileManager::instance()->destroyFile(tableName);
		free(nameBuffer);
		memset(buffer,0,50);

		//testInfo
//		cout<<endl<<"RID is "<<rid.pageNum<<":"<<rid.slotNum<<endl;
//		cout<<endl<<"tableName is "<<tableName<<endl;

	}

//	//Nov added
	iterator.close();


	//delete two system table
	RecordBasedFileManager::instance()->destroyFile("Table");
	RecordBasedFileManager::instance()->destroyFile("Column");


	free(buffer);
	//testInfo
//	cout<<endl<<"destroyFile successfully"<<endl;

    return 0;
}

RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs)
{
	//increate table number
	increaseTableIDForAll();
	//allocate a table-id for this table
	int tableNum = getTableIDForAll();

	//create two rbf corresponding to this table
	RecordBasedFileManager::instance()->createFile(tableName);

	//according to the attrs and insert tuples into Table table and Column table
	char* tuple = (char*)malloc(SYS_TUPLE_SIZE);
	memset(tuple,0,SYS_TUPLE_SIZE);

	prepareTableTable((void*)tuple,attrs,tableNum,tableName,tableName );
	prepareColumnTable((void*)tuple,attrs,tableNum);

	//free tuple
	free(tuple);
    return 0;
}

RC RelationManager::deleteTable(const string &tableName)
{
	//delete table info from Table table and Column table

	//prepare variable
	RID rid;
	char* buffer = (char*)malloc(10);

	//according table name search Table table to get table-id
	string str = "table-id";
	vector<string> vec ;
	vec.push_back(str);
	RM_ScanIterator iterator ;
	scan("Table","table-name",EQ_OP,(void*)tableName.c_str(),vec,iterator);

	iterator.getNextTuple(rid,(void*)buffer);


	//use rid to delete tuple in Table table
	deleteTuple("Table",rid);

	iterator.close();

	int tableID = 0;
	int nullsByte = 1;
	memcpy(&tableID,(char*)buffer+nullsByte,sizeof(int));


	RM_ScanIterator iterator1 ;
	//prepare variable for deleting tuples int Column table
	scan("Column","table-id",EQ_OP,(void*)&tableID,vec,iterator1);

	while(iterator1.getNextTuple(rid,buffer)!=-1){

		deleteTuple("Column",rid);
	}

	iterator1.close();
	//testInfo
//	cout<< endl<< "delete table successfully" << endl;

	free(buffer);

	//delete table
//	RecordBasedFileManager::instance()->destroyFile(tableName);

    return 0;
}

RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs)
{
	if(tableVec.size()==0){
		prepareTableVec();
		prepareColumnVec();
	}
	if(tableName=="Table"){
		attrs = tableVec;
		return 0;
	}
	if(tableName=="Column") {
		attrs =  columnVec;
		return 0;
	}

	//testInfo
//	char* Buffer = (char*)malloc(PAGE_SIZE);
//	RID test = {0,1};
//	for(int i=0;i<6;i++){
//		test.slotNum = i+1;
//		readTuple("Table",test,Buffer);
//		printTuple(tableVec,Buffer);
//	}
//	free(Buffer);

	vector<string> vec;
	vec.push_back("table-id");
	RM_ScanIterator iterator;
	RID rid;
	int tableID=0 , nullsByte = 1;
	char* buffer = (char*)malloc(100);
	scan("Table","table-name",EQ_OP,(void*)tableName.c_str(),vec,iterator);
	if(iterator.getNextTuple(rid,buffer)==-1){
		perror("get attributes failed, failed to get next tuple");
		free(buffer);
		return -1;
	}
	iterator.close();

	//testInfo
//	cout<<endl<<"rid info is "<<rid.pageNum<<""<<rid.slotNum<<endl;

	memcpy(&tableID,(char*)buffer+nullsByte,sizeof(int));
	vector<string> attrsName;
	attrsName.push_back("column-name");
	attrsName.push_back("column-type");
	attrsName.push_back("column-length");

	RM_ScanIterator iterator1;
	scan("Column","table-id",EQ_OP,(void*)&tableID,attrsName,iterator1);
	int offset,nameSize=0;
	//allocate memory for name
	Attribute attr;
	while(iterator1.getNextTuple(rid,buffer)!=-1){
		offset = 1;
		memcpy(&nameSize,(char*)buffer+offset,sizeof(int)	);
		offset+=sizeof(int);
		char* nameBuffer = (char*)malloc(nameSize);
		memcpy(nameBuffer,(char*)buffer+offset,nameSize);
		attr.name = string(nameBuffer);
		offset+=nameSize;
		memcpy(&attr.type,(char*)buffer+offset,sizeof(AttrType));
		offset+=sizeof(AttrType);
		memcpy(&attr.length,(char*)buffer+offset,sizeof(AttrLength));
		offset+=sizeof(AttrLength);

		attrs.push_back(attr);
		free(nameBuffer);
	}
	iterator1.close();

	free(buffer);
    return 0;
}

RC  RelationManager::insertTuple(const string &tableName, const void *data, RID &rid)
{
    //get the attributes of the given tuple
    vector<Attribute> recordDescriptor;
    if(getAttributes(tableName, recordDescriptor)){
    		perror("insert tuple failed, failed to get attributes");
    		return -1;
    }

    //using API of the rbfm to insert the tuple
    FileHandle fileHandle;
    if(RecordBasedFileManager::instance()->openFile(tableName, fileHandle) != 0)    //need fix fileName
        return -1;
    RecordBasedFileManager::instance()->insertRecord(fileHandle, recordDescriptor, data, rid);

    RecordBasedFileManager::instance()->closeFile(fileHandle);
    return 0;
}

RC RelationManager::deleteTuple(const string &tableName, const RID &rid)
{
    //get the attributes of the given tuple
    vector<Attribute> recordDescriptor;
    getAttributes(tableName, recordDescriptor);

    //using API of the rbfm to delete the tuple
    FileHandle fileHandle;
    if(RecordBasedFileManager::instance()->openFile(tableName, fileHandle) != 0)     //need fix fileName
        return -1;
    if(RecordBasedFileManager::instance()->deleteRecord(fileHandle, recordDescriptor, rid)){
    		perror("failed to delete tuple ");
    		RecordBasedFileManager::instance()->closeFile(fileHandle);
    		return -1;
    }
    RecordBasedFileManager::instance()->closeFile(fileHandle);

    return 0;
}

RC RelationManager::updateTuple(const string &tableName, const void *data, const RID &rid)
{
    //get the attributes of the given tuple
    vector<Attribute> recordDescriptor;
    getAttributes(tableName, recordDescriptor);

    //using API of the rbfm to update the tuple
    FileHandle fileHandle;
    if(RecordBasedFileManager::instance()->openFile(tableName, fileHandle) != 0)     //need fix fileName
        return -1;
    RecordBasedFileManager::instance()->updateRecord(fileHandle, recordDescriptor, data, rid);
    RecordBasedFileManager::instance()->closeFile(fileHandle);
    return 0;
}

RC RelationManager::readTuple(const string &tableName, const RID &rid, void *data)
{
    //get the attributes of the given tuple
    vector<Attribute> recordDescriptor;
    if(getAttributes(tableName, recordDescriptor)){
//    		perror("get attributes failed in read tuple");
    		return -1;
    }

    //using API of the rbfm to update the tuple
    FileHandle fileHandle;
    if(RecordBasedFileManager::instance()->openFile(tableName, fileHandle) != 0)     //need fix fileName
        return -1;
    if(RecordBasedFileManager::instance()->readRecord(fileHandle, recordDescriptor, rid, data) == -1){
//    		perror("read tuple failed!");
    		RecordBasedFileManager::instance()->closeFile(fileHandle);
    		return -1;
    }
    RecordBasedFileManager::instance()->closeFile(fileHandle);
    return 0;
}

RC RelationManager::printTuple(const vector<Attribute> &attrs, const void *data)
{
    //using API of the rbfm to update the tuple
	RecordBasedFileManager::instance()->printRecord(attrs, data);
    return 0;
}

RC RelationManager::readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data)
{
    //get the attributes of the given tuple
    vector<Attribute> recordDescriptor;
    if(getAttributes(tableName, recordDescriptor)){
    		perror("read attribute failed");
    		return -1;
    }

    //using API of the rbfm to update the tuple
    FileHandle fileHandle;
    if(RecordBasedFileManager::instance()->openFile(tableName, fileHandle) != 0)     //need fix fileName
        return -1;
    if(RecordBasedFileManager::instance()->readAttribute(fileHandle, recordDescriptor, rid, attributeName, data)){
    		RecordBasedFileManager::instance()->closeFile(fileHandle);
    		return -1;
    }
    RecordBasedFileManager::instance()->closeFile(fileHandle);
    return 0;
}

// Extra credit work
RC RelationManager::dropAttribute(const string &tableName, const string &attributeName)
{
    return -1;
}

// Extra credit work
RC RelationManager::addAttribute(const string &tableName, const Attribute &attr)
{
    return -1;
}


inline void RelationManager::addAttributeToRecordDescriptor(vector<Attribute> &recordDescriptor,const string name,AttrType type,AttrLength length) {

    Attribute attr;
    attr.name = name;
    attr.type = type;
    attr.length = length;
    recordDescriptor.push_back(attr);
}

//prepare tuple and insert tuple into Column table in succession
RC RelationManager::prepareColumnTable(const void* tuple,const vector<Attribute> &attrs,const int tableID){
	//initial variable
	string name;
	int nameSize=0;;
	RID rid;
	int position = 0;
	unsigned offset ;
	for(int i=0;i<attrs.size();i++){
		offset = 1;
		name = attrs[i].name;
		nameSize = name.length()+1;
		char* namePtr = (char*)malloc(nameSize);
		strcpy(namePtr,name.c_str());
		position = i+1;
		//copy data into tuple
		memcpy((char*)tuple+offset,&tableID,sizeof(int));
		offset += sizeof(int);


		memcpy((char*)tuple+offset,&nameSize,sizeof(int));
		offset += sizeof(int);

		memcpy((char*)tuple+offset, namePtr, nameSize);
		offset += nameSize;

		memcpy((char*)tuple+offset,&attrs[i].type,sizeof(AttrType));
		offset += sizeof(AttrType);

		memcpy((char*)tuple+offset,&attrs[i].length,sizeof(AttrLength));
		offset += sizeof(AttrLength);

		memcpy((char*)tuple+offset,&position,sizeof(int));
		offset += sizeof(int);

		memcpy((char*)tuple+offset,&tableAccess,sizeof(int));
		offset += sizeof(int);

		free(namePtr);
		//allocate a tuple buffer to store tuple and then insert into Column table
		char* tupleBuffer = (char*)malloc(offset);
		memset(tupleBuffer,0,offset);
		memcpy(tupleBuffer,tuple,offset);
		insertTuple("Column",tupleBuffer,rid);

		//free tupleBuffer
		free(tupleBuffer);
	}

	return 0;
}
RC RelationManager::prepareTableTable(const void* tuple,const vector<Attribute> &attrs,const int tableID,const string &name,const string &fileName){
	//set offset to 1,because Table table only have 3 fields and cannot be NULL
	unsigned offset = 1;
	RID rid={0,0};
	//prepare a tuple buffer to store the
	int nameSize = name.length()+1;
	int fileNameSize = fileName.length()+1;
	char* namePtr = (char*)malloc(name.length());
	char* fileNamePtr = (char*)malloc(fileName.length());
	strcpy(namePtr,name.c_str());
	strcpy(fileNamePtr,fileName.c_str());

	//prepare the tuple
	memcpy((char*)tuple+offset,&tableID,sizeof(int));
	offset += sizeof(int);

	memcpy((char*)tuple+offset,&nameSize,sizeof(int));
	offset += sizeof(int);

	memcpy((char*)tuple+offset,namePtr,nameSize);
	offset += nameSize;

	memcpy((char*)tuple+offset,&fileNameSize,sizeof(int));
	offset += sizeof(int);

	memcpy((char*)tuple+offset,fileNamePtr,fileNameSize);
	offset += fileNameSize;

	memcpy((char*)tuple+offset,&tableAccess,sizeof(int));
	offset += sizeof(int);

	//create a tuple buffer and copy tuple data into it and insert into Table table
	char* tupleBuffer = (char*)malloc(offset);
	memset(tupleBuffer,0,offset);
	memcpy(tupleBuffer,tuple,offset);
	insertTuple("Table",tupleBuffer,rid);

	//free tuple buffer
	free(namePtr);
	free(fileNamePtr);
	free(tupleBuffer);
	return 0;
}

RC RelationManager::prepareTableVec(){
	//create vector<Attrbute> to store table's attrs
	//table-id:int, table-name:varchar(50), file-name:varchar(50)
	addAttributeToRecordDescriptor(tableVec,"table-id",TypeInt,(AttrLength)4);
	addAttributeToRecordDescriptor(tableVec,"table-name",TypeVarChar,(AttrLength)50);
	addAttributeToRecordDescriptor(tableVec,"file-name",TypeVarChar,(AttrLength)50);
	addAttributeToRecordDescriptor(tableVec,"table-access",TypeInt,(AttrLength)4);
	return 0;
}

RC RelationManager::prepareColumnVec(){
	//create vector<Attrbute> to store columns's attrs
	//Columns(table-id:int, column-name:varchar(50), column-type:int, column-length:int, column-position:int)
	addAttributeToRecordDescriptor(columnVec,"table-id",TypeInt,(AttrLength)4);
	addAttributeToRecordDescriptor(columnVec,"column-name",TypeVarChar,(AttrLength)50);
	addAttributeToRecordDescriptor(columnVec,"column-type",TypeInt,(AttrLength)4);
	addAttributeToRecordDescriptor(columnVec,"column-length",TypeInt,(AttrLength)4);
	addAttributeToRecordDescriptor(columnVec,"column-position",TypeInt,(AttrLength)4);
	addAttributeToRecordDescriptor(columnVec,"table-access",TypeInt,(AttrLength)4);
	return 0;
}


RC RelationManager::setTableAccess(const int access){
	tableAccess = access;
	return 0;
}
int RelationManager::getTableAccess(){
	return tableAccess;
}
RC RelationManager::resetTableIDForAll(){
	tableIDForAll = 0;
	return 0;
}
int RelationManager::getTableIDForAll(){
	return tableIDForAll;
}
RC RelationManager::increaseTableIDForAll(){
	tableIDForAll++;
	return 0;
}







RC RelationManager::scan(const string &tableName,
      const string &conditionAttribute,
      const CompOp compOp,
      const void *value,
      const vector<string> &attributeNames,
      RM_ScanIterator &rm_ScanIterator) {

	//prepare the data for scan
	FileHandle fileHandle;
	RecordBasedFileManager::instance()->openFile(tableName, fileHandle);
	vector<Attribute> recordDescriptor;
	if(getAttributes(tableName, recordDescriptor)){
		perror("get attribute failed in scan");
		RecordBasedFileManager::instance()->closeFile(fileHandle);
		return -1;
	}
	RBFM_ScanIterator rbfm_ScanIterator;

	//using the scan API in the rbfm layer
	RecordBasedFileManager::instance()->scan(fileHandle, recordDescriptor, conditionAttribute, compOp, value, attributeNames, rbfm_ScanIterator);
	rm_ScanIterator.scanIterator = rbfm_ScanIterator;

    return 0;
}

RC RM_ScanIterator::getNextTuple(RID &rid, void *data) {
    return scanIterator.getNextRecord(rid, data);
}

RC RM_ScanIterator::close() {
    scanIterator.close();
    return 0;
}


inline bool RelationManager::exists(const string &fileName)
{
	return (access(fileName.c_str(),F_OK) != -1);
}
