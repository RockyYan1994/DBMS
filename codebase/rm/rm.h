
#ifndef _rm_h_
#define _rm_h_

#include <string>
#include <vector>

#include<unistd.h>


#include "../rbf/rbfm.h"

#define SYS_TUPLE_SIZE 120

using namespace std;


# define RM_EOF (-1)  // end of a scan operator

// RM_ScanIterator is an iteratr to go through tuples
class RM_ScanIterator {
public:
  RM_ScanIterator() {};
  ~RM_ScanIterator() {};

  // "data" follows the same format as RelationManager::insertTuple()
  RC getNextTuple(RID &rid, void *data);
  RC close();

  RBFM_ScanIterator scanIterator;
};


// Relation Manager
class RelationManager
{
public:
  static RelationManager* instance();

  RC createCatalog();

  RC deleteCatalog();

  RC createTable(const string &tableName, const vector<Attribute> &attrs);

  RC deleteTable(const string &tableName);

  RC getAttributes(const string &tableName, vector<Attribute> &attrs);

  RC insertTuple(const string &tableName, const void *data, RID &rid);

  RC deleteTuple(const string &tableName, const RID &rid);

  RC updateTuple(const string &tableName, const void *data, const RID &rid);

  RC readTuple(const string &tableName, const RID &rid, void *data);

  // Print a tuple that is passed to this utility method.
  // The format is the same as printRecord().
  RC printTuple(const vector<Attribute> &attrs, const void *data);

  RC readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data);

  // Scan returns an iterator to allow the caller to go through the results one by one.
  // Do not store entire results in the scan iterator.
  RC scan(const string &tableName,
      const string &conditionAttribute,
      const CompOp compOp,                  // comparison type such as "<" and "="
      const void *value,                    // used in the comparison
      const vector<string> &attributeNames, // a list of projected attributes
      RM_ScanIterator &rm_ScanIterator);

// Extra credit work (10 points)
public:
  RC addAttribute(const string &tableName, const Attribute &attr);

  RC dropAttribute(const string &tableName, const string &attributeName);



  //added functions
  void addAttributeToRecordDescriptor(vector<Attribute> &recordDescriptor,const string name,AttrType type,AttrLength length);
  //
  RC prepareColumnTable(const void* tuple,const vector<Attribute> &attrs,const int tableID);
  RC prepareTableTable(const void* tuple, const vector<Attribute> &attrs,const int tableID,const string &name,const string &fileName);
  RC prepareTableVec();
  RC prepareColumnVec();
//  //create rbfm instance
//  RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
  //check if file exists
  bool exists(const string &fileName);

  int getTableIDForAll();

//private:
  RC setTableAccess(const int access);
  int getTableAccess();
  RC resetTableIDForAll();

  RC increaseTableIDForAll();
  //added variable
  //to allocate table ID for every table
  int tableIDForAll=0;
  //0 means read-only
  int tableAccess=0;

  vector<Attribute> tableVec,columnVec;


protected:
  RelationManager();
  ~RelationManager();

private:
  static RelationManager *_rm_manager;

};

#endif
