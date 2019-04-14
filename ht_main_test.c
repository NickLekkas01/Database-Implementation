/*H**********************************************************************
* FILENAME : ht_main_test.c
*
* DESCRIPTION :
*       First version of the assignment validator.
*
* NOTES :
*       The function includes 11 tests.
*		You may pipe the main to grep Results in order to check the tests results.
*		Change the naming  of your files according to this example  main .
*		Example  compile: gcc -std=c99 -o ht_main_test ht_main_test.c ht.c BF_64.a
*		Assumes that your implementation is placed in ht.c and your header  is HT.h
* PARAMETERS: 
*		1) Number of records for test.
*		2) Proportion for deletes.
* EXAMPLE:
		ht_main_test 1000 0.1
* AUTHOR :	Nikolaos Panagiotou
*H*/
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdlib.h>
#include "BF.h"
#include "HT.h"


int main(char argc,char** argv)
{
	/*
	How many records to use for the  test.
	*/
	int testRecordsNumber=atoi(argv[1]);
	/*
	The proportion for the deletes.
	*/
	int testDeleteRecords=(int)testRecordsNumber*atof(argv[2]);
	/*
	Init the BF layer.
	*/
	BF_Init();
	/*
	Index parameters.
	*/
	char* fileName="primary.index";
	char attrType='i';
	char* attrName="id";
	int attrLength=4;
	int buckets=10;
	char* sfileName="secondary.index";
	char sAttrType='c';
	char* sAttrName="name";
	int sAttrLength=15;
	int sBuckets=10;
	/*
	C1: Create the  index.
	*/
	printf("@Checkpoint 1: Create Index\n");
	int createNumCode=HT_CreateIndex(fileName,attrType,attrName,attrLength,buckets);
	if (createNumCode==0)
	{
		printf("Checkpoint Result 1: SUCCESS\n");
	}
	else
	{
		printf("Checkpoint Result 1: FAIL\n");
	}
	HT_info* hi;
	/*
	C2: Open index.
	*/
	printf("@Checkpoint 2: Open Index\n");
	hi=HT_OpenIndex(fileName);
	if(hi!=NULL && hi->attrType==attrType && strcmp(hi->attrName,attrName)==0)
	{
		printf("Checkpoint Result 2: SUCCESS\n");
	}
	else
	{
		printf("Checkpoint Result 2: FAIL\n");
	}
	/*
	C3: Insert records.
	*/
	printf("@Checkpoint 3: Insert Records\n");
	for (int i=0;i<testRecordsNumber;i++)
	{
		Record record;
		record.id=i;
		sprintf(record.name,"name_%d",i);
		sprintf(record.surname,"surname_%d",i);
		sprintf(record.address,"address_%d",i);
		HT_InsertEntry(*hi,record);
	}
	/*
	C4: Get all entries.
	*/
	printf("@Checkpoint 4: Get all entries (Expecting greater than zero return code)\n");
	int ch4=0;
	for (int i=0;i<testRecordsNumber;i++)
	{
		Record record;
		record.id=i;
		sprintf(record.name,"name_%d",i);
		sprintf(record.surname,"surname_%d",i);
		sprintf(record.address,"address_%d",i);
		int err=HT_GetAllEntries(*hi,(void*)&record.id);
		if (err<0)
		{
			ch4+=1;
		}
	}
	if(ch4>0)
	{
		printf("Checkpoint Result 4: FAIL\n");
	}
	else
	{
		printf("Checkpoint Result 4: SUCCESS\n");
	}
	printf("@Checkpoint 5: Get all entries (Expecting -1 return code, records do not exist)\n");
	int ch5=0;
	/*
	C5: Get entries that dont exist.
	*/
	for (int i=testRecordsNumber;i<testRecordsNumber*2;i++)
	{
		Record record;
		record.id=i;
		sprintf(record.name,"name_%d",i);
		sprintf(record.surname,"surname_%d",i);
		sprintf(record.address,"address_%d",i);
		int err=HT_GetAllEntries(*hi,(void*)&record.id);
		if (err<0)
		{
			ch5+=1;
		}
	}
	if(ch5!=testRecordsNumber)
	{
		printf("Checkpoint Result 5: FAIL\n");
	}
	else
	{
		printf("Checkpoint Result 5: SUCCESS\n");
	}
	/*
	C6: Delete entries.
	*/
	printf("@Checkpoint 6: Delete some entries\n");
	int ch6=0;
	int deletesTest=(int)testDeleteRecords;
	for (int i=0;i<deletesTest;i++)
	{
		Record record;
		record.id=i;
		sprintf(record.name,"name_%d",i);
		sprintf(record.surname,"surname_%d",i);
		sprintf(record.address,"address_%d",i);
		int err=HT_DeleteEntry(*hi,(void*)&record.id);
		if (err!=0)
		{
			ch6+=1;
		}
	}
	if(ch6!=0)
	{
		printf("Checkpoint Result 6: FAIL\n");
	}
	else
	{
		printf("Checkpoint Result 6: SUCCESS\n");
	}
	/*
	C7: Get all entries.
	*/
	printf("@Checkpoint 7: Get all entries (%d should not exist)\n",testDeleteRecords);
	int ch7=0;
	for (int i=0;i<testRecordsNumber;i++)
	{
		Record record;
		record.id=i;
		sprintf(record.name,"name_%d",i);
		sprintf(record.surname,"surname_%d",i);
		sprintf(record.address,"address_%d",i);
		int err=HT_GetAllEntries(*hi,(void*)&record.id);
		if (err<0)
		{
			ch7+=1;
		}
	}
	printf("%d %d\n",deletesTest,ch7);
	if(ch7!=deletesTest)
	{
		printf("Checkpoint Result 7: FAIL\n");
	}
	else
	{
		printf("Checkpoint Result 7: SUCCESS\n");
	}
	/*
	Secondary index part.
	*/
	/*
	C8: Create/Open secondary index.
	*/
	printf("@Checkpoint 8: Create Secondary/ Open Index\n");
	int createErrorCode=SHT_CreateSecondaryIndex(sfileName,sAttrName,sAttrLength,sBuckets,fileName);
	if (createErrorCode<0)
	{
		printf("Checkpoint Result 8: FAILED\n");
		return -1;
	}
	SHT_info* shi=SHT_OpenSecondaryIndex(sfileName);
	if(shi!=NULL)
	{
		printf("Checkpoint Result 8 SUCCESS\n");
	}
	else
	{
		printf("Checkpoint Result 8: FAIL\n");
	}
	/*
	Secondary index insert records.
	*/
	/*
	C9: Insert entries to both indexes.
	*/
	printf("@Checkpoint 9: Insert Records Secondary\n");
	int ch9=0;
	for (int i=testRecordsNumber;i<testRecordsNumber*2;i++)
	{
		Record record;
		record.id=i;
		sprintf(record.name,"name_%d",i);
		sprintf(record.surname,"surname_%d",i);
		sprintf(record.address,"address_%d",i);
		/*
		We need to do two inserts:
			* One in the HT.
			* One in the SHT.
		*/
		int blockId=HT_InsertEntry(*hi,record);
		if (blockId>0)
		{
			SecondaryRecord sRecord;
			sRecord.record=record;
			sRecord.blockId=blockId;
			int sInsertError=SHT_SecondaryInsertEntry(*shi,sRecord);
			if(sInsertError<0)
			{
				ch9+=1;
			}
		}
	}
	if (ch9==0)
	{
		printf("Checkpoint Result 9: SUCCESS\n");
	}
	else
	{
		printf("Checkpoint Result 10: Fail\n");
	}
	/*
	C10: Get all entries using secondary the  index.
	*/
	printf("@Checkpoint 10: SHT Get all entries (All should exist except the deleted)\n");
	int ch10=0;
	for (int i=0;i<testRecordsNumber*2;i++)
	{
		Record record;
		record.id=i;
		sprintf(record.name,"name_%d",i);
		sprintf(record.surname,"surname_%d",i);
		sprintf(record.address,"address_%d",i);
		int err=SHT_SecondaryGetAllEntries(*shi,*hi,(void*)record.name);
		if (err<0)
		{
			ch10+=1;
		}
	}
	if(ch10>deletesTest)
	{
		/*
		If more than the deleted do not exist the test failed.
		*/
		printf("Checkpoint Result 10: FAIL\n");
	}
	else
	{
		printf("Checkpoint Result 10: SUCCESS\n");
	}
	printf("@Checkpoint 11\n");
	int htCloseError=HT_CloseIndex(hi);
	int shtCloseError=SHT_CloseSecondaryIndex(shi);
	if (htCloseError==0 && shtCloseError==0)
	{
		printf("Checkpoint Result 11: SUCCESS\n");
	}
	else
	{
		printf("Checkpoint Result 11: FAIL\n");
	}
	/*
	Hash statistics.
	*/

	printf("Statistics:HT\n");
	HashStatistics(fileName);
	printf("Statistics:SHT\n");
	HashStatistics(sfileName);
	return 0;
}   