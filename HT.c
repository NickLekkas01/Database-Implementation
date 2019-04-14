#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "HT.h"
#include "BF.h"

/*File Descriptor for Primary Hash Table*/
int primary_fileDesc;
/*Table with first block's ID for every bucket(Primary Hash Table)*/
int primaryHashTable[MAX_BUCKETS];
/*Table with first block's ID for every bucket(Secondary Hash Table)*/
int secondaryHashTable[MAX_BUCKETS];

int HT_CreateIndex(
    char *fileName,               /* όνομα αρχείου */
    char attrType,                /* τύπος πεδίου-κλειδιού: 'c', 'i' */
    char* attrName,               /* όνομα πεδίου-κλειδιού */
    int attrLength,               /* μήκος πεδίου-κλειδιού */
    int buckets)                   /* αριθμός κάδων κατακερματισμού*/
{
    int result;
    int blockID, hi_blockID;

    result = BF_CreateFile(fileName);
    if(result < 0)
    {
        BF_PrintError("CreateIndex CreateFile problem\n");
        return -1;
    }
    int fileDesc = BF_OpenFile(fileName);
    if(fileDesc < 0)
    {
        BF_PrintError("OpenIndex OpenFile problem\n");
        return -1;
    }

    void* info;

    HT_info hi;
    hi.fileDesc = fileDesc;
    hi.attrLength = attrLength;
    hi.numBuckets = buckets;
    hi.attrType = attrType;
    hi.attrName = malloc(attrLength*sizeof(char));
    strcpy(hi.attrName, attrName);
    
    Block *p_block;
    /*Allocate Memory for First Block which is HT_info*/
    result = BF_AllocateBlock(fileDesc);
    if(result < 0)
    {
        BF_PrintError("CreateIndex Allocate problem\n");
        return -1;
    }
    hi_blockID = BF_GetBlockCounter(fileDesc)-1;
    /**/

    for (int i = 0; i < buckets; ++i)
    {
        /*Allocate Block for every bucket*/
        result = BF_AllocateBlock(fileDesc);
        if(result < 0)
        {
            BF_PrintError("CreateIndex Allocate problem\n");
            return -1;
        }
        /*Get the Block's ID*/
        blockID = BF_GetBlockCounter(fileDesc)-1;
        
        /*Read the block from disk*/
        result = BF_ReadBlock(fileDesc, blockID, (void **)&info);
        if(result < 0)
        {
            BF_PrintError("CreateIndex ReadBlock problem\n");
            return -1;
        }
        p_block = (Block *)info;
        /*Initialize data for block*/
        p_block->recordsCounter = 0;
        p_block->nextBlock = -1;
        for(int j = 0; j < P_MAX_RECORDS; j++)
        {
            p_block->records[j].id = -1;        //ID = -1 is Empty Record
        }
        /*Save blockID for every bucket to primaryHashTable*/
        primaryHashTable[i] = blockID;
    
        /*Write Block to disk*/
        result = BF_WriteBlock(fileDesc, blockID);
        if(result < 0)
        {
            BF_PrintError("CreateIndex WriteBlock problem\n");
            return -1;
        }
        
    }
    /*Write ht_info to the block*/
    result = BF_ReadBlock(fileDesc, hi_blockID, (void **)&info);
    if(result < 0)
    {
        BF_PrintError("CreateIndex ReadBlock problem\n");
        return -1;
    }
    /*Copy the static data of HT_info struct to disc*/
    memcpy(info, &hi, sizeof(HT_info));
    /*Copy the dynamic data of HT_info struct to disc*/
    memcpy(info+sizeof(HT_info), hi.attrName, sizeof(char)*attrLength);
    
    result = BF_WriteBlock(fileDesc, hi_blockID);
    if(result < 0)
    {
        BF_PrintError("CreateIndex WriteBlock problem\n");
        return -1;
    }
    /**/

    result = BF_CloseFile(fileDesc);
    if(result < 0)
    {
        BF_PrintError("CreateIndex CloseFile problem\n");
        return -1;
    }
    free(hi.attrName);

    return 0;
}

HT_info* HT_OpenIndex( char *fileName )
{
    int fileDesc = BF_OpenFile(fileName);
    primary_fileDesc = fileDesc;

    if(fileDesc < 0)
    {
        BF_PrintError("OpenIndex OpenFile problem\n");
        return NULL;
    }
    void *info;
    int result = BF_ReadBlock(fileDesc, 0, (void **)&info);
    if(result < 0)
    {
        BF_PrintError("OpenIndex ReadBlock problem\n");
        return NULL;
    }
    /*Copy the fileDescriptor to disc*/
    ((HT_info *)info)->fileDesc = fileDesc; 
    
    HT_info *hi = malloc(sizeof(HT_info));
    memcpy(hi, info, sizeof(HT_info));
    hi->attrName = malloc((hi->attrLength)*sizeof(char));
    memcpy(hi->attrName, info+sizeof(HT_info), (hi->attrLength)*sizeof(char));

    return hi;
}

int HT_CloseIndex( HT_info *header_info )
{
    int result = BF_CloseFile(header_info->fileDesc);
    if(result < 0)
        return -1;
    free(header_info->attrName);
    free(header_info);
    return 0;
}

int HT_InsertEntry( HT_info header_info, Record record)
{
    
    if(recordExists(record.id, "PHT", header_info.fileDesc) >= 0)
        return -1;
        

    int key;
    if (strcmp(header_info.attrName,"id") == 0)
        key = hash_integer(record.id, header_info.numBuckets);
    else if (strcmp(header_info.attrName,"name") == 0)
        key = hash_string(record.name, header_info.numBuckets);
    else if (strcmp(header_info.attrName,"surname") == 0)
        key = hash_string(record.surname, header_info.numBuckets);
    else if (strcmp(header_info.attrName,"address") == 0)
        key = hash_string(record.address, header_info.numBuckets);

    void *info;
    /*Take the first Block's ID in the bucket of our hash key*/
    int blockID = primaryHashTable[key];
    int result = BF_ReadBlock(header_info.fileDesc, blockID, (void **)&info);
    if (result < 0)
    {
        BF_PrintError("InsertEntry ReadBlock problem\n");
        return -1;
    } 
    Block *p_block;
    p_block = (Block *)info;
    while(1)
    {
        /*If the block is full*/
        if (p_block->recordsCounter >= P_MAX_RECORDS)
        {
            /*If there is no Block after that one*/
            if(p_block->nextBlock == -1)
            {
                /*Allocate a new block*/
                result = BF_AllocateBlock(header_info.fileDesc);
                if (result < 0)
                {
                    BF_PrintError("InsertEntry Allocate problem\n");
                    return -1;
                }
                /*Make this one has the ID of the block we just created*/
                p_block->nextBlock = BF_GetBlockCounter(header_info.fileDesc)-1;
                
                /*Write the updated block to disc*/
                result = BF_WriteBlock(header_info.fileDesc, blockID);
                if (result < 0)
                {
                    BF_PrintError("InsertEntry WriteBlock problem\n");
                    return -1;
                }
                /*Make current block the new allocared one*/
                blockID = p_block->nextBlock;
       
                /*Read it from disc*/
                result = BF_ReadBlock(header_info.fileDesc, blockID, (void **)&info);
                if (result < 0)
                {
                    BF_PrintError("InsertEntry ReadBlock problem\n");
                    return -1;
                }
                p_block = (Block *)info;
                /*Initialize it*/
                p_block->recordsCounter = 0;
                for(int j = 0; j < P_MAX_RECORDS; j++)
                {
					p_block->records[j].id = -1;
				}
                p_block->nextBlock = -1;
                break;
            }
            /*If another block exists after that one*/
            else
            {
                /*Read the next block*/
                blockID = p_block->nextBlock;
                result = BF_ReadBlock(header_info.fileDesc, blockID, (void **)&info);
                if (result < 0)
                {
                    BF_PrintError("InsertEntry ReadBlock problem\n");
                    return -1;
                }
                p_block = (Block *)info;
                continue;
            }
        }
        else
            break;
    }
    int index = 0;
    while(index < P_MAX_RECORDS)
    {
        /*If record position is empty*/
        if(p_block->records[index].id == -1)
        {
            /*Save our record to block*/
            p_block->records[index].id = record.id;
            strcpy(p_block->records[index].name, record.name);
            strcpy(p_block->records[index].surname, record.surname);
            strcpy(p_block->records[index].address, record.address);

            p_block->recordsCounter++;
            
            /*Write it to disc*/
            result = BF_WriteBlock(header_info.fileDesc, blockID);
            if (result < 0)
            {
                BF_PrintError("InsertEntry Write problem\n");
                return -1;
            }
            return blockID;
        }
        index++;
    }
    return -1;
}

int HT_DeleteEntry(HT_info header_info, void *value)
{
    void *info;
    int result;
    int index;
    
    Block *p_block;
    int key;
	if (strcmp(header_info.attrName,"id") == 0)    
        key = hash_integer(*(int *)value ,header_info.numBuckets);
    else
        key = hash_string((char *)value ,header_info.numBuckets);
    /*Take the first Block's ID in the bucket of our hash key*/
    key = primaryHashTable[key];
    int flag = 0;
    while(1)
    {
        /*Read the block from disk*/
        result = BF_ReadBlock(header_info.fileDesc, key, (void **)&info);
        if (result < 0)
        {
            BF_PrintError("DeleteEntry Read problem\n");
            return -1;
        }
        p_block = (Block *)info;
         
        index = 0;
        while(index < P_MAX_RECORDS)
        {
            /*If record position is not empty*/
            if(p_block->records[index].id != -1)
            {
                /*Check accordingly to the attribute Name if the value is the same with the record's value*/
                if (strcmp(header_info.attrName,"id") == 0)
                    if (p_block->records[index].id == *(int*)value) 
                        flag = 1;
                else if (strcmp(header_info.attrName,"name") == 0)
                    if (strcmp(p_block->records[index].name,(char*)value) == 0)
                        flag = 1;
                else if (strcmp(header_info.attrName,"surname") == 0)
                    if (strcmp(p_block->records[index].surname,(char*)value) == 0)
                        flag = 1;
                else if (strcmp(header_info.attrName,"address") == 0)
                    if (strcmp(p_block->records[index].address,(char*)value) == 0)
                        flag = 1;
                
                /*If it is*/
                if(flag == 1)
                {
                    /*Delete it making it not visible(ID = -1)*/
                    p_block->records[index].id = -1;
                    p_block->recordsCounter--;
                    result = BF_WriteBlock(header_info.fileDesc, key);
                    if (result < 0)
                    {
                        BF_PrintError("DeleteEntry Write problem \n");
                        return -1;
                    }
                    return 0;
                }
            }
            index++;
        }    
        /*Move to next block if exists*/
        if(p_block->nextBlock != -1)
            key = p_block->nextBlock;
        else
            break;
    }
    return -1;
}

int HT_GetAllEntries(HT_info header_info, void *value)
{
    void *info;
    Block *p_block;
    int result;
    int blockCounter = 0;
    int key;
    if (strcmp(header_info.attrName,"id") == 0)
        key = hash_integer(*(int *)value, MAX_BUCKETS);
    else
        key = hash_string((char *)value, MAX_BUCKETS);
    
    /*Take the first Block's ID in the bucket of our hash key*/
    key = primaryHashTable[key];
    int found = 0;
    while(1)
    {
        result = BF_ReadBlock(header_info.fileDesc, key, (void **)&info);
        if (result < 0)
        {
            BF_PrintError("GetAllEntries Read problem\n");
            return -1;
        }
        p_block = (Block *)info;
        for(int i = 0; i < P_MAX_RECORDS; ++i)
        {
            /*If record position is not empty*/
            if (p_block->records[i].id != -1)
            {
                /*Check accordingly to the attribute Name 
                if the value is the same with the record's value
                and print it*/
                if (strcmp(header_info.attrName,"id") == 0)
                {
                    if(p_block->records[i].id == (*(int *)value))
                    {    
                        printRecord(p_block->records[i]);
                        found = 1;
                    }
                }
                else if (strcmp(header_info.attrName,"name") == 0)
                {
                    if (strcmp(p_block->records[i].name,(char*)value) == 0)
                    {    
                        printRecord(p_block->records[i]);
                        found = 1;
                    }
                }
                else if (strcmp(header_info.attrName,"surname") == 0) 
                {
					if (strcmp(p_block->records[i].surname,(char*)value) == 0) 
                    {    
                        printRecord(p_block->records[i]);
                        found = 1;
                    }
				}
				else if (strcmp(header_info.attrName,"address") == 0)
                {
					if (strcmp(p_block->records[i].address,(char*)value) == 0) 
                    {    
                        printRecord(p_block->records[i]);
                        found = 1;
                    }
				}
            }
        }
        /*Counts how many blocks we passed*/
        blockCounter++;
        /*Move to next block if exists*/
        if(p_block->nextBlock != -1)
            key = p_block->nextBlock;
        else
            break;
    }
    /*If we found at least one record print the number of blocks we passed*/
    if(found == 1)
        return blockCounter;
    else
        return -1;
}