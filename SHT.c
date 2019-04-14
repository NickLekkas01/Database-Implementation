#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "HT.h"
#include "BF.h"

/*File Descriptor for Primary Hash Table*/
extern int primary_fileDesc;
/*Table with first block's ID for every bucket(Primary Hash Table)*/
extern int primaryHashTable[MAX_BUCKETS];
/*Table with first block's ID for every bucket(Secondary Hash Table)*/
extern int secondaryHashTable[MAX_BUCKETS];

int SHT_SecondaryInsertEntry( SHT_info header_info, SecondaryRecord record)
{
    
    int key;
    if (strcmp(header_info.attrName,"id") == 0)
        key = hash_integer(record.record.id, header_info.numBuckets);
    else if (strcmp(header_info.attrName,"name") == 0)
        key = hash_string(record.record.name, header_info.numBuckets);
    else if (strcmp(header_info.attrName,"surname") == 0)
        key = hash_string(record.record.surname, header_info.numBuckets);
    else if (strcmp(header_info.attrName,"address") == 0)
        key = hash_string(record.record.address, header_info.numBuckets);
    void *info;
    /*Take the first Block's ID in the bucket of our hash key*/
    int blockID = secondaryHashTable[key];
    int result = BF_ReadBlock(header_info.fileDesc, blockID, (void **)&info);
    if (result < 0)
    {
        BF_PrintError("SHT InsertEntry ReadBlock problem\n");
        return -1;
    } 
    SBlock *s_block;
    s_block = (SBlock *)info;
    while(1)
    {
        /*If the block is full*/
        if (s_block->recordsCounter >= S_MAX_RECORDS)
        {
            /*If there is no Block after that one*/
            if(s_block->nextBlock == -1)
            {
                /*Allocate a new block*/
                result = BF_AllocateBlock(header_info.fileDesc);
                if (result < 0)
                {
                    BF_PrintError("SHT InsertEntry Allocate problem\n");
                    return -1;
                }
                /*Make this one has the ID of the block we just created*/
                s_block->nextBlock = BF_GetBlockCounter(header_info.fileDesc)-1;

                /*Write the updated block to disc*/
                result = BF_WriteBlock(header_info.fileDesc, blockID);
                if (result < 0)
                {
                    BF_PrintError("InsertEntry WriteBlock problem\n");
                    return -1;
                }
                /*Make current block the new allocared one*/
                blockID = s_block->nextBlock;

                /*Read it from disc*/
                result = BF_ReadBlock(header_info.fileDesc, blockID, (void **)&info);
                if (result < 0)
                {
                    BF_PrintError("SHT InsertEntry ReadBlock problem\n");
                    return -1;
                }
                s_block = (SBlock *)info;
                /*Initialize it*/
                s_block->recordsCounter = 0;
                for(int j = 0; j < S_MAX_RECORDS; j++)
                {
					s_block->records[j].recordID = -1;
				}
                s_block->nextBlock = -1;
                break;
            }
            /*If another block exists after that one*/
            else
            {
                /*Read the next block*/
                blockID = s_block->nextBlock;
                result = BF_ReadBlock(header_info.fileDesc, blockID, (void **)&info);
                if (result < 0)
                {
                    BF_PrintError("SHT InsertEntry ReadBlock problem\n");
                    return -1;
                }
                s_block = (SBlock *)info;
                continue;
            }
        }
        else
            break;
    }

    int index = 0;
    while(index < S_MAX_RECORDS)
    {
        /*If record position is empty*/
        if(s_block->records[index].recordID == -1)
        { 
            /*Save our record to block*/
            s_block->records[index].recordID = record.record.id;
            if(strcmp(header_info.attrName,"name") == 0)
                strcpy(s_block->records[index].secondaryKey, record.record.name);
            else if(strcmp(header_info.attrName,"surname") == 0)
                strcpy(s_block->records[index].secondaryKey, record.record.surname);
            else if(strcmp(header_info.attrName,"address") == 0)
                strcpy(s_block->records[index].secondaryKey, record.record.address);
            s_block->records[index].blockID = record.blockId;
            s_block->recordsCounter++;
            
            /*Write it to disc*/
            result = BF_WriteBlock(header_info.fileDesc, blockID);
            if (result < 0)
            {
                BF_PrintError("InsertEntry Write problem\n");
                return -1;
            }
            return 0;
        }
        index++;
    }
    return -1;
}

int SHT_CreateSecondaryIndex(char *sfileName, char *attrName, 
                            int attrLength, int buckets,
                            char *fileName)
{
    int result;
    int blockID, hi_blockID;

    result = BF_CreateFile(sfileName);
    if(result < 0)
    {
        BF_PrintError("SHT CreateIndex CreateFile problem\n");
        return -1;
    }
    int fileDesc = BF_OpenFile(sfileName);
    if(fileDesc < 0)
    {
        BF_PrintError("SHT OpenIndex OpenFile problem\n");
        return -1;
    }

    void* info;

    SHT_info hi;
    hi.fileDesc = fileDesc;
    hi.attrLength = attrLength;
    hi.numBuckets = buckets;
    hi.attrName = malloc(attrLength*sizeof(char));
    strcpy(hi.attrName, attrName);
    hi.fileNameSize = (strlen(fileName)+1);
    hi.fileName = malloc(hi.fileNameSize*sizeof(char));
    strcpy(hi.fileName, fileName);
    
    SBlock *s_block;
    /*Allocate Memory for First Block which is HT_info*/
    result = BF_AllocateBlock(fileDesc);
    if(result < 0)
    {
        BF_PrintError("SHT CreateIndex Allocate problem\n");
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
            BF_PrintError("SHT CreateIndex Allocate problem\n");
            return -1;
        }
        /*Get the Block's ID*/
        blockID = BF_GetBlockCounter(fileDesc)-1;
        
        /*Read the block from disk*/
        result = BF_ReadBlock(fileDesc, blockID, (void **)&info);
        if(result < 0)
        {
            BF_PrintError("SHT CreateIndex ReadBlock problem\n");
            return -1;
        }
        s_block = (SBlock *)info;
        /*Initialize data for block*/
        s_block->recordsCounter = 0;
        s_block->nextBlock = -1;
        for(int j = 0; j < S_MAX_RECORDS; j++)
        {
            s_block->records[j].recordID = -1;
        }
        /*Save blockID for every bucket to secondaryHashTable*/
        secondaryHashTable[i] = blockID;
    
        /*Write Block to disk*/
        result = BF_WriteBlock(fileDesc, blockID);
        if(result < 0)
        {
            BF_PrintError("SHT CreateIndex WriteBlock problem\n");
            return -1;
        }
        
    }
    /*Write ht_info to the block*/
    result = BF_ReadBlock(fileDesc, hi_blockID, (void **)&info);
    if(result < 0)
    {
        BF_PrintError("SHT CreateIndex ReadBlock problem\n");
        return -1;
    }
    
    /*Copy the static data of HT_info struct to disc*/
    memcpy(info, &hi, sizeof(SHT_info));
    /*Copy the dynamic data of HT_info struct to disc*/
    memcpy(info+sizeof(SHT_info), hi.attrName, sizeof(char)*hi.attrLength);
    memcpy(info+sizeof(SHT_info)+(sizeof(char)*attrLength), hi.fileName, sizeof(char)*(hi.fileNameSize));

    result = BF_WriteBlock(fileDesc, hi_blockID);
    if(result < 0)
    {
        BF_PrintError("SHT CreateIndex WriteBlock problem\n");
        return -1;
    }
    /**/

    /*Sychronize SHT hashTable with HT hashTable*/
    
    int key;
    Block p_block;
    for(int i = 0; i < MAX_BUCKETS; ++i)
    {
        key = primaryHashTable[i];
        while(1)
        {
            result = BF_ReadBlock(primary_fileDesc, key, (void **)&info);
            if(result < 0)
            {
                BF_PrintError("CreateIndex ReadBlock problem\n");
                return -1;
            }
            p_block = *(Block *)info;
            for(int j = 0; j < P_MAX_RECORDS; ++j)
            {
                if(p_block.records[j].id != -1)
                {
                    /*If record does not already exists in SecondaryHashTable*/
                    result = recordExists(p_block.records[j].id, "SHT", fileDesc);
                    if(result < 0)
                    {
                        /*Insert it*/
                        /*Initialize SR*/
                        SecondaryRecord sr;
                        sr.blockId = key;
                        //sr.record = p_block.records[j];
                        sr.record.id = p_block.records[j].id;
                        
                        if(strcmp(hi.attrName,"name") == 0)
                            strcpy(sr.record.name, p_block.records[j].name);
                        else if(strcmp(hi.attrName,"surname") == 0)
                            strcpy(sr.record.surname, p_block.records[j].surname);
                        else if(strcmp(hi.attrName,"address") == 0)
                            strcpy(sr.record.address, p_block.records[j].address);
                        
                        result = SHT_SecondaryInsertEntry(hi, sr);
                    }
                }
            }

            /*Move to next block if exists*/
            if(p_block.nextBlock != -1)
                key = p_block.nextBlock;
            else
                break;
        }
    }
    /**/

    result = BF_CloseFile(fileDesc);
    if(result < 0)
    {
        BF_PrintError("CreateIndex CloseFile problem\n");
        return -1;
    }
    free(hi.attrName);
    free(hi.fileName);
    return 0;
}


SHT_info* SHT_OpenSecondaryIndex( char *sfileName )
{
    int fileDesc = BF_OpenFile(sfileName);
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
    ((SHT_info *)info)->fileDesc = fileDesc;

    SHT_info *hi = malloc(sizeof(SHT_info));
    memcpy(hi, info, sizeof(HT_info));
    hi->attrName = malloc((hi->attrLength)*sizeof(char));
    memcpy(hi->attrName, info+sizeof(SHT_info), (hi->attrLength)*sizeof(char));
    hi->fileName = malloc(hi->fileNameSize*sizeof(char));
    memcpy(hi->fileName, info+sizeof(SHT_info)+((hi->attrLength)*sizeof(char)), hi->fileNameSize*sizeof(char));

    return hi;
}

int SHT_CloseSecondaryIndex( SHT_info *header_info )
{
    int result = BF_CloseFile(header_info->fileDesc);
    if(result < 0)
        return -1;
    free(header_info->attrName);
    free(header_info->fileName);
    free(header_info);
    return 0;
}


int SHT_SecondaryGetAllEntries(SHT_info header_info_sht, HT_info header_info_ht, void *value)
{
    void *info;
    int flag = 0;
    Block *p_block;
    SBlock *s_block;
    int result;
    int blockCounter = 0;
    int key;
    int found = 0;
    if (strcmp(header_info_sht.attrName,"id") == 0)
        key = hash_integer(*(int *)value, header_info_sht.numBuckets);
    else
        key = hash_string((char *)value, header_info_sht.numBuckets);
    
    /*Take the first Block's ID in the bucket of our hash key*/
    key = secondaryHashTable[key];
    while(1)
    {
        result = BF_ReadBlock(header_info_sht.fileDesc, key, (void **)&info);
        if (result < 0)
        {
            BF_PrintError("SHT GetAllEntries Read problem\n");
            return -1;
        }
        s_block = (SBlock *)info;
        for(int i = 0; i < S_MAX_RECORDS; ++i)
        {
            flag = 0;
            /*If record position is not empty*/
            if (s_block->records[i].recordID != -1)
            {
                /*Check accordingly to the attribute Name 
                if the value is the same with the record's value
                and print it*/
                if (strcmp(header_info_sht.attrName,"id") == 0)
                {
                    if(s_block->records[i].recordID == *(int *)value)
                        flag = 1;
                }
                else if (strcmp(header_info_sht.attrName,"name") == 0)
                {
                    if (strcmp(s_block->records[i].secondaryKey,(char*)value) == 0)
                        flag = 1;
                }
                else if (strcmp(header_info_sht.attrName,"surname") == 0) 
                {
					if (strcmp(s_block->records[i].secondaryKey,(char*)value) == 0) 
                        flag = 1;
				}
				else if (strcmp(header_info_sht.attrName,"address") == 0)
                {
					if (strcmp(s_block->records[i].secondaryKey,(char*)value) == 0) 
                        flag = 1;
				}
                /*If it is*/
                if(flag == 1)
                {
                    /*Read from PrimaryHashTable the block with the record we search for*/
                    result = BF_ReadBlock(header_info_ht.fileDesc, s_block->records[i].blockID, (void **)&info);
                    if(result < 0)
                    {
                        BF_PrintError("SHT GetAllEntries ReadBlock problem\n");
                        return -1;
                    }
                    p_block = (Block *)info;
                    for(int j = 0; j < P_MAX_RECORDS; ++j)
                    {
                        /*If record position is not empty*/
                        if(p_block->records[j].id != -1)
                        {
                            /*Check accordingly to the attribute Name 
                            if the value is the same with the record's value
                            and print it from the PrimaryTable*/
                            if (strcmp(header_info_sht.attrName,"name") == 0)
                            {
                                if (strcmp(s_block->records[i].secondaryKey, p_block->records[j].name) == 0)
                                {
                                    printRecord(p_block->records[j]);
                                    found = 1;
                                }
                            }
                            else if (strcmp(header_info_sht.attrName,"surname") == 0) 
                            {
                                if (strcmp(s_block->records[i].secondaryKey, p_block->records[j].surname) == 0) 
                                {
                                    printRecord(p_block->records[j]);                                        
                                    found = 1;
                                }
                            }
                            else if (strcmp(header_info_sht.attrName,"address") == 0)
                            {
                                if (strcmp(s_block->records[i].secondaryKey, p_block->records[j].address) == 0) 
                                {    
                                    printRecord(p_block->records[j]);
                                    found = 1;
                                }
                            }
                        }
                    }
                }
            }
        }
        /*Counts how many blocks we passed*/
        blockCounter++;
        /*Move to next block if exists*/
        if(s_block->nextBlock != -1)
            key = s_block->nextBlock;
        else
            break;
    }
    /*If we found at least one record print the number of blocks we passed*/
    if(found == 1)
    {
        return blockCounter;
    }
    else
        return -1;
    
}
