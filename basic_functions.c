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

/*online source*/
int hash_integer(int key, int buckets) {
    key = ((key >> 16) ^ key) * 0x45d9f3b;
    key = ((key >> 16) ^ key) * 0x45d9f3b;
    key = (key >> 16) ^ key;
    key = key % buckets;
    return key;
}

/*online source*/
int hash_string(char *str, int buckets)
{
    unsigned long key = 5381;
    int c;

    while (c = *str++)
        key = ((key << 5) + key) + c; /* hash * 33 + c */

    return (int)(key % buckets);
}

void printRecord(Record record)
{   
    printf("ID:%d\n",record.id);
    printf("Name:%s\n",record.name);
    printf("Surname:%s\n",record.surname);
    printf("Address:%s\n",record.address);
}


/*Search if record exists in primary or secondary hashTable
and return block if found or -1 if not found*/
int recordExists(int id, char *table, int fileDesc)
{
    int result;
    void *info;
    
    int key = hash_integer(id, MAX_BUCKETS);
    if(strcmp(table, "PHT") == 0)
    {
        Block *p_block;
        /*Take the first Block's ID in the bucket of our hash key*/
        key = primaryHashTable[key];
        while(1)
        {
            result = BF_ReadBlock(fileDesc, key, (void **)&info);
            if(result < 0)
            {
                BF_PrintError("CreateIndex ReadBlock problem\n");
                return -1;
            }
            p_block = (Block *)info;      
            for(int i = 0; i < P_MAX_RECORDS; ++i)
            {
                /*If record position is not empty*/
                if(p_block->records[i].id != -1)
                {
                    /*If record has the same id return the blockID 
                    in which we found it */
                    if(p_block->records[i].id == id)
                        return key;
                }
            }
            /*Move to next block if exists*/
            if(p_block->nextBlock != -1)
                key = p_block->nextBlock;
            else
                return -1;
        }
    }
    else if(strcmp(table, "SHT") == 0)
    {
        SBlock *s_block;
        /*Take the first Block's ID in the bucket of our hash key*/
        key = secondaryHashTable[key];
        while(1)
        {
            result = BF_ReadBlock(fileDesc, key, (void **)&info);
            if(result < 0)
            {
                BF_PrintError("CreateIndex ReadBlock problem\n");
                return -1;
            }
            s_block = (SBlock *)info;        
            for(int i = 0; i < S_MAX_RECORDS; ++i)
            {
                /*If record position is not empty*/
                if(s_block->records[i].recordID != -1)
                {
                    /*If record has the same id return the blockID 
                    in which we found it */
                    if(s_block->records[i].recordID == id)
                        return key;
                }
            }
            if(s_block->nextBlock == -1)
                return -1;
            else
                key = s_block->nextBlock;
        }
    }
}

void HashStatistics(char *fileName)
{
    void *info;
    int result;
    int key;
    Block *b;
    SBlock *sb;
    HT_info *hi = HT_OpenIndex(fileName);
    SHT_info *shi;
    int min = P_MAX_RECORDS;
    if(strcmp(hi->attrName,"id") != 0)
    {
        shi = SHT_OpenSecondaryIndex(fileName);
        int min = S_MAX_RECORDS;
    }
    /*Total number of blocks in the hashtable*/
    int counterBlocks = 0;
    /*Number of block in every bucket*/
    int counterBucket = 0;
    /*Number of blocks with overflow*/
    int counterOverflow = 0;
    int max = 0;
    /*Amount of Records in every bucket*/
    int sumRecords;
    /*Average records in block in every bucket*/
    double average;
    /*Average Blocks in hashTable*/
    double averageBlocks;

    for(int i = 0 ; i < MAX_BUCKETS; ++i)
    {
        if(strcmp(hi->attrName,"id") == 0)
        {    
            key = primaryHashTable[i];
            min = P_MAX_RECORDS;
        }
        else
        {
            key = secondaryHashTable[i];
            min = S_MAX_RECORDS;
        }
        counterBucket = 0;
        max = 0;
        sumRecords = 0;
        while(1)
        {
            if(strcmp(hi->attrName,"id") == 0)
                result = BF_ReadBlock(hi->fileDesc, key, (void **)&info);
            else
                result = BF_ReadBlock(shi->fileDesc, key, (void **)&info);
            if(result < 0)
            {
                BF_PrintError("HashStatistics ReadBlock problem\n");
                return ;
            }
            counterBucket++;
            counterBlocks++;
            if(strcmp(hi->attrName,"id") == 0)
            {
                b = (Block *)info;
                sumRecords += b->recordsCounter;
                if(b->recordsCounter > max)
                    max = b->recordsCounter;
                if(b->recordsCounter < min)
                    min = b->recordsCounter;
                
                if(b->nextBlock != -1)
                    key = b->nextBlock;
                else
                    break;
            }
            else
            {
                sb = (SBlock *)info;
                sumRecords += sb->recordsCounter;
                if(sb->recordsCounter > max)
                    max = sb->recordsCounter;
                if(sb->recordsCounter < min)
                    min = sb->recordsCounter;
                
                if(sb->nextBlock != -1)
                    key = sb->nextBlock;
                else
                    break;
            }
        }
        average = (double)sumRecords / (double)counterBucket;
        if(counterBucket-1 > 0)
            counterOverflow++;
        printf("BUCKET %d: MIN: %d MAX %d AVERAGE %f\n",i, min, max, average);
        printf("           OVERFLOW BlOCKS %d\n",counterBucket-1);
        printf("\n");
    }
    printf("Total Blocks: %d\n",counterBlocks);
    averageBlocks = (double)counterBlocks / (double)MAX_BUCKETS;
    printf("Average Blocks: %f\n",averageBlocks);
    printf("Buckets with overflow: %d\n",counterOverflow);
    
    HT_CloseIndex(hi);    
    if(strcmp(hi->attrName,"id") != 0)
        SHT_CloseSecondaryIndex(shi);
}
