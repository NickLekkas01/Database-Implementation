#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "HT.h"
#include "BF.h"

int main(void)
{
    BF_Init();
    char file[10];
    strcpy(file, "kati.txt");
    int result = HT_CreateIndex(file, 'i', "id", strlen("id")+1, MAX_BUCKETS);
    if(result < 0)
    {
        printf("CreateIndex problem\n");
        return -1;
    }
    HT_info *hi = HT_OpenIndex(file);
    char trash;
    char line[256], tmp[20];
    char *token;
    FILE *fp;
    fp = fopen("records1K.txt", "r");
    if (fp == NULL)
    {
        perror("Error while opening the file.\n");
        exit(EXIT_FAILURE);
    }
    Record r[1000];
    int blockID[1000];
    for(int i = 0; i < 1000; ++i)
    {
        fgets(line, sizeof(line), fp);
        
        token = strtok(line+1, ",");
        r[i].id = atoi(token);
        
        token = strtok(NULL, ",");
        strcpy(tmp, token+1);
        tmp[strlen(tmp)-1] = '\0';
        strcpy(r[i].name, tmp);
        
        token = strtok(NULL, ",");
        strcpy(tmp, token+1);
        tmp[strlen(tmp)-1] = '\0';
        strcpy(r[i].surname, tmp);    
        
        token = strtok(NULL, "}");
        strcpy(tmp, token+1);
        tmp[strlen(tmp)-1] = '\0';
        strcpy(r[i].address, tmp);    
        
        //printf("ID:%d ->NAME:%s ->SURNAME:%s ->ADDRESS:%s\n",r[i].id, r[i].name, r[i].surname, r[i].address);
        result = HT_InsertEntry(*hi, r[i]);
        if (result < 0)
        {
            printf("InsertEntry problem\n");
            return -1;    
        }
        blockID[i] = result;
    } 
    fclose(fp);
    //result = HT_DeleteEntry(*hi, &r.id);
    //if(result < 0)
    //    printf("DeleteEntry problem\n");
    
    
    result = HT_GetAllEntries(*hi, &r[0].id);
    if(result < 0)
    {
        printf("GetAllEntries problem\n");
        return -1;
    }
    printf("Blocks read: %d\n",result);

    char file2[10];
    strcpy(file2, "kati2.txt");
    result = SHT_CreateSecondaryIndex(file2, "name", 5, MAX_BUCKETS, file);
    if (result < 0)
    {
        printf("SHT CreateSecondaryIndex problem\n");
        return -1;
    }
    SHT_info *shi = SHT_OpenSecondaryIndex(file2);

    SecondaryRecord rec[1000];
    for(int i = 0; i < 1000; ++i)
    {
        rec[i].blockId = blockID[i];
        rec[i].record = r[i];
    }
    /*
    result = SHT_SecondaryInsertEntry(*shi, rec);
    if(result < 0)
    {
        printf("SHT SecondaryInsert problem \n");
        return -1;
    }
    */
    printf("\n");
    result = SHT_SecondaryGetAllEntries(*shi, *hi, &r[0].name);
    if(result < 0)
    {
        printf("SHT GetAllEntries problem\n");
        return -1;    
    }
    printf("Blocks read: %d\n",result);
    result = SHT_CloseSecondaryIndex(shi);
    if(result < 0)
    {
        printf("SHT CloseSecondaryIndex problem\n");
        return -1;    
    }
    result = HT_CloseIndex(hi);
    if(result < 0)
    {
        printf("CloseIndex problem\n");
        return -1;    
    }
}