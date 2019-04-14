#include "BF.h"
#define P_MAX_RECORDS ((BLOCK_SIZE - 2 * sizeof(int)) / sizeof(Record))
#define S_MAX_RECORDS ((BLOCK_SIZE - 2 * sizeof(int)) / sizeof(SRecord))
#define MAX_BUCKETS 10

typedef struct
{
    int id;
    char name[15];
    char surname[20];
    char address[40];
} Record;

typedef struct{
    char secondaryKey[40];
    int recordID;
    int blockID;        //Το block στο οποίο έγινε η εισαγωγή της εγγραφής στο πρωτεύον ευρετήριο.
} SRecord;


typedef struct{
    Record record;
    int blockId;        //Το block στο οποίο έγινε η εισαγωγή της εγγραφής στο πρωτεύον ευρετήριο.
}SecondaryRecord;

typedef struct
{
    int recordsCounter;
    int nextBlock;
    Record records[P_MAX_RECORDS];
}Block;

typedef struct
{
    int recordsCounter;
    int nextBlock;
    SRecord records[S_MAX_RECORDS];
}SBlock;

typedef struct 
{
    int fileDesc;           /* αναγνωριστικός αριθμός ανοίγματος αρχείου από το επίπεδο block */
    int attrLength;         /* το μέγεθος του πεδίου που είναι κλειδί για το συγκεκριμένο αρχείο */
    long int numBuckets;    /* το πλήθος των “κάδων” του αρχείου κατακερματισμού */
    char attrType;          /* ο τύπος του πεδίου που είναι κλειδί για το συγκεκριμένο αρχείο, 'c' ή'i' */
    char* attrName;         /* το όνομα του πεδίου που είναι κλειδί για το συγκεκριμένο αρχείο */
} HT_info;

typedef struct 
{
    int fileDesc;           /* αναγνωριστικός αριθμός ανοίγματος αρχείου από το επίπεδο block */
    int attrLength;         /* το μέγεθος του πεδίου που είναι κλειδί για το συγκεκριμένο αρχείο */
    int fileNameSize;
    long int numBuckets;    /* το πλήθος των “κάδων” του αρχείου κατακερματισμού */
    char* attrName;         /* το όνομα του πεδίου που είναι κλειδί για το συγκεκριμένο αρχείο */
    char *fileName;         /* όνομα αρχείου με το πρωτεύον ευρετήριο στο id */
} SHT_info;

int hash_integer(int key, int buckets);
int hash_string(char *str, int buckets);
int HT_CreateIndex(char *fileName, char attrType, char* attrName, int attrLength, int buckets);
HT_info* HT_OpenIndex( char *fileName );
int HT_CloseIndex( HT_info *header_info );
int HT_InsertEntry( HT_info header_info, Record record);
int HT_DeleteEntry(HT_info header_info, void *value);
void printRecord(Record record);
int HT_GetAllEntries(HT_info header_info, void *value);
int recordExists(int id, char *table, int fileDesc);
int SHT_SecondaryInsertEntry( SHT_info header_info, SecondaryRecord record);
int SHT_CreateSecondaryIndex(char *sfileName, char *attrName, int attrLength, int buckets, char *fileName);
SHT_info* SHT_OpenSecondaryIndex( char *sfileName );
int SHT_CloseSecondaryIndex( SHT_info *header_info );
int SHT_SecondaryGetAllEntries(SHT_info header_info_sht, HT_info header_info_ht, void *value);
void HashStatistics(char *fileName);