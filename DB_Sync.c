#include <mysql.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <stdbool.h>
#include <pthread.h>
#define MAX_IOT_DEVICES 32

/*
	Global Variable
	Database:	[IN4.0]
	table:		[IOT_List]
	Column:			|
					|-[conn_sta] row[5]	Update to DB2
					|-[status]	 row[7]	Update to DB2
					|-[last_cmd] row[9] Update to DB1
*/
typedef struct {
    char conn_sta[30];
    bool conn_sta_update;
    char status[100];
    bool status_update;
    char last_cmd[30];
    bool last_cmd_update;
} ColumnData;
ColumnData GColumnData[MAX_IOT_DEVICES];

/*
    DB work status
    false:can't work
    true:can work
*/
bool DB_1_Status = false;
bool DB_2_Status = false;

/*
	DB_1 is Global Database
	DB_2 is Inner  Database
	Both DB are same schema.
*/
pthread_t DB_1, DB_2 ;
pthread_mutex_t mutex_conn_sta, mutex_status, mutex_last_cmd;

void *Connect_DB_1()
{
    printf("Waiting connect to Global DB...\n");
    int IOT_ID = 0, i;
    MYSQL *conn;
    MYSQL_RES *res;
    MYSQL_ROW row;
    char *server = "localhost";
    char *user = "root";
    char *password = "123456"; /* set me first */
    char *database = "IOT_list";
    conn = mysql_init(NULL);
    /* Connect to database */
    if (!mysql_real_connect(conn, server, user, password, database, 0, NULL, 0)) {
        printf("%s\n", mysql_error(conn));
        DB_1_Status = false;
        pthread_exit(NULL);
    }
    DB_1_Status = true;
    /* If Inner DB are not ready,then wait */
    printf("Global DB connted , waiting Inner DB connect\n");
    while (!DB_2_Status);
    printf("Global DB start work!\n");

    /* Updata GColumnData from Global Database */
    char iot_id[2];
    char query_syntax[200];
    bool  firsttime = true;
    while(DB_2_Status){
        if (mysql_query(conn, "SELECT * FROM IOT_device")) {
            printf("%s\n", mysql_error(conn));
            DB_1_Status = false;
            pthread_exit(NULL);
        }
        res = mysql_use_result(conn);
        
        while ((row = mysql_fetch_row(res)) != NULL) {
            IOT_ID = atoi(row[0]);
            if (strcmp(GColumnData[IOT_ID].last_cmd, row[9]) != 0) {
                if (!firsttime) {                        /*If first time query do not give last_cmd_update true*/
                    //here need thread mutex lock
                    pthread_mutex_lock(&mutex_last_cmd);
                    GColumnData[IOT_ID].last_cmd_update = true;
                    pthread_mutex_unlock(&mutex_last_cmd);
                }
            }
            strcpy(GColumnData[IOT_ID].last_cmd, row[9]);
        }
        mysql_free_result(res);
        firsttime = false;
        //Update GColumnData to Global Database
        for (i = 0; i < MAX_IOT_DEVICES; i++) {
            sprintf(iot_id,"%d", i);
            if (GColumnData[i].conn_sta_update == true) {
                memset(query_syntax, 0, sizeof(query_syntax));
                strcat(query_syntax, "UPDATE IOT_device SET conn_sta='");
                strcat(query_syntax, GColumnData[i].conn_sta);
                strcat(query_syntax, "' WHERE iot_id=");
                strcat(query_syntax, iot_id);
                printf("%s\n", query_syntax);
                if (mysql_query(conn, query_syntax)) {
                    printf("%s\n", mysql_error(conn));
                }
                else {
                    printf("%ld products updated",(long) mysql_affected_rows(conn));
                    //here need thread mutex lock
                    pthread_mutex_lock(&mutex_conn_sta);
                    GColumnData[i].conn_sta_update = false;
                    pthread_mutex_unlock(&mutex_conn_sta);
                }
            }
            if (GColumnData[i].status_update == true) {
                memset(query_syntax, 0, sizeof(query_syntax));
                strcat(query_syntax, "UPDATE IOT_device SET status='");
                strcat(query_syntax, GColumnData[i].status);
                strcat(query_syntax, "' WHERE iot_id=");
                strcat(query_syntax, iot_id);
                printf("%s\n", query_syntax);
                
                if (mysql_query(conn, query_syntax)) {
                    printf("%s\n", mysql_error(conn));
                }
                else {
                    printf("%ld products updated",(long) mysql_affected_rows(conn));
                    //here need thread mutex lock
                    pthread_mutex_lock(&mutex_status);
                    GColumnData[i].status_update = false;
                    pthread_mutex_unlock(&mutex_status);
                }
            }
        }
        sleep(5);
    }
    
    /* close connection */
    mysql_close(conn);
    pthread_exit(NULL);
}

void *Connect_DB_2()
{   
    printf("Waiting connect to Inner DB...\n");
    int IOT_ID = 0, i;
    MYSQL *conn;
    MYSQL_RES *res;
    MYSQL_ROW row;
    char *server = "localhost";
    char *user = "root";
    char *password = "123456"; /* set me first */
    char *database = "IOT_list_inner";
    conn = mysql_init(NULL);
    /* Connect to database */
    if (!mysql_real_connect(conn, server, user, password, database, 0, NULL, 0)) {
        printf("%s\n", mysql_error(conn));
        DB_2_Status = false;
        pthread_exit(NULL);
    }
    DB_2_Status = true;
    /*If Global DB are not ready,then wait*/
    printf("Inner DB connted , waiting Global DB connect\n");
    while (!DB_1_Status);
    printf("Inner DB start work!\n");

	char iot_id[2];
    char query_syntax[200];
    bool  firsttime = true;
    while(DB_1_Status){
        // Updata GColumnData from Global Database
        if (mysql_query(conn, "SELECT * FROM IOT_device")) {
            printf("%s\n", mysql_error(conn));
            DB_2_Status = false;
            pthread_exit(NULL);
        }
        res = mysql_use_result(conn);
        
        while ((row = mysql_fetch_row(res)) != NULL) {
            IOT_ID = atoi(row[0]);
            if (strcmp(GColumnData[IOT_ID].conn_sta, row[5]) != 0) { 		/*check GColumnData change*/
                if (!firsttime) {            /*If first time query do not give conn_sta_update true*/
                    //here need thread mutex lock
                    pthread_mutex_lock(&mutex_conn_sta);
                    GColumnData[IOT_ID].conn_sta_update = true;
                    pthread_mutex_unlock(&mutex_conn_sta);
                }
            }
            strcpy(GColumnData[IOT_ID].conn_sta, row[5]);
			
			if (strcmp(GColumnData[IOT_ID].status, row[7]) != 0) { 		/*check GColumnData change*/
                if (!firsttime) {            /*If first time query do not give conn_sta_update true*/
                    //here need thread mutex lock
                    pthread_mutex_lock(&mutex_status);
                    GColumnData[IOT_ID].status_update = true;
                    pthread_mutex_unlock(&mutex_status);
                }
            }
            strcpy(GColumnData[IOT_ID].status, row[7]);
        }
        mysql_free_result(res);
        firsttime = false;
        //Update GColumnData to Global Database
        for (i = 0; i < MAX_IOT_DEVICES; i++) {
            sprintf(iot_id,"%d", i);
            if (GColumnData[i].last_cmd_update == true) {
                memset(query_syntax, 0, sizeof(query_syntax));
                strcat(query_syntax, "UPDATE IOT_device SET last_cmd='");
                strcat(query_syntax, GColumnData[i].last_cmd);
                strcat(query_syntax, "' WHERE iot_id=");
                strcat(query_syntax, iot_id);
                printf("%s\n", query_syntax);
                
                if (mysql_query(conn, query_syntax)) {
                    printf("%s\n", mysql_error(conn));
                }
                else {
                    printf("%ld products updated ",(long) mysql_affected_rows(conn));
                    //here need thread mutex lock
                    pthread_mutex_lock(&mutex_last_cmd);
                    GColumnData[i].last_cmd_update = false;
                    pthread_mutex_unlock(&mutex_last_cmd);
                }
            }
        }
        
        sleep(5);
    }
	   
    /* close connection */
    mysql_close(conn);
    pthread_exit(NULL);
}

int main(int argc ,char *argv[]) {
    int rc;
    if (mysql_library_init(0, NULL, NULL)) {
        printf("could not initialize MySQL library\n");
        return 0;
    }
	rc = pthread_create(&DB_1, NULL, Connect_DB_1, NULL);
	if (rc) {
		printf("ERROR; return code from pthread_create() is %d\n", rc);
        return 0;
    }

    rc = pthread_create(&DB_2, NULL, Connect_DB_2, NULL);
    if (rc) {
        printf("ERROR; return code from pthread_create() is %d\n", rc);
        return 0;
    }

    pthread_join(DB_1, NULL);
    pthread_join(DB_2, NULL);
    printf("main is done!\n");
	return 0;
}
