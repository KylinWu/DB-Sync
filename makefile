CC = gcc
DEBUG = -g
CFLAG = -std=c99 -Wall -lpthread `mysql_config --cflags --libs`
OBJS = DB_Sync.o
EXE = DB_Sync

%.o: %.c
	$(CC) -c -o $@ $< $(CFLAG) $(DEBUG) 

default: $(OBJS)
	$(CC) -o $(EXE) $(OBJS) $(CFLAG) $(DEBUG)

clear:
	rm *.o $(EXE)
