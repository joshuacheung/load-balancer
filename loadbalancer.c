#include <sys/socket.h>
#include <sys/types.h>
#include <err.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <pthread.h>
#include <time.h>
#include <stdio.h>
#include <stdbool.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

// message variables
char *successString = (char *)" 200 OK\r\n";
char *notFoundString = (char *)" 404 Not found\r\n";
char *forbiddenString = (char *)" 403 Forbidden\r\n";
char *badRequestString = (char *)" 400 Bad request\r\n";
char *internalMessage = (char*)"500 internal server error";

// server booleans 
int numDownServers;
bool healthRequest = false;
bool makingHealthRequest = false;
bool serversDown = false;
bool needThreads = false;
bool serversDownTimeout = false;

// function implicit declarations
int client_connect(uint16_t connectport);
int server_listen(int port);
int bridge_connections(int fromfd, int tofd);
void bridge_loop(int sockfd1, int sockfd2);
int findServerLeastLoad(int entries[50], int errors[50]);
void *healthcheck(void *thread_pkg);
void *dispatch(void *q);


// array for thread and server indexes
int numReq[100] = {0};
int numErrors[100] = {0};
int currentServer[100] = {0};
int indexServer[100] = {0};
int indexErrors[100] = {0};

// default amount of threads
#define DEFAULT_POOL_SIZE 4

// thread variables 
pthread_mutex_t serverMutex = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t queueMutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t serverQueue = PTHREAD_COND_INITIALIZER;
pthread_cond_t queueConditional = PTHREAD_COND_INITIALIZER;
pthread_cond_t serverConditionalQueue = PTHREAD_COND_INITIALIZER;
pthread_cond_t serverConditional = PTHREAD_COND_INITIALIZER;

/*
* Use a linked list queue structure to enqueue and dequeue requests
* Adapted from source: https://www.geeksforgeeks.org/queue-linked-list-implementation/?fbclid=IwAR0GUHBrDLP5JEGP6VHljS3GJKKQ5eeEnFjSytcoRGUH4A80LN8VtEmsHl4
*/

typedef struct QNode
{
    int *key;
    struct QNode *next;
} QNode;

QNode *front, *rear = NULL;

struct QNode* newNode() 
{ 
    struct QNode* temp = (struct QNode*)malloc(sizeof(QNode)); 
    temp->next = NULL; 
    return temp; 
} 

void enQueue(int *k)
{
    // make node containing request 
    QNode *newnode = (QNode *)malloc(sizeof(QNode));
    newnode->key = k;
    newnode->next = NULL;

    // if reached end of list
    if (rear == NULL)
    {
        front = newnode;
        rear = newnode;
        return;
    }

    rear->next = newnode;
    rear = newnode;
}

// create the queue
struct Queue* createQueue() 
{ 
    struct Queue* q = (struct Queue*)malloc(sizeof(char)); 
    // q->front = q->rear = NULL; 
    return q; 
} 


QNode *deQueue()
{
    // If queue is empty, return NULL.
    if (front == NULL)
    {
        return NULL;
    }
    else
    {
        QNode *temp = front;
        front = front->next;
        if (front == NULL)
            rear = NULL;

        return temp;
    }
}

// passing needed info to worker threads 
typedef struct threadArguments
{
    bool isRunning;
    bool maxLoad;
    int tofd;
    int numServers;
    int lowestLoad;
    int servers[100];
} ThreadArguments;

// passing needed info to healthcheck threads 
typedef struct maxLoadArguments
{
    bool isCurrentMaxLoad;
    int numIndex;
    int numServers;
    int maxLoadIndex;
} MaxLoadArguments;

typedef struct currentHealthArguments
{
    bool isCurrentMaxLoad;
    int numIndex;
    int numServers;
    int maxLoadIndex;
} currentHealthArguments;


// passing needed info to healthcheck threads 
typedef struct healthcheckArguments
{
    bool isRunning;
    bool maxLoad;
    int servers[100];
    int numServers;
} HealthcheckArguments;


int main(int argc, char **argv)
{
    // server int variables
    int listenfd;
    int acceptfd;
    int *new_cd = malloc(sizeof(int));
    int opt;
    int server_ports[100];
    int numHealthChecks = 0;
    int maxRequests = 0;
    int numServers = 0;

    uint16_t clientport;
    
    // server index buffers
    bool secondArg = false;
    bool initialArg = true;
    bool isDefault = false;
    char healthcheck_buffer[100];
    char healthcheck_response[50];

    // set to default threads initially
    ssize_t numThreads = DEFAULT_POOL_SIZE;

    if (argc < 3)
    {
        printf("missing arguments: usage %s port_to_connect port_to_listen", argv[0]);
        return 1;
    }

    if(secondArg == false)
    {
        printf("Second arg not present\n");
    }

    // parsing commandline arguments with getopt
    // adapted from source: https://www.geeksforgeeks.org/getopt-function-in-c-to-parse-command-line-arguments/
    while ((opt = getopt(argc, argv, "N:R:")) != -1)
    {
        switch (opt)
        {
        case 'N':
            if (optarg == NULL)
            {
                numThreads = DEFAULT_POOL_SIZE;
                isDefault = true;
                
            }
            else
            {
                numThreads = atoi(optarg);
                isDefault = false;
            }

            break;
        case 'R':
            if (optarg == NULL)
            {
                err(-1, "-R requires integer argument\n");
            }
            else
            {
                numHealthChecks = atoi(optarg);
            }

            break;
        }
    }

    if(isDefault)
    {
        printf("using default amount of threads\n");
    }
    
    // get excess arguments not preceded by flags
    for (; optind < argc; optind++)
    {
        if (initialArg)
        {
            isDefault = false;
            initialArg = false;
            clientport = atoi(argv[optind]);
        }
        else
        {
            isDefault = true;
            server_ports[numServers] = atoi(argv[optind]);
            ++numServers;
        }
    }

    pthread_mutex_lock(&serverMutex);
    for (int i = 0; i < numServers; ++i)
    {
        char curr_entries[100];
        char curr_err[100];
        int int_entry;
        int int_err;

        sprintf(healthcheck_buffer, "GET /healthcheck HTTP/1.1\r\nHost: localhost:%d\r\nUser-Agent: curl/7.58.0\r\nAccept: */*\r\n\r\n", server_ports[i]);

        int h_sock = client_connect(server_ports[i]);
        if (h_sock < 0)
        {
            ++numDownServers;
            continue;
        }
        else
        {
            // for each server we send a healthcheck response
            send(h_sock, healthcheck_buffer, strlen(healthcheck_buffer), 0);
            recv(h_sock, healthcheck_response, sizeof(healthcheck_response), 0);
            if(isDefault)
            {
                // printf("default value being set correctly\n");
            }
            serversDown = false;
            sscanf(healthcheck_response, "%*s %*s %*s %*s %*s %s", curr_err);
            sscanf(healthcheck_response, "%*s %*s %*s %*s %*s %*s %s", curr_entries);

            sscanf(curr_entries, "%d", &int_entry);
            sscanf(curr_err, "%d", &int_err);

            numReq[i] = int_entry;
            numErrors[i] = int_err;
            pthread_cond_signal(&serverConditional);
        }
    }
    if (numDownServers == numServers)
    {
        if(isDefault)
        {
            // printf("still reaching default\n");
        }
        serversDown = true;
        serversDownTimeout = true;
    }
    
    pthread_mutex_unlock(&serverMutex);

    pthread_t thread_pool[numThreads];
    pthread_t healthcheck_thread;
    HealthcheckArguments h_arg;


    // printf("number of args is\n");
    // printf("reached heren\n");

    h_arg.numServers = numServers;
    memcpy(h_arg.servers, server_ports, numServers * sizeof(int));
    pthread_create(&healthcheck_thread, NULL, healthcheck, &h_arg);

    ThreadArguments args[numThreads];
    for (int i = 0; i < numThreads; ++i)
    {
        memcpy(args[i].servers, server_ports, numServers * sizeof(int));
        ++maxRequests;
        args[i].numServers = numServers;
        pthread_create(&thread_pool[i], NULL, dispatch, &args[i]);
    }


    // Remember to validate return values
    // You can fail tests for not validating

    //listening for client
    if ((listenfd = server_listen(clientport)) < 0)
        err(1, "failed listening");

    int request_count = 0;
    char *server_error = (char *)"HTTP/1.1 500 Internal Server Error\r\n\r\n";

    while (true)
    {
        if ((acceptfd = accept(listenfd, NULL, NULL)) < 0)
            err(1, "failed accepting");
        if ((serversDownTimeout == true) || (serversDown == true))
        {
            int size_sent = send(acceptfd, server_error, strlen(server_error), 0);
            printf("size sent: %d\n", size_sent);
            close(acceptfd);
        }

        ++request_count;
        // printf("reached end of following function\n");

        // printf("reached end of following function\n");

        // printf("reached end of following function\n");

        if (numHealthChecks > 0)
        {
            if(isDefault)
            {
                // printf("reaching here,trying to fix bug\n");
            }
            if ((request_count % numHealthChecks == 0))
            {
                ++maxRequests;
                isDefault = true;
                pthread_mutex_lock(&serverMutex);
                healthRequest = true;
                pthread_mutex_unlock(&serverMutex);
            }
        }
        *new_cd = acceptfd;
        pthread_mutex_lock(&queueMutex);
        ++maxRequests;
        enQueue(new_cd);
        // printf("reached end of following function\n");


        // printf("reached end of following function\n");

        // printf("reached end of following function\n");
        pthread_cond_signal(&queueConditional);
        pthread_mutex_unlock(&queueMutex);
    }
}



/*
 * client_connect takes a port number and establishes a connection as a client.
 * connectport: port number of server to connect to
 * returns: valid socket if successful, -1 otherwise
 */
int client_connect(uint16_t connectport)
{
    int connfd;
    struct sockaddr_in servaddr;

    connfd = socket(AF_INET, SOCK_STREAM, 0);
    if (connfd < 0)
        return -1;
    memset(&servaddr, 0, sizeof servaddr);

    servaddr.sin_family = AF_INET;
    servaddr.sin_port = htons(connectport);

    /* For this assignment the IP address can be fixed */
    inet_pton(AF_INET, "127.0.0.1", &(servaddr.sin_addr));

    if (connect(connfd, (struct sockaddr *)&servaddr, sizeof(servaddr)) < 0)
        return -1;
    return connfd;
}

/*
 * server_listen takes a port number and creates a socket to listen on 
 * that port.
 * port: the port number to receive connections
 * returns: valid socket if successful, -1 otherwise
 */
int server_listen(int port)
{
    int listenfd;
    int enable = 1;
    struct sockaddr_in servaddr;

    listenfd = socket(AF_INET, SOCK_STREAM, 0);
    if (listenfd < 0)
        return -1;
    memset(&servaddr, 0, sizeof servaddr);
    servaddr.sin_family = AF_INET;
    servaddr.sin_addr.s_addr = htons(INADDR_ANY);
    servaddr.sin_port = htons(port);

    if (setsockopt(listenfd, SOL_SOCKET, SO_REUSEADDR, &enable, sizeof(enable)) < 0)
        return -1;
    if (bind(listenfd, (struct sockaddr *)&servaddr, sizeof servaddr) < 0)
        return -1;
    if (listen(listenfd, 500) < 0)
        return -1;
    return listenfd;
}

/*
 * bridge_connections send up to 100 bytes from fromfd to tofd
 * fromfd, tofd: valid sockets
 * returns: number of bytes sent, 0 if connection closed, -1 on error
 */

int bridge_connections(int fromfd, int tofd)
{
    char recvline[100];
    int n = recv(fromfd, recvline, 100, 0);
    if (n < 0)
    {
        printf("connection error receiving\n");
        return -1;
    }
    else if (n == 0)
    {
        //printf("receiving connection ended\n");
        return 0;
    }
    recvline[n] = '\0';
    // removed initial sleep to add own timer
    n = send(tofd, recvline, n, 0);
    if (n < 0)
    {
        printf("connection error sending\n");
        return -1;
    }
    else if (n == 0)
    {
        printf("sending connection ended\n");
        return 0;
    }
    return n;
}

/*
 * bridge_loop forwards all messages between both sockets until the connection
 * is interrupted. It also prints a message if both channels are idle.
 * sockfd1, sockfd2: valid sockets
 */


void bridge_loop(int sockfd1, int sockfd2)
{
    fd_set set;
    struct timeval timeout;
    int fromfd, tofd;
    while (1)
    {
        // set for select usage must be initialized before each select call
        // set manages which file descriptors are being watched
        FD_ZERO(&set);
        FD_SET(sockfd1, &set);
        FD_SET(sockfd2, &set);

        // same for timeout
        // max time waiting, 5 seconds, 0 microseconds
        timeout.tv_sec = 5;
        timeout.tv_usec = 0;

        // select return the number of file descriptors ready for reading in set
        switch (select(FD_SETSIZE, &set, NULL, NULL, &timeout))
        {
        case -1:
            printf("error during select, exiting\n");
            return;
        case 0:
            printf("both channels are idle, waiting again\n");
            continue;
        default:
            if (FD_ISSET(sockfd1, &set))
            {
                fromfd = sockfd1;
                tofd = sockfd2;
            }
            else if (FD_ISSET(sockfd2, &set))
            {
                fromfd = sockfd2;
                tofd = sockfd1;
            }
            else
            {
                printf("this should be unreachable\n");
                return;
            }
        }
        if (bridge_connections(fromfd, tofd) <= 0)
        {
            //printf("bridge connection terminated\n");
            return;
        }
    }
}

/* 
    Connect each thread to server, and when successful, will dequeue a request from a queue, and then call bridgeloop
    to perform the request
 */


void *dispatch(void *q)
{
    ThreadArguments *threadArgs = (ThreadArguments *)q;
    // int errorPorts[100];
    // bool isErrorPort = false;
    int dispatchPort;
    int serverports[100];
    int num_servers = threadArgs->numServers;
    memcpy(serverports, threadArgs->servers, num_servers * sizeof(int));
    int connfd;

    while (true)
    {

        int currentServerIndex = 0;
        // int numErrorIndex = 0;
        // int numErrorsTotal = 0;
        // if (!isErrorPort)
        // {
        //     ++numErrorIndex;
        //     ++numErrorsTotal;
        //     // printf("reached is error port\n");
        // }

        pthread_mutex_lock(&serverMutex);
        pthread_cond_wait(&serverConditional, &serverMutex);

        int min_load_index = findServerLeastLoad(numReq, numErrors);
        dispatchPort = serverports[min_load_index];
        ++numReq[min_load_index];
        pthread_mutex_unlock(&serverMutex);

        if ((connfd = client_connect(dispatchPort)) < 0)
        {
            err(1, "failed connecting");
        }
        pthread_mutex_lock(&queueMutex);
        QNode *pclient = deQueue();
        if (pclient == NULL)
        {
            pthread_cond_wait(&queueConditional, &queueMutex);
            pclient = deQueue();
        }

        pthread_mutex_unlock(&queueMutex);

        // printf("locked mutex before cheking null\n");
        // printf("locked mutex before cheking null\n");

        // printf("locked mutex before cheking null\n");

        // printf("locked mutex before cheking null\n");


        if (pclient != NULL)
        {
            // ++numErrorIndex;
            // ++numErrorsTotal;
            int *acceptsocket = pclient->key;
            int acceptfd = *acceptsocket;
            bridge_loop(acceptfd, connfd);
        }

        // printf("reached end of dispatch before lock\n");
        pthread_mutex_lock(&serverMutex);
        currentServer[currentServerIndex] = 0;
        pthread_mutex_unlock(&serverMutex);

        // printf("reached end of dispatch after lock\n");

    }

    return NULL;
}


/* 
    every 2 seconds or reached r number of requests, will send a healthcheck GET request to server
 */

void *healthcheck(void *thread_pkg)
{
    HealthcheckArguments *threadArgument = (HealthcheckArguments *)thread_pkg;
    int servers[100];
    int num_servers = threadArgument->numServers;
    memcpy(servers, threadArgument->servers, num_servers * sizeof(int));
    char healthcheck_buffer[100];
    char healthcheck_response[50];

    // timeout adapted from starter code 
    struct timeval timeout;

    while (true)
    {
        timeout.tv_sec = 2;
        timeout.tv_usec = 0;
        numDownServers = 0;
        // https://stackoverflow.com/questions/9798948/usage-of-select-for-timeout 
        // used above link to use select to timeout
        if ((select(0, NULL, NULL, NULL, &timeout) == 0) || healthRequest == true)
        {
            
            pthread_mutex_lock(&serverMutex);

            for (int i = 0; i < num_servers; ++i)
            {
                char curr_entries[100];
                int int_entry;
                char curr_err[100];
                int int_err;

                sprintf(healthcheck_buffer, "GET /healthcheck HTTP/1.1\r\nHost: localhost:%d\r\nUser-Agent: curl/7.58.0\r\nAccept: */*\r\n\r\n", servers[i]);

                int h_sock = client_connect(servers[i]);
                if (h_sock < 0)
                {
                    ++numDownServers;
                    continue;
                }
                else
                {
                    send(h_sock, healthcheck_buffer, strlen(healthcheck_buffer), 0);
                    while (true)
                    {
                        int healthcheck_reply = recv(h_sock, healthcheck_response, sizeof(healthcheck_response), 0);
                        if (healthcheck_reply == 0)
                        {
                            break;
                        }
                    }

                    sscanf(healthcheck_response, "%*s %*s %*s %*s %*s %s", curr_err);
                    sscanf(healthcheck_response, "%*s %*s %*s %*s %*s %*s %s", curr_entries);

                    sscanf(curr_entries, "%d", &int_entry);
                    sscanf(curr_err, "%d", &int_err);

                    numReq[i] = int_entry;
                    numErrors[i] = int_err;
                    pthread_cond_signal(&serverConditional);
                }
            }
            if (numDownServers == num_servers)
            {
                serversDown = true;
                serversDownTimeout = true;
            }
            

            healthRequest = false;
            pthread_mutex_unlock(&serverMutex);
        }
    }

    return NULL;
}


// return the server index that has the least load, --> least amount of requests 
// assuming that 50 is the maximum amount of servers present 


int findServerLeastLoad(int entries[50], int errors[50])
{
    int min = 5000000;
    int err_min = 5000000;
    int min_index = 0;

    for (int i = 0; i < 50; ++i)
    {
        if (entries[i] <= min && entries[i] != 0)
        {
            min = entries[i];
            min_index = i;
            if (errors[i] < err_min && entries[i] == min)
            {
                err_min = errors[i];
            }
        }

    }

    return min_index;
}


// return the server index that has the least load, --> least amount of requests 
// assuming that 50 is the maximum amount of servers present 


int findServerMaxLoad(int entries[50], int errors[50])
{
    int max = 5000000;
    int err_min = 0;
    int max_index = 50000;

    for (int i = 0; i < 50; ++i)
    {
        if (entries[i] <= max && entries[i] != 0)
        {
            max = entries[i];
            max_index = i;
            if (errors[i] > err_min && entries[i] == max)
            {
                err_min = errors[i];
            }
        }

    }

    return max_index;
}


