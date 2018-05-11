#include <iostream>
#include <fstream>
#include <cstdlib>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>
#include <fcntl.h>
#include <math.h>
#include <set>
#include <map>
#include <vector>
#include <string>
#include <sstream>
#include <algorithm>

using namespace std;

// Constants
#define KB 1024L
#define GB (KB*KB*KB)
#define NODE_SIZE (32*KB)
#define TEMP_FILE "/home/aarati/workspace/CS736/ssd-measurements/temp.txt"

// Parameters
#define RATIO_GOOD_OFFSET 0// Ratio of popular nodes at good offsets
#define NODE_FILE_SIZE (10*GB)
#define NODE_FILE_NAME "/home/aarati/workspace/CS736/ssd-measurements/test_final"
#define WORKLOAD_FILE_NAME "/mnt/hdd/workloads/no_inserts.txt"

// We have one good node position in 256KB. 
// We want to divide the file into 256KB blocks.
#define BLOCK_SIZE (256*KB)
#define NUM_BLOCKS (NODE_FILE_SIZE/BLOCK_SIZE)

// Each block is in turn divided into smaller chunks (less than the NODE_SIZE)
#define BLOCK_CHUNK_SIZE (4*KB)
#define NUM_CHUNKS_IN_BLOCK (BLOCK_SIZE/BLOCK_CHUNK_SIZE)
#define NODE_SIZE_IN_CHUNKS (NODE_SIZE/BLOCK_CHUNK_SIZE)

// Number of valid (chunk) offsets in a block
#define NUM_VALID_OFFSETS_IN_BLOCK 5
#define NUM_BAD_OFFSETS_IN_BLOCK 4
#define BAD_OFFSETS_IN_BLOCK {4, 20, 35, 53}
#define NUM_GOOD_OFFSETS_IN_BLOCK 1
#define GOOD_OFFSET_IN_BLOCK 12

int getNextUnmappedNode(int nodeId, int numNodes, map<int, int>& nodeMapping) {
    int i;
    for (i=nodeId; i<numNodes; i++) {
        if (nodeMapping[i] == -1) {
            return i;
        }
    }
    // Invalid node id
    return -1;
}

long getTimeDiffMicroSec(struct timeval startTime, struct timeval endTime) {
        return (long)((endTime.tv_sec - startTime.tv_sec)*1000000 +
                (endTime.tv_usec - startTime.tv_usec));
}

void generateMapping(int numNodes, set<int>& popularNodes, map<int, int>& nodeMapping) {
    vector<int> goodOffsets, badOffsets;
    vector<int> popularNodesVector(popularNodes.begin(), popularNodes.end());
    // Round-about way of initializing
    int badOffsetsTemp[] = BAD_OFFSETS_IN_BLOCK;
    set<int> badOffsetsInBlock(badOffsetsTemp, badOffsetsTemp+NUM_BAD_OFFSETS_IN_BLOCK);
    set<int>::iterator setIt;
    vector<int>::iterator vectorItPopularNodes, vectorItGoodOffsets, vectorItBadOffsets;
    int i, j;
    int nodeId;
    int numPopularNodesAtGoodOffsets = int(popularNodes.size()*RATIO_GOOD_OFFSET);

    // Initialize nodeMapping with invalid values
    for (i=0; i<numNodes; i++) {
        nodeMapping[i] = -1;
    }

    for (i=0; i<NUM_BLOCKS; i++) {
        for(setIt=badOffsetsInBlock.begin(); setIt!=badOffsetsInBlock.end(); setIt++) {
            badOffsets.push_back(i*NUM_CHUNKS_IN_BLOCK + *(setIt));
        }
        goodOffsets.push_back(i*NUM_CHUNKS_IN_BLOCK + GOOD_OFFSET_IN_BLOCK);
    }
    // Shuffle the two vectors
    random_shuffle(badOffsets.begin(), badOffsets.end());
    random_shuffle(goodOffsets.begin(), goodOffsets.end());
    random_shuffle(popularNodesVector.begin(), popularNodesVector.end());

    // Assign good offsets first
    for(i=0, vectorItPopularNodes=popularNodesVector.begin(), vectorItGoodOffsets=goodOffsets.begin(); 
        i<numPopularNodesAtGoodOffsets && vectorItPopularNodes!=popularNodesVector.end() && vectorItGoodOffsets!=goodOffsets.end(); 
        i++, vectorItPopularNodes++, vectorItGoodOffsets++) {
        nodeMapping[*(vectorItPopularNodes)] = *(vectorItGoodOffsets);
    }

    // Assign bad offsets to remaining nodes
    for(vectorItBadOffsets=badOffsets.begin();
        vectorItPopularNodes!=popularNodesVector.end() && vectorItBadOffsets!=badOffsets.end();
        vectorItPopularNodes++, vectorItBadOffsets++) {
        nodeMapping[*(vectorItPopularNodes)] = *(vectorItBadOffsets);
    }

    // Assign the remaining bad offsets to unpopular nodes
    nodeId = 0;
    for (;vectorItBadOffsets!=badOffsets.end(); vectorItBadOffsets++) {
        nodeId = getNextUnmappedNode(nodeId, numNodes, nodeMapping);
        if (nodeId < 0) {
            // All nodes have been mapped
            return;
        }
        nodeMapping[nodeId] = (*vectorItBadOffsets);
    }

    // Assign the remaining good offsets to unpopular nodes
    for (; vectorItGoodOffsets!=goodOffsets.end(); vectorItGoodOffsets++) {
        nodeId = getNextUnmappedNode(nodeId, numNodes, nodeMapping);
        if (nodeId < 0) {
            // All nodes have been mapped
            return;
        }
        nodeMapping[nodeId] = *(vectorItGoodOffsets);
    }
    // There might be some unmapped nodes
}

void polluteSSDCache(int fd, void* buf, int numReads=30000) {
    for (int i=0; i<numReads; i++) {
        read(fd, (char*)buf, 4*KB);
    }
    fsync(fd);
}

int main() {
    ifstream workloadFile(WORKLOAD_FILE_NAME);
    int nodeFileFD, fdPollute;
    int numNodes, numPopularNodes;
    int i, j;
    set<int> popularNodes;
    string command;
    char commandType;
    int numCommands;
    long executionTime = 0;
    struct timeval startTime, endTime;
    int nodeId;
    long offset;
    void* buf;
    void* bufPollute;

    // For sanity checking
    // ofstream temp(TEMP_FILE);

    // Map of the nodes, to (chunk) offsets in the Node file
    map<int, int> nodeMapping;

    nodeFileFD = open(NODE_FILE_NAME, O_RDWR | O_DIRECT);
    if (nodeFileFD < 0) {
        cout << "Failed to open node file" << endl;
        exit(1);
    }

    if (!workloadFile.is_open()) {
        cout << "Couldn't open workload file" << endl;
        exit(1);
    }

    if (posix_memalign(&buf, 4*KB, NODE_SIZE) != 0) {
        cout << "posix_memalign failed" << endl;
        exit(1);
    }

    if (posix_memalign(&bufPollute, 4*KB, 4*KB) != 0) {
        cout << "Posix memalign failed" << endl;
        exit(1);
    }

    fdPollute = open("/home/aarati/workspace/CS736/ssd-measurements/pollute", O_RDWR | O_DIRECT);
    if  (fdPollute < 0) {
        cout << "Failed to open pollute file" << endl;
        exit(1);
    }
    // Load data from workload file
    workloadFile >> numNodes;
    workloadFile >> numPopularNodes;

    // Loading popular nodes from the workload file
    for (i=0; i<numPopularNodes; i++) {
        workloadFile >> j;
        popularNodes.insert(j);
    }

    // Sanity check
    if (popularNodes.size() != numPopularNodes) {
        cout << "Invalid workload file; not enough popular nodes" << endl;
        exit(1); 
    }

    // Generate a good mapping, such that popular nodes are mapped to good locations
    generateMapping(numNodes, popularNodes, nodeMapping);

    // Sanity check the mappings
    // ofstream temp("/home/aarati/workspace/CS736/ssd-measurements/temp.txt");
    // if (!temp.is_open()) {
    //     cout << "Couldn't open file!" << endl;
    //     exit(0);
    // }
    // map<int, int>::iterator mapIt;
    // for (mapIt=nodeMapping.begin(); mapIt!=nodeMapping.end(); mapIt++) {
    //     temp << "Node ID: " << mapIt->first << ": " << mapIt->second << endl;
    // }

    // One initial getline to consume the new line after the last integer read
    getline(workloadFile, command);

    // Execute the workload
    numCommands = 0;
    while(getline(workloadFile, command)) {
        stringstream commandStream(command);
        commandStream >> commandType;
        commandStream >> nodeId;
        offset = nodeMapping[nodeId]*BLOCK_CHUNK_SIZE;
        // Unmapped node! Shoulnd't arise, but just in case
        if (offset < 0) {
            continue;
        }
        // // Sanity check the offset, all of these should be 12 for this workload
        // long offset_temp = (offset - int(offset/BLOCK_SIZE)*BLOCK_SIZE)/BLOCK_CHUNK_SIZE;
        // temp << offset_temp << endl;
        lseek(nodeFileFD, (off_t)offset, SEEK_SET);

        // Start time
        gettimeofday(&startTime, NULL);

        if (commandType == 'R') {
            read(nodeFileFD, (char*)buf, NODE_SIZE);
        } else {
            write(nodeFileFD, (char*)buf, NODE_SIZE);
        }
        fsync(nodeFileFD);

        // End time
        gettimeofday(&endTime, NULL);
        executionTime += getTimeDiffMicroSec(startTime, endTime);

        // Pollute SSD cache every 100 commands
        numCommands++;
        if (numCommands == 100) {
            numCommands = 0;
            // cout << "Polluting SSD cache" << endl;
            polluteSSDCache(fdPollute, bufPollute, 100000);
        }
    }
    cout << "Total execution time (usec): " << executionTime << endl;
    close(fdPollute);
    return 0;
}
