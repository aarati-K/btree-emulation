#include <iostream>
#include <fstream>
#include <cstdlib>
#include <math.h>
#include <set>
#include <algorithm>

using namespace std;

// Constants
#define KB 1024L
#define GB (KB*KB*KB)
#define FILESIZE 1*GB

// B-Tree parameters
#define NUM_LEVELS 4
#define NODE_SIZE 32*KB
#define FANOUT 64

// Workload parameters
#define POPULAR_NODE_RATIO 0.1 // Ratio of nodes accessed 90% of the time (say)
#define RATIO_REQUESTS_TO_POPULAR_NODES 0.9 // Ratio of total requests to popular nodes
#define INSERT_RATIO 0.1 // Ratio of insert queries
#define FREQUENCY_OF_SPLITS 2000 // One node split every FREQUENCY_OF_SPLITS queries
#define TOTAL_SPLITS 1 // Can be used to control the total number of queries

#define FILENAME "/mnt/hdd/workloads/2000q_200i.txt"

void generateNodeSets(set<int>& popularNodes, set<int>& unpopularNodes, int numNodes) {
    int i, j;
    int numPopularNodes = int(numNodes*POPULAR_NODE_RATIO);
    int remainingPopularNodes;
    int nodeOffsetInGroup;

    // All the nodes in the first NUM_LEVELS-1 levels should be popular
    int numNodesInLastLevel = pow(FANOUT, NUM_LEVELS-1);
    int nodesInTopLevels = numNodes - numNodesInLastLevel;
    cout << "numPopularNodes: " << numPopularNodes << endl;
    cout << "nodesInTopLevels: " << nodesInTopLevels << endl;
    for (i=0; i<nodesInTopLevels; i++) {
        popularNodes.insert(i);
    }

    remainingPopularNodes = numPopularNodes - nodesInTopLevels;
    // Divide the nodes in the last level into remainingPopularNodes groups
    // Choose one node randomly from each group
    int nodeGroupSize = numNodesInLastLevel/remainingPopularNodes;
    for (i=nodesInTopLevels; i<numNodes; i+=nodeGroupSize) {
        nodeOffsetInGroup = rand() % nodeGroupSize;
        for (j=0; j<nodeGroupSize; j++) {
            if ((i+j) == numNodes) {
                // Sanity check
                cout << "Number of popular nodes: " << popularNodes.size() << endl;
                cout << "Number of unpopular nodes: " << unpopularNodes.size() << endl;
                return;
            }
            if (j==nodeOffsetInGroup) {
                popularNodes.insert(i+j);
            } else {
                unpopularNodes.insert(i+j);
            }
        }
    }
}

int getNodeAtPosition(set<int>& nodeList, int position) {
    set<int>::iterator it = nodeList.begin();
    advance(it, position);
    return *it;
}

void generateAncestors(int node, set<int>& ancestors) {
    if (ancestors.size() != 0) {
        cout << "Error: Non empty set of ancestors" << endl;
        exit(2);
    }

    for (int i=node; i>0; i=i/FANOUT) {
        ancestors.insert(i);
    }
    // root node is always present
    ancestors.insert(0);
}

void generateSearchWorkload(int node, ofstream& workloadFile) {
    set<int> ancestors;
    set<int>::iterator it;

    generateAncestors(node, ancestors);
    for (it=ancestors.begin(); it!=ancestors.end(); it++) {
        workloadFile << "R " << *it << endl;
    }
}

void generateInsertWorkload(int node, ofstream& workloadFile) {
    generateSearchWorkload(node, workloadFile);
    workloadFile << "W " << node << endl;
}

void generateSplitWorkload(int node, ofstream& workloadFile) {
    int i;
    int numAncestors;
    set<int> ancestors;
    set<int>::reverse_iterator it;

    generateSearchWorkload(node, workloadFile);
    generateAncestors(node, ancestors);
    numAncestors = ancestors.size();

    // Randomly decide how many nodes to split
    int numSplits = rand() % numAncestors + 1;
    for (it=ancestors.rbegin(), i=0; it!=ancestors.rend() && i<numSplits; it++, i++) {
        workloadFile << "W " << *it << endl;
    }
}

int main(int argc, char** argv) {
    int i, j;
    int numNodes = 0;
    int numPopularNodes, numUnpopularNodes;
    int numQueriesPopularNodes, numQueriesUnpopularNodes;
    int numInsertQueriesPopularNodes, numInsertQueriesUnpopularNodes;
    int randomNode;
    set<int> popularNodes, unpopularNodes;
    set<int>::iterator it;
    ofstream workloadFile(FILENAME);

    if (!workloadFile.is_open()) {
        cout << "Failed to open workload file" << endl;
        exit(1);
    }

    for (i=0; i<NUM_LEVELS; i++) {
        numNodes += pow(FANOUT, i);
    }

    // Sanity check
    cout << "Total number of nodes: " << numNodes << endl;
    workloadFile << numNodes << endl;

    // Generate the set of popular nodes
    generateNodeSets(popularNodes, unpopularNodes, numNodes);
    numPopularNodes = popularNodes.size();
    numUnpopularNodes = unpopularNodes.size();
    workloadFile << numPopularNodes << endl;
    for (it=popularNodes.begin(); it!=popularNodes.end(); it++) {
        workloadFile << *it << endl;
    }

    // Create queries in groups of size FREQUENCY_OF_SPLITS
    numQueriesPopularNodes = FREQUENCY_OF_SPLITS*RATIO_REQUESTS_TO_POPULAR_NODES;
    numQueriesUnpopularNodes = FREQUENCY_OF_SPLITS - numQueriesPopularNodes;
    numInsertQueriesPopularNodes = numQueriesPopularNodes*INSERT_RATIO;
    numInsertQueriesUnpopularNodes = numQueriesUnpopularNodes*INSERT_RATIO;

    cout << "numQueriesPopularNodes: " << numQueriesPopularNodes << endl;
    cout << "numInsertQueriesPopularNodes: " << numInsertQueriesPopularNodes << endl;
    cout << "numQueriesUnpopularNodes: " << numQueriesUnpopularNodes << endl;
    cout << "numInsertQueriesUnpopularNodes: " << numInsertQueriesUnpopularNodes << endl;

    // generate queries
    for (j=0; j<TOTAL_SPLITS; j++) {
	// Keep track
	cout << "Split count: " << j << endl;
        // Insert queries for popular nodes
        for (i=0; i<numInsertQueriesPopularNodes; i++) {
            randomNode = getNodeAtPosition(popularNodes, rand()%numPopularNodes);
            generateInsertWorkload(randomNode, workloadFile);
        }

        // Splitting a popular node
        randomNode = getNodeAtPosition(popularNodes, rand()%numPopularNodes);
        generateSplitWorkload(randomNode, workloadFile);

        // Search queries for popular nodes
        for (i=numInsertQueriesPopularNodes; i<numQueriesPopularNodes; i++) {
            randomNode = getNodeAtPosition(popularNodes, rand()%numPopularNodes);
            generateSearchWorkload(randomNode, workloadFile);
        }
        // Insert queries for unpopular nodes
        for (i=0; i<numInsertQueriesUnpopularNodes; i++) {
            randomNode = getNodeAtPosition(unpopularNodes, rand()%numUnpopularNodes);
            generateInsertWorkload(randomNode, workloadFile);
        }
        // search queries for unpopular nodes
        for (i=numInsertQueriesUnpopularNodes; i<numQueriesUnpopularNodes; i++) {
            randomNode = getNodeAtPosition(unpopularNodes, rand()%numUnpopularNodes);
            generateSearchWorkload(randomNode, workloadFile);
        }
    }
    
    workloadFile.close();
    return 0;
}
