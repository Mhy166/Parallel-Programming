#include <iostream>
#include <fstream>
#include <vector>
#include <set>
#include <sstream>
#include <string>
#include <Windows.h>
//baseline-1²âÊÔÎÄ¼þ
using namespace std;

vector<vector<uint32_t>> arrays;
set<uint32_t> res;
vector<bool> Flag;

bool compareBySize(uint32_t& a,uint32_t& b) {
    return arrays[a].size() != arrays[b].size()?arrays[a].size() < arrays[b].size():a<b;
}
int main() {
    ifstream file("ExpIndex", ios::binary);
    if (!file) {
        cerr << "Failed to open the file." <<endl;
        return 1;
    }

    uint32_t arrayLength;
    while (file.read(reinterpret_cast<char*>(&arrayLength), sizeof(arrayLength))) {
        std::vector<uint32_t> array(arrayLength);
        file.read(reinterpret_cast<char*>(array.data()), arrayLength * sizeof(uint32_t));
        arrays.push_back(array);
    }
    file.close();

    ifstream queryFile("ExpQuery");
    if (!queryFile) {
        cerr << "Failed to open the query file." << endl;
        return 1;
    }

    ofstream resultFile("Baseline");
    if (!resultFile) {
        cerr << "Failed to create the result file." << endl;
        return 1;
    }
    LARGE_INTEGER freq, start, end0,s1,t1;
    QueryPerformanceFrequency(&freq);
    string line;
    int queryCount = 1;
    QueryPerformanceCounter(&s1);
    while (getline(queryFile, line)) {

        uint32_t k = 0;
        stringstream ss(line);
        vector<uint32_t> queryIndices;
        uint32_t row_idx;
        bool flag = true;
        // Read and sort the indices for this query
        while (ss >> row_idx) {
            queryIndices.push_back(row_idx);
            k++;
        }
        sort(queryIndices.begin(), queryIndices.end(),compareBySize);
        QueryPerformanceCounter(&start); // Record the start time of the query
        // Compute intersection of arrays based on sorted indices
        for (auto idx : queryIndices) {
            if (flag) {
                for (auto elem : arrays[idx]) {
                    res.insert(elem);
                    Flag.push_back(true);
                }
                flag = false;
            } else {
                vector<uint32_t> unfound;
                for (auto elem : res) {
                    bool flag0=false;
                    for (size_t i = 0; i < arrays[idx].size(); i++) {
                        if (elem==arrays[idx][i]) {
                            flag0 = true;
                            break;
                        }
                    }
                    if(!flag0){
                        unfound.push_back(elem);
                    }
                }
                for (size_t i = 0; i < unfound.size(); i++) {
                    res.erase(unfound[i]);
                }
            }
        }
        QueryPerformanceCounter(&end0); // Record the end time of the query
        double elapsedSeconds = static_cast<double>(end0.QuadPart - start.QuadPart) / freq.QuadPart;
        resultFile <<elapsedSeconds <<endl;
        //cout<<res.size()<<endl;
        queryCount++;
        res.clear();
        Flag.clear();
    }
    QueryPerformanceCounter(&t1);
    double elapsedSeconds = static_cast<double>(t1.QuadPart - s1.QuadPart) / freq.QuadPart;
    resultFile <<elapsedSeconds <<endl;
    queryFile.close();
    resultFile.close();
    return 0;
}
