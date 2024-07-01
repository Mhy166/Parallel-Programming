#include <iostream>
#include<bits/stdc++.h>
#include <fstream>
#include <sstream>
#include <Windows.h>
using namespace std;

vector<vector<uint32_t>> arrays;
set<uint32_t> res;
vector<bool> Flag;

bool compareBySize(uint32_t& a, uint32_t& b) {
    return arrays[a].size() != arrays[b].size() ? arrays[a].size() < arrays[b].size() : a < b;
}
bool cmp(list<uint32_t>& a, list<uint32_t>& b) {
    return a.size() < b.size();
}
int main() {
    ifstream file("ExpIndex", ios::binary);
    if (!file) {
        cerr << "Failed to open the file." << endl;
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

    ofstream resultFile("Baseline-2");
    if (!resultFile) {
        cerr << "Failed to create the result file." << endl;
        return 1;
    }
    LARGE_INTEGER freq, start, end0,start1,end1;
    QueryPerformanceFrequency(&freq);
    QueryPerformanceCounter(&start1);
    string line;
    int queryCount = 1;
    while (getline(queryFile, line)) {

        uint32_t k = 0;
        stringstream ss(line);
        vector<uint32_t> queryIndices;
        uint32_t row_idx;
        while (ss >> row_idx) {
            queryIndices.push_back(row_idx);
            k++;
        }
        sort(queryIndices.begin(), queryIndices.end(), compareBySize);

        vector<list<uint32_t>> lists(queryIndices.size());
        for (uint32_t i = 0; i < queryIndices.size(); i++) {
            for (uint32_t j = 0; j < arrays[queryIndices[i]].size(); j++) {
                lists[i].push_back(arrays[queryIndices[i]][j]);
            }
        }
        QueryPerformanceCounter(&start);
        while (!lists[0].empty()) {
            uint32_t tmp = lists[0].front();
            uint32_t cnt = 1;
            bool flag = false;
            lists[0].pop_front();
            for (uint32_t i = 1; i < lists.size(); i++) {
                while (true) {
                    if (lists[i].empty()) {
                        flag = true;
                        break;
                    }
                    if (lists[i].front() < tmp) {
                        lists[i].pop_front();
                    }
                    else if (lists[i].front() == tmp) {
                        lists[i].pop_front();
                        cnt++;
                    }
                    else {
                        break;
                    }
                }
                if (flag) {
                    break;
                }
            }
            if (cnt == lists.size()) {
                res.insert(tmp);
            }
            sort(lists.begin(), lists.end(), cmp);
        }
        QueryPerformanceCounter(&end0); // Record the end time of the query
        double elapsedSeconds = static_cast<double>(end0.QuadPart - start.QuadPart) / freq.QuadPart;
        resultFile <<elapsedSeconds <<endl;
       // cout << res.size() << endl;
        queryCount++;
        res.clear();
        Flag.clear();
    }
    QueryPerformanceCounter(&end1);
    double elapsedSeconds = static_cast<double>(end1.QuadPart - start1.QuadPart) / freq.QuadPart;
    resultFile <<elapsedSeconds <<endl;
    queryFile.close();
    resultFile.close();
    return 0;
}
