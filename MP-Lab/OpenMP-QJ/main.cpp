#include <iostream>
#include <bits/stdc++.h>
#include <fstream>
#include <sstream>
#include <Windows.h>
#include <omp.h>
using namespace std;
int numThreads = 4;
vector<vector<uint32_t>> arrays;
vector<set<uint32_t>> res;
vector<bool> Flag;
vector<double> res_time;
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

    ofstream resultFile("omp-4-bw-gu");
    if (!resultFile) {
        cerr << "Failed to create the result file." << endl;
        return 1;
    }

    LARGE_INTEGER freq, start1, end1;
    QueryPerformanceFrequency(&freq);
    vector<string> queryLines;
    string line;
    while (getline(queryFile, line)) {
        queryLines.push_back(line);
    }
    res.resize(queryLines.size());
    for(uint32_t i=0;i<res.size();i++){
           res_time.push_back(0);
    }
    QueryPerformanceCounter(&start1);
    #pragma omp parallel for num_threads(numThreads),schedule(guided)
    for (int i = 0; i < queryLines.size(); i++) {
        LARGE_INTEGER start, end0;
        stringstream ss(queryLines[i]);
        vector<uint32_t> queryIndices;
        uint32_t row_idx;
        while (ss >> row_idx) {
            queryIndices.push_back(row_idx);
        }
        sort(queryIndices.begin(), queryIndices.end(), compareBySize);
        vector<list<uint32_t>> lists(queryIndices.size());
        for (uint32_t j = 0; j < queryIndices.size(); j++) {
            for (uint32_t k = 0; k < arrays[queryIndices[j]].size(); k++) {
                lists[j].push_back(arrays[queryIndices[j]][k]);
            }
        }

        QueryPerformanceCounter(&start);
        while (!lists[0].empty()) {
            uint32_t tmp = lists[0].front();
            uint32_t cnt = 1;
            bool flag = false;
            lists[0].pop_front();
            for (uint32_t j = 1; j < lists.size(); j++) {
                while (true) {
                    if (lists[j].empty()) {
                        flag = true;
                        break;
                    }
                    if (lists[j].front() < tmp) {
                        lists[j].pop_front();
                    } else if (lists[j].front() == tmp) {
                        lists[j].pop_front();
                        cnt++;
                    } else {
                        break;
                    }
                }
                if (flag) {
                    break;
                }
            }
            if (cnt == lists.size()) {
                res[i].insert(tmp);
            }
            sort(lists.begin(), lists.end(), cmp);
        }
        QueryPerformanceCounter(&end0);
        double elapsedSeconds = static_cast<double>(end0.QuadPart - start.QuadPart) / freq.QuadPart;
        #pragma omp critical
        {
            res_time[i]=elapsedSeconds;
        }
    }
    QueryPerformanceCounter(&end1);
    for(uint32_t i=0;i<res_time.size();i++){
          resultFile<<res_time[i]<<endl ;
    }
    double elapsedSeconds = static_cast<double>(end1.QuadPart - start1.QuadPart) / freq.QuadPart;
    resultFile <<elapsedSeconds<< endl;
    queryFile.close();
    resultFile.close();
    return 0;
}
