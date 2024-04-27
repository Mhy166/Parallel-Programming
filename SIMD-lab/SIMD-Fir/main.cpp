#include <iostream>
#include <fstream>
#include <vector>
#include <set>
#include <sstream>
#include <string>
#include <bitset>
#include <Windows.h>
#include <bits/stdc++.h>
//baseline-1测试文件
using namespace std;

vector<vector<uint32_t>> arrays;
bitset<25205180> res;
vector<bool> Flag;
vector<bitset<25205180>>bitmaps;

bool compareBySize(uint32_t& a, uint32_t& b) {
    return arrays[a].size() != arrays[b].size() ? arrays[a].size() < arrays[b].size() : a < b;
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


    for (uint32_t i = 0; i < arrays.size(); i++) {
        bitmaps.push_back(res);
        for (uint32_t j = 0; j < arrays[i].size(); j++) {
            bitmaps[i][arrays[i][j]] = 1;
        }
        //cout << bitmaps[i].count();
    }

    ifstream queryFile("ExpQuery");
    if (!queryFile) {
        cerr << "Failed to open the query file." << endl;
        return 1;
    }

    ofstream resultFile("SIMD-Not");
    if (!resultFile) {
        cerr << "Failed to create the result file." << endl;
        return 1;
    }

    LARGE_INTEGER freq, start, end0;
    QueryPerformanceFrequency(&freq);
    string line;
    int queryCount = 1;
    while (getline(queryFile, line)) {
        uint32_t k = 0;
        stringstream ss(line);
        vector<uint32_t> queryIndices;
        uint32_t row_idx;
        uint32_t col_nums = 0;
        bool flag = true;

        // Read and sort the indices for this query
        while (ss >> row_idx) {
            col_nums += arrays[row_idx].size();
            queryIndices.push_back(row_idx);
            k++;
        }
        sort(queryIndices.begin(), queryIndices.end(), compareBySize);
        col_nums=arrays[queryIndices[0]].size();
        QueryPerformanceCounter(&start); // Record the start time of the query
        // Compute intersection of arrays based on sorted indices
        for (auto idx : queryIndices) {
            if (flag) {
                res = bitmaps[idx];
                flag = false;
            }
            else {
                res &= bitmaps[idx];
            }
        }
        QueryPerformanceCounter(&end0); // Record the end time of the query

        double elapsedSeconds = static_cast<double>(end0.QuadPart - start.QuadPart) / freq.QuadPart;
        //resultFile << "第" << queryCount << "条查询结果：" << endl;

        //resultFile << res.count() << endl;

        //resultFile<<queryCount<<endl;
        resultFile <<  elapsedSeconds <<endl;
        //uint32_t cnt = 0;
        //resultFile << endl;
        //resultFile << k << endl;
        //resultFile << col_nums << endl;
        //resultFile << "结果列表长度：" << cnt << endl;

        queryCount++;
        res.reset();
        Flag.clear();
    }
    //resultFile << "总耗时：" << all_time << endl;
    queryFile.close();
    resultFile.close();

    return 0;
}
