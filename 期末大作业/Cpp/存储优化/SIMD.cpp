#include <iostream>
#include <fstream>
#include <vector>
#include <set>
#include <sstream>
#include <string>
#include <bitset>
#include <Windows.h>
#include <bits/stdc++.h>
#include <immintrin.h> // Include AVX header
#include <emmintrin.h> // Include SSE header
//baseline-1测试文件
using namespace std;
const uint32_t index_num = 196916;//196916*128=25205248
vector<vector<uint32_t>> arrays;
bitset<25205248> res;
vector<bool> Flag;
vector<bitset<25205248>>bitmaps;

bool compareBySize(uint32_t& a, uint32_t& b) {
    return arrays[a][arrays[a].size()-1] != arrays[b][arrays[b].size() - 1] ? arrays[a][arrays[a].size() - 1] < arrays[b][arrays[b].size() - 1] : a < b;
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
    }

    ifstream queryFile("ExpQuery");
    if (!queryFile) {
        cerr << "Failed to open the query file." << endl;
        return 1;
    }
    ofstream resultFile("SIMD-SSE");
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
        bool flag = true;

        // Read and sort the indices for this query
        while (ss >> row_idx) {
            queryIndices.push_back(row_idx);
            k++;
        }
        sort(queryIndices.begin(), queryIndices.end(), compareBySize);//排序
        //vector<bitset<196916>> index(queryIndices.size());
        vector<vector<__m128i>> rebit(queryIndices.size());
        uint32_t id = queryIndices[0];
        for (uint32_t i = 0; i < arrays[id][arrays[id].size() - 1]; i += 128) {
            bitset<128> tmp;
            for (uint32_t j = 0; j < 128; j++) {
                tmp[j] = bitmaps[queryIndices[0]][j + i];
            }
            const uint32_t* bitmapPtr = reinterpret_cast<const uint32_t*>(&tmp);
            __m128i bitmap = _mm_load_si128((__m128i*)bitmapPtr);
            rebit[0].push_back(bitmap);
        }
        for (uint32_t i = 1; i < queryIndices.size(); i++) {
            for (uint32_t k = 0; k < arrays[id][arrays[id].size() - 1]; k += 128) {
                bitset<128> tmp;
                for (uint32_t j = 0; j < 128; j++) {
                    tmp[j] = bitmaps[queryIndices[i]][j + k];
                }
                const uint32_t* bitmapPtr = reinterpret_cast<const uint32_t*>(&tmp);
                __m128i bitmap = _mm_load_si128((__m128i*)bitmapPtr);
                rebit[i].push_back(bitmap);
            }
        }
        QueryPerformanceCounter(&start); // Record the start time of the query
        // Compute intersection of arrays based on sorted indices
        for (uint32_t i = 1; i < queryIndices.size(); i++) {
            for (uint32_t j = 0; j < rebit[i].size(); j++) {

                // Perform bitwise AND between result and the current bitmap
                rebit[0][j] = _mm_and_si128(rebit[0][j], rebit[i][j]);
            }
        }
        QueryPerformanceCounter(&end0); // Record the end time of the query

        double elapsedSeconds = static_cast<double>(end0.QuadPart - start.QuadPart) / freq.QuadPart;
        uint32_t cnt = 0;
        for (uint32_t i = 0; i < rebit[0].size(); i++) {
            bitset<128>tmp;
            _mm_store_si128((__m128i*) & tmp,rebit[0][i]);
            cnt += tmp.count();
        }
        //resultFile << res.count() << endl;
        cout << cnt;
        //resultFile<<queryCount<<endl;
        resultFile << elapsedSeconds << endl;

        queryCount++;
        res.reset();
        Flag.clear();
    }
    //resultFile << "总耗时：" << all_time << endl;
    queryFile.close();
    resultFile.close();

    return 0;
}

