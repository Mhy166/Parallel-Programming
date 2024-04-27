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
//AVX≤‚ ‘Œƒº˛
using namespace std;
const uint32_t index_num = 196916;//196916*128=25205248
vector<vector<uint32_t>> arrays;
bitset<25205248> res;
vector<bool> Flag;
vector<bitset<25205248>>bitmaps;

bool compareBySize(uint32_t& a, uint32_t& b) {
    return arrays[a][arrays[a].size() - 1] != arrays[b][arrays[b].size() - 1] ? arrays[a][arrays[a].size() - 1] < arrays[b][arrays[b].size() - 1] : a < b;
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
    ofstream resultFile("SIMD-AVX-256");
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
        while (ss >> row_idx) {
            queryIndices.push_back(row_idx);
            k++;
        }
        sort(queryIndices.begin(), queryIndices.end(), compareBySize);//≈≈–Ú
        vector<vector<__m256i>> rebit(queryIndices.size());
        uint32_t id = queryIndices[0];
        for (uint32_t i = 0; i < arrays[id][arrays[id].size() - 1]; i += 256) {
                 bitset<256> tmp;
            for (uint32_t j = 0; j < 256; j++) {
                tmp[j] = bitmaps[queryIndices[0]][j + i];
            }
            const uint32_t* bitmapPtr = reinterpret_cast<const uint32_t*>(&tmp);
            __m256i bitmap = _mm256_load_si256((reinterpret_cast<const __m256i*>(bitmapPtr)));
            rebit[0].push_back(bitmap);
        }

        for (uint32_t i = 1; i < queryIndices.size(); i++) {
            for (uint32_t k = 0; k < arrays[id][arrays[id].size() - 1]; k += 256) {
                bitset<256> tmp;
                for (uint32_t j = 0; j < 256; j++) {
                    tmp[j] = bitmaps[queryIndices[i]][j + k];
                }
                const uint32_t* bitmapPtr = reinterpret_cast<const uint32_t*>(&tmp);
                __m256i bitmap = _mm256_load_si256(reinterpret_cast<const __m256i*>(bitmapPtr));
                rebit[i].push_back(bitmap);
            }
        }
        QueryPerformanceCounter(&start);
        for (uint32_t i = 1; i < queryIndices.size(); i++) {
            for (uint32_t j = 0; j < rebit[i].size(); j++) {
                rebit[0][j] = _mm256_and_si256(rebit[0][j], rebit[i][j]);
            }
        }
        QueryPerformanceCounter(&end0);
        double elapsedSeconds = static_cast<double>(end0.QuadPart - start.QuadPart) / freq.QuadPart;
        uint32_t cnt = 0;
        for (uint32_t i = 0; i < rebit[0].size(); i++) {
            bitset<256>tmp;
            _mm256_store_si256(reinterpret_cast<__m256i*>(&tmp), rebit[0][i]);
            cnt += tmp.count();
        }
        resultFile << elapsedSeconds << endl;
        //cout<<cnt<<endl;
        queryCount++;
        res.reset();
        Flag.clear();
    }
    queryFile.close();
    resultFile.close();

    return 0;
}

