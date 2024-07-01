#include <iostream>
#include <vector>
#include <set>
#include <list>
#include <algorithm>
#include <fstream>
#include <sstream>
#include <Windows.h>
#include <cuda_runtime.h>
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

__global__ void processQueries(uint32_t** d_lists, uint32_t* d_sizes, uint32_t* d_results, uint32_t num_lists, uint32_t max_len) {
    int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx < num_lists) {
        uint32_t tmp = d_lists[0][idx];
        uint32_t cnt = 1;
        for (uint32_t i = 1; i < num_lists; i++) {
            for (uint32_t j = 0; j < d_sizes[i]; j++) {
                if (d_lists[i][j] == tmp) {
                    cnt++;
                    break;
                }
            }
        }
        if (cnt == num_lists) {
            d_results[idx] = tmp;
        }
    }
}

int main() {
    ifstream file("ExpIndex", ios::binary);
    if (!file) {
        cerr << "Failed to open the file." << endl;
        return 1;
    }

    uint32_t arrayLength;
    while (file.read(reinterpret_cast<char*>(&arrayLength), sizeof(arrayLength))) {
        vector<uint32_t> array(arrayLength);
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

    LARGE_INTEGER freq, start, end0, start1, end1;
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

        uint32_t max_len = 0;
        for (auto& l : lists) {
            if (l.size() > max_len) max_len = l.size();
        }

        uint32_t** h_lists = new uint32_t * [lists.size()];
        uint32_t* h_sizes = new uint32_t[lists.size()];
        uint32_t* h_results = new uint32_t[max_len];
        for (size_t i = 0; i < lists.size(); i++) {
            h_sizes[i] = lists[i].size();
            h_lists[i] = new uint32_t[lists[i].size()];
            copy(lists[i].begin(), lists[i].end(), h_lists[i]);
        }

        uint32_t** d_lists;
        uint32_t* d_sizes;
        uint32_t* d_results;
        cudaMalloc((void**)&d_lists, lists.size() * sizeof(uint32_t*));
        cudaMalloc((void**)&d_sizes, lists.size() * sizeof(uint32_t));
        cudaMalloc((void**)&d_results, max_len * sizeof(uint32_t));

        for (size_t i = 0; i < lists.size(); i++) {
            uint32_t* d_list;
            cudaMalloc((void**)&d_list, lists[i].size() * sizeof(uint32_t));
            cudaMemcpy(d_list, h_lists[i], lists[i].size() * sizeof(uint32_t), cudaMemcpyHostToDevice);
            cudaMemcpy(d_lists + i, &d_list, sizeof(uint32_t*), cudaMemcpyHostToDevice);
        }
        cudaMemcpy(d_sizes, h_sizes, lists.size() * sizeof(uint32_t), cudaMemcpyHostToDevice);

        QueryPerformanceCounter(&start);
        int blockSize = 256;
        int numBlocks = (max_len + blockSize - 1) / blockSize;
        processQueries << <numBlocks, blockSize >> > (d_lists, d_sizes, d_results, lists.size(), max_len);
        cudaDeviceSynchronize();
        QueryPerformanceCounter(&end0);

        cudaMemcpy(h_results, d_results, max_len * sizeof(uint32_t), cudaMemcpyDeviceToHost);

        double elapsedSeconds = static_cast<double>(end0.QuadPart - start.QuadPart) / freq.QuadPart;
        resultFile << elapsedSeconds << endl;

        for (uint32_t i = 0; i < max_len; i++) {
            if (h_results[i] != 0) {
                res.insert(h_results[i]);
            }
        }
        queryCount++;
        res.clear();
        Flag.clear();

        for (size_t i = 0; i < lists.size(); i++) {
            cudaFree(h_lists[i]);
        }
        delete[] h_lists;
        delete[] h_sizes;
        delete[] h_results;
        cudaFree(d_lists);
        cudaFree(d_sizes);
        cudaFree(d_results);
    }

    QueryPerformanceCounter(&end1);
    double elapsedSeconds = static_cast<double>(end1.QuadPart - start1.QuadPart) / freq.QuadPart;
    resultFile << elapsedSeconds << endl;

    queryFile.close();
    resultFile.close();
    return 0;
}
