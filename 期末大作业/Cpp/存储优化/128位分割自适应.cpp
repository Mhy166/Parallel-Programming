#include <iostream>
#include <fstream>
#include <vector>
#include <set>
#include <sstream>
#include <string>
#include <bitset>
#include <Windows.h>
#include <bits/stdc++.h>
#include <cuda_runtime.h>

using namespace std;
const uint32_t index_num = 196916; // 196916*128=25205248
vector<vector<uint32_t>> arrays;
bitset<25205248> res;
vector<bool> Flag;
vector<bitset<25205248>> bitmaps;

bool compareBySize(uint32_t& a, uint32_t& b)
{
    return arrays[a][arrays[a].size() - 1] != arrays[b][arrays[b].size() - 1] ? arrays[a][arrays[a].size() - 1] < arrays[b][arrays[b].size() - 1] : a < b;
}

__global__ void computeIntersection(bitset<128>* rebit, int num_queries, int num_bits)
{
    int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx < num_bits)
    {
        for (int i = 1; i < num_queries; i++)
        {
            rebit[idx] &= rebit[idx + i * num_bits];
        }
    }
}

int main()
{
    ifstream file("ExpIndex", ios::binary);
    if (!file)
    {
        cerr << "Failed to open the file." << endl;
        return 1;
    }
    uint32_t arrayLength;
    while (file.read(reinterpret_cast<char*>(&arrayLength), sizeof(arrayLength)))
    {
        std::vector<uint32_t> array(arrayLength);
        file.read(reinterpret_cast<char*>(array.data()), arrayLength * sizeof(uint32_t));
        arrays.push_back(array);
    }
    file.close();
    for (uint32_t i = 0; i < arrays.size(); i++)
    {
        bitmaps.push_back(res);
        for (uint32_t j = 0; j < arrays[i].size(); j++)
        {
            bitmaps[i][arrays[i][j]] = 1;
        }
    }

    ifstream queryFile("ExpQuery");
    if (!queryFile)
    {
        cerr << "Failed to open the query file." << endl;
        return 1;
    }
    ofstream resultFile("SIMD-128");
    if (!resultFile)
    {
        cerr << "Failed to create the result file." << endl;
        return 1;
    }
    LARGE_INTEGER freq, start, end0;
    QueryPerformanceFrequency(&freq);
    string line;
    int queryCount = 1;
    while (getline(queryFile, line))
    {
        uint32_t k = 0;
        stringstream ss(line);
        vector<uint32_t> queryIndices;
        uint32_t row_idx;
        bool flag = true;

        // Read and sort the indices for this query
        while (ss >> row_idx)
        {
            queryIndices.push_back(row_idx);
            k++;
        }
        sort(queryIndices.begin(), queryIndices.end(), compareBySize); // ≈≈–Ú
        vector<vector<bitset<128>>> rebit(queryIndices.size());
        uint32_t id = queryIndices[0];
        for (uint32_t i = 0; i < arrays[id][arrays[id].size() - 1]; i += 128)
        {
            bitset<128> tmp;
            for (uint32_t j = 0; j < 128; j++)
            {
                tmp[j] = bitmaps[queryIndices[0]][j + i];
            }
            rebit[0].push_back(tmp);
        }
        for (uint32_t i = 1; i < queryIndices.size(); i++)
        {
            for (uint32_t k = 0; k < arrays[id][arrays[id].size() - 1]; k += 128)
            {
                bitset<128> tmp;
                for (uint32_t j = 0; j < 128; j++)
                {
                    tmp[j] = bitmaps[queryIndices[i]][j + k];
                }
                rebit[i].push_back(tmp);
            }
        }

        // Allocate memory on the GPU
        bitset<128>* d_rebit;
        size_t num_bits = rebit[0].size();
        cudaMalloc(&d_rebit, num_bits * queryIndices.size() * sizeof(bitset<128>));
        cudaMemcpy(d_rebit, rebit[0].data(), num_bits * queryIndices.size() * sizeof(bitset<128>), cudaMemcpyHostToDevice);

        // GPU timing
        cudaEvent_t startEvent, stopEvent;
        cudaEventCreate(&startEvent);
        cudaEventCreate(&stopEvent);

        cudaEventRecord(startEvent, 0); // Record the start time on GPU

        // Compute intersection of arrays based on sorted indices
        int blockSize = 128; // Number of threads per block
        int numBlocks = (num_bits + blockSize - 1) / blockSize;
        computeIntersection << <numBlocks, blockSize >> > (d_rebit, queryIndices.size(), num_bits);
        cudaEventRecord(stopEvent, 0); // Record the end time on GPU

        cudaEventSynchronize(stopEvent);
        float milliseconds = 0;
        cudaEventElapsedTime(&milliseconds, startEvent, stopEvent); // Compute the elapsed time

        // Copy the result back to the host
        cudaMemcpy(rebit[0].data(), d_rebit, num_bits * sizeof(bitset<128>), cudaMemcpyDeviceToHost);

        uint32_t cnt = 0;
        for (uint32_t i = 0; i < rebit[0].size(); i++)
        {
            cnt += rebit[0][i].count();
        }
        resultFile << milliseconds / 1000.0 << endl; // Write the elapsed time in seconds

        queryCount++;
        res.reset();
        Flag.clear();

        // Free memory on the GPU
        cudaFree(d_rebit);
        cudaEventDestroy(startEvent);
        cudaEventDestroy(stopEvent);
    }
    queryFile.close();
    resultFile.close();

    return 0;
}
