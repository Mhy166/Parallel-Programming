#include <bits/stdc++.h>
#include <Windows.h>
#include <mpi.h>
#include <omp.h>
#include <emmintrin.h>  // SSE2

using namespace std;

const uint32_t index_num = 196916;
vector<vector<uint32_t>> arrays;
bitset<25205248> res;
vector<bitset<25205248>> bitmaps;

bool compareBySize(uint32_t& a, uint32_t& b) {
    return arrays[a][arrays[a].size() - 1] != arrays[b][arrays[b].size() - 1] ? arrays[a][arrays[a].size() - 1] < arrays[b][arrays[b].size() - 1] : a < b;
}

void processQuery(int queryID, const vector<uint32_t>& queryIndices, int& resultSize, double& elapsedSeconds) {
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

    LARGE_INTEGER freq, start, end;
    QueryPerformanceFrequency(&freq);
    QueryPerformanceCounter(&start);

    for (int i = 1; i < static_cast<int>(queryIndices.size()); i++) {
#pragma omp parallel for num_threads(4)
        for (int j = 0; j < static_cast<int>(rebit[i].size()); j++) {
            rebit[0][j] = _mm_and_si128(rebit[0][j], rebit[i][j]);
        }
    }

    QueryPerformanceCounter(&end);
    elapsedSeconds = static_cast<double>(end.QuadPart - start.QuadPart) / freq.QuadPart;

    resultSize = 0;
    for (uint32_t i = 0; i < rebit[0].size(); i++) {
        bitset<128>tmp;
        _mm_store_si128((__m128i*) & tmp, rebit[0][i]);
        resultSize += tmp.count();
    }
  }

int main(int argc, char* argv[]) {
    MPI_Init(&argc, &argv);

    int rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    if (rank == 0) {
        // 主进程
        ifstream file("ExpIndex", ios::binary);
        if (!file) {
            cerr << "Failed to open the file." << endl;
            MPI_Abort(MPI_COMM_WORLD, 1);
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
            MPI_Abort(MPI_COMM_WORLD, 1);
        }

        string line;
        int queryCount = 0;
        while (getline(queryFile, line)) {
            queryCount++;
        }
        queryFile.clear();
        queryFile.seekg(0, ios::beg);

        for (int i = 1; i < size; i++) {
            MPI_Send(&queryCount, 1, MPI_INT, i, 0, MPI_COMM_WORLD);
        }

        LARGE_INTEGER freq1, startTotal, endTotal;
        QueryPerformanceFrequency(&freq1);
        QueryPerformanceCounter(&startTotal);
        int queryID = 0;
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

            int dest = queryID % (size - 1) + 1;
            int querySize = queryIndices.size();
            MPI_Send(&queryID, 1, MPI_INT, dest, 0, MPI_COMM_WORLD);
            MPI_Send(&querySize, 1, MPI_INT, dest, 0, MPI_COMM_WORLD);
            MPI_Send(queryIndices.data(), querySize, MPI_UINT32_T, dest, 0, MPI_COMM_WORLD);
            queryID++;
        }

        for (int i = 1; i < size; i++) {
            int endSignal = -1;
            MPI_Send(&endSignal, 1, MPI_INT, i, 0, MPI_COMM_WORLD);
        }

        queryFile.close();

        ofstream resultFile("MPI-SSE-2");
        if (!resultFile) {
            cerr << "Failed to create the result file." << endl;
            MPI_Abort(MPI_COMM_WORLD, 1);
        }

        vector<pair<int, double>> results(queryCount);
        vector<int> resultSizes(queryCount);
        for (int i = 0; i < queryCount; i++) {
            int id;
            double elapsedSeconds;
            int resultSize;
            MPI_Recv(&id, 1, MPI_INT, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            MPI_Recv(&elapsedSeconds, 1, MPI_DOUBLE, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            MPI_Recv(&resultSize, 1, MPI_INT, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            results[id] = { id, elapsedSeconds };
            resultSizes[id] = resultSize;
        }

        sort(results.begin(), results.end());

        for (const auto& result : results) {
            resultFile << result.second << endl;
        }
        QueryPerformanceCounter(&endTotal);
        double totalElapsedSeconds = static_cast<double>(endTotal.QuadPart - startTotal.QuadPart) / freq1.QuadPart;
        resultFile << totalElapsedSeconds << endl;

        resultFile.close();
    }
    else {
        // 子进程
        int queryCount;
        MPI_Recv(&queryCount, 1, MPI_INT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

        ifstream file("ExpIndex", ios::binary);
        if (!file) {
            cerr << "Failed to open the file." << endl;
            MPI_Abort(MPI_COMM_WORLD, 1);
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

        while (true) {
            int queryID;
            MPI_Recv(&queryID, 1, MPI_INT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            if (queryID == -1) {
                break;
            }

            int querySize;
            MPI_Recv(&querySize, 1, MPI_INT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            vector<uint32_t> queryIndices(querySize);
            MPI_Recv(queryIndices.data(), querySize, MPI_UINT32_T, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

            int resultSize;
            double elapsedSeconds;
            processQuery(queryID, queryIndices, resultSize, elapsedSeconds);

            MPI_Send(&queryID, 1, MPI_INT, 0, 0, MPI_COMM_WORLD);
            MPI_Send(&elapsedSeconds, 1, MPI_DOUBLE, 0, 0, MPI_COMM_WORLD);
            MPI_Send(&resultSize, 1, MPI_INT, 0, 0, MPI_COMM_WORLD);
            //cout << queryID << ": " << resultSize << endl;
        }
    }

    MPI_Finalize();
    return 0;
}
