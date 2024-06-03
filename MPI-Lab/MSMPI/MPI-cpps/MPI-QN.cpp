#include <iostream>
#include <bits/stdc++.h>
#include <fstream>
#include <sstream>
#include <Windows.h>
#include <mpi.h>

using namespace std;

vector<vector<uint32_t>> arrays;
set<uint32_t> res;

bool compareBySize(uint32_t& a, uint32_t& b) {
    return arrays[a].size() != arrays[b].size() ? arrays[a].size() < arrays[b].size() : a < b;
}

bool cmp(list<uint32_t>& a, list<uint32_t>& b) {
    return a.size() < b.size();
}

void processQueryPart(int startIdx, int endIdx, const vector<uint32_t>& queryIndices, vector<list<uint32_t>>& lists, set<uint32_t>& localRes) {
    while (startIdx < endIdx && !lists[0].empty()) {
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
            localRes.insert(tmp);
        }
        sort(lists.begin(), lists.end(), cmp);
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

        ifstream queryFile("ExpQuery");
        if (!queryFile) {
            cerr << "Failed to open the query file." << endl;
            MPI_Abort(MPI_COMM_WORLD, 1);
        }

        ofstream resultFile("MPI-QN-9");
        if (!resultFile) {
            cerr << "Failed to create the result file." << endl;
            MPI_Abort(MPI_COMM_WORLD, 1);
        }

        LARGE_INTEGER freq, startTotal, endTotal;
        QueryPerformanceFrequency(&freq);
        QueryPerformanceCounter(&startTotal);

        string line;
        int queryCount = 1;
        while (getline(queryFile, line)) {
            LARGE_INTEGER startQuery, endQuery;
            QueryPerformanceCounter(&startQuery);

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

            int segmentSize = lists[0].size() / (size - 1);
            int startIdx = 0;
            for (int i = 1; i < size; i++) {
                int endIdx = (i == size - 1) ? lists[0].size() : startIdx + segmentSize;
                MPI_Send(&startIdx, 1, MPI_INT, i, 0, MPI_COMM_WORLD);
                MPI_Send(&endIdx, 1, MPI_INT, i, 0, MPI_COMM_WORLD);
                int querySize = queryIndices.size();
                MPI_Send(&querySize, 1, MPI_INT, i, 0, MPI_COMM_WORLD);
                MPI_Send(queryIndices.data(), querySize, MPI_UINT32_T, i, 0, MPI_COMM_WORLD);

                // 发送当前列表数据
                for (uint32_t j = 0; j < lists.size(); j++) {
                    int listSize = lists[j].size();
                    MPI_Send(&listSize, 1, MPI_INT, i, 0, MPI_COMM_WORLD);
                    vector<uint32_t> listData(lists[j].begin(), lists[j].end());
                    MPI_Send(listData.data(), listSize, MPI_UINT32_T, i, 0, MPI_COMM_WORLD);
                }

                startIdx = endIdx;
            }

            set<uint32_t> resultSet;
            for (int i = 1; i < size; i++) {
                int resultSize;
                MPI_Recv(&resultSize, 1, MPI_INT, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                vector<uint32_t> localRes(resultSize);
                MPI_Recv(localRes.data(), resultSize, MPI_UINT32_T, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                resultSet.insert(localRes.begin(), localRes.end());
            }

            QueryPerformanceCounter(&endQuery);
            double elapsedSeconds = static_cast<double>(endQuery.QuadPart - startQuery.QuadPart) / freq.QuadPart;
            resultFile << elapsedSeconds << endl;
            //cout << resultSet.size() << endl;
            queryCount++;
        }

        QueryPerformanceCounter(&endTotal);
        double totalElapsedSeconds = static_cast<double>(endTotal.QuadPart - startTotal.QuadPart) / freq.QuadPart;
        resultFile  << totalElapsedSeconds  << endl;
        
        queryFile.close();
        resultFile.close();
    }
    else {
        // 子进程
        while (true) {
            int startIdx, endIdx, querySize;
            MPI_Recv(&startIdx, 1, MPI_INT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            MPI_Recv(&endIdx, 1, MPI_INT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            MPI_Recv(&querySize, 1, MPI_INT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            vector<uint32_t> queryIndices(querySize);
            MPI_Recv(queryIndices.data(), querySize, MPI_UINT32_T, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

            vector<list<uint32_t>> lists(querySize);
            for (uint32_t i = 0; i < querySize; i++) {
                int listSize;
                MPI_Recv(&listSize, 1, MPI_INT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                vector<uint32_t> listData(listSize);
                MPI_Recv(listData.data(), listSize, MPI_UINT32_T, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                lists[i] = list<uint32_t>(listData.begin(), listData.end());
            }

            set<uint32_t> localRes;
            processQueryPart(startIdx, endIdx, queryIndices, lists, localRes);

            int resultSize = localRes.size();
            vector<uint32_t> localResVec(localRes.begin(), localRes.end());
            MPI_Send(&resultSize, 1, MPI_INT, 0, 0, MPI_COMM_WORLD);
            MPI_Send(localResVec.data(), resultSize, MPI_UINT32_T, 0, 0, MPI_COMM_WORLD);
        }
    }

    MPI_Finalize();
    return 0;
}
