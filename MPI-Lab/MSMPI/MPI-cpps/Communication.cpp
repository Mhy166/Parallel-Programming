#include <bits/stdc++.h>
#include <Windows.h>
#include <mpi.h>

using namespace std;

vector<vector<uint32_t>> arrays;

bool compareBySize(uint32_t& a, uint32_t& b) {
    return arrays[a].size() != arrays[b].size() ? arrays[a].size() < arrays[b].size() : a < b;
}

bool cmp(list<uint32_t>& a, list<uint32_t>& b) {
    return a.size() < b.size();
}

void processQuery(int queryID, const vector<uint32_t>& queryIndices, set<uint32_t>& res, double& elapsedSeconds) {
    vector<list<uint32_t>> lists(queryIndices.size());
    for (uint32_t i = 0; i < queryIndices.size(); i++) {
        for (uint32_t j = 0; j < arrays[queryIndices[i]].size(); j++) {
            lists[i].push_back(arrays[queryIndices[i]][j]);
        }
    }

    LARGE_INTEGER freq, start, end;
    QueryPerformanceFrequency(&freq);
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

    QueryPerformanceCounter(&end);
    elapsedSeconds = static_cast<double>(end.QuadPart - start.QuadPart) / freq.QuadPart;
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

            MPI_Request sendRequests[3];
            MPI_Isend(&queryID, 1, MPI_INT, dest, 0, MPI_COMM_WORLD, &sendRequests[0]);
            MPI_Isend(&querySize, 1, MPI_INT, dest, 0, MPI_COMM_WORLD, &sendRequests[1]);
            MPI_Isend(queryIndices.data(), querySize, MPI_UINT32_T, dest, 0, MPI_COMM_WORLD, &sendRequests[2]);

            MPI_Waitall(3, sendRequests, MPI_STATUSES_IGNORE);
            queryID++;
        }

        for (int i = 1; i < size; i++) {
            int endSignal = -1;
            MPI_Request endRequest;
            MPI_Isend(&endSignal, 1, MPI_INT, i, 0, MPI_COMM_WORLD, &endRequest);
            MPI_Wait(&endRequest, MPI_STATUS_IGNORE);
        }

        queryFile.close();

        ofstream resultFile("MPI-Com-16");
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

            MPI_Request recvRequests[3];
            MPI_Irecv(&id, 1, MPI_INT, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &recvRequests[0]);
            MPI_Irecv(&elapsedSeconds, 1, MPI_DOUBLE, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &recvRequests[1]);
            MPI_Irecv(&resultSize, 1, MPI_INT, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &recvRequests[2]);

            MPI_Waitall(3, recvRequests, MPI_STATUSES_IGNORE);
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

        /*
        for (const auto& resultSize : resultSizes) {
            resultFile << resultSize << endl;
        }
        */
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

        while (true) {
            int queryID;
            MPI_Request queryIDRequest;
            MPI_Irecv(&queryID, 1, MPI_INT, 0, 0, MPI_COMM_WORLD, &queryIDRequest);
            MPI_Wait(&queryIDRequest, MPI_STATUS_IGNORE);

            if (queryID == -1) {
                break;
            }

            int querySize;
            MPI_Request querySizeRequest;
            MPI_Irecv(&querySize, 1, MPI_INT, 0, 0, MPI_COMM_WORLD, &querySizeRequest);
            MPI_Wait(&querySizeRequest, MPI_STATUS_IGNORE);

            vector<uint32_t> queryIndices(querySize);
            MPI_Request queryIndicesRequest;
            MPI_Irecv(queryIndices.data(), querySize, MPI_UINT32_T, 0, 0, MPI_COMM_WORLD, &queryIndicesRequest);
            MPI_Wait(&queryIndicesRequest, MPI_STATUS_IGNORE);

            set<uint32_t> res;
            double elapsedSeconds;
            processQuery(queryID, queryIndices, res, elapsedSeconds);

            int resultSize = res.size();

            MPI_Request sendRequests[3];
            MPI_Isend(&queryID, 1, MPI_INT, 0, 0, MPI_COMM_WORLD, &sendRequests[0]);
            MPI_Isend(&elapsedSeconds, 1, MPI_DOUBLE, 0, 0, MPI_COMM_WORLD, &sendRequests[1]);
            MPI_Isend(&resultSize, 1, MPI_INT, 0, 0, MPI_COMM_WORLD, &sendRequests[2]);

            MPI_Waitall(3, sendRequests, MPI_STATUSES_IGNORE);
        }
    }

    MPI_Finalize();
    return 0;
}
