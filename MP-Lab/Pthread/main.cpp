#include <iostream>
#include <bits/stdc++.h>
#include <fstream>
#include <sstream>
#include <pthread.h>
#include <Windows.h>
using namespace std;

vector<vector<uint32_t>> arrays;
vector<bool> Flag;
vector<double> restime;
vector<string> queries;

int thread_num = 1; // Number of threads to use
pthread_mutex_t res_mutex;



bool compareBySize(uint32_t& a, uint32_t& b) {
    return arrays[a].size() != arrays[b].size() ? arrays[a].size() < arrays[b].size() : a < b;
}

bool cmp(list<uint32_t>& a, list<uint32_t>& b) {
    return a.size() < b.size();
}

void* processQueries(void* arg) {
    int thread_id = *(int*)arg;
    LARGE_INTEGER freq, start, end0;
    QueryPerformanceFrequency(&freq);

    for (int i = thread_id; i < queries.size(); i += thread_num) {
        string line = queries[i];
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
        set<uint32_t> res;
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
        QueryPerformanceCounter(&end0);
        double elapsedSeconds = static_cast<double>(end0.QuadPart - start.QuadPart) / freq.QuadPart;
        pthread_mutex_lock(&res_mutex);
        //cout<<i<<" "<<res.size()<<" "<<endl;
        restime[i]= elapsedSeconds;
        pthread_mutex_unlock(&res_mutex);
    }
    return nullptr;
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

    string line;
    while (getline(queryFile, line)) {
        queries.push_back(line);
    }
    queryFile.close();
    for(int i=0;i<queries.size();i++){
        restime.push_back(0);
    }
    pthread_mutex_init(&res_mutex, nullptr);
    pthread_t threads[thread_num];
    int thread_ids[thread_num];


    LARGE_INTEGER freq1, start1, end1;
    QueryPerformanceFrequency(&freq1);
    QueryPerformanceCounter(&start1);
    for (int i = 0; i < thread_num; i++) {
        thread_ids[i] = i;
        pthread_create(&threads[i], nullptr, processQueries, (void*)&thread_ids[i]);
    }

    for (int i = 0; i < thread_num; i++) {
        pthread_join(threads[i], nullptr);
    }
    QueryPerformanceCounter(&end1);
    ofstream resultFile("Pthread-1-bw");
    for(int i=0;i<restime.size();i++){
        resultFile<<restime[i]<<endl;
    }
    double elapsedSeconds = static_cast<double>(end1.QuadPart - start1.QuadPart) / freq1.QuadPart;
    resultFile<<elapsedSeconds<<endl;
    resultFile.close();
    pthread_mutex_destroy(&res_mutex);
    return 0;
}
