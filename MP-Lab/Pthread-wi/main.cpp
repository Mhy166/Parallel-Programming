#include <iostream>
#include <bits/stdc++.h>
#include <fstream>
#include <sstream>
#include <pthread.h>
#include <Windows.h>
using namespace std;
LARGE_INTEGER freq, start, end0,start1,end1;
ofstream resultFile("Pthread-2-wi");
vector<vector<uint32_t>> arrays;
set<uint32_t> global_res;
pthread_mutex_t res_mutex;

struct SubTask {
    vector<list<uint32_t>> lists;
};

bool compareBySize(uint32_t& a, uint32_t& b) {
    return arrays[a].size() != arrays[b].size() ? arrays[a].size() < arrays[b].size() : a < b;
}

bool cmp(list<uint32_t>& a, list<uint32_t>& b) {
    return a.size() < b.size();
}

void* processSublist(void* arg) {
    SubTask* task = (SubTask*)arg;
    set<uint32_t> local_res;

    while (!task->lists[0].empty()) {
        uint32_t tmp = task->lists[0].front();
        uint32_t cnt = 1;
        bool flag = false;
        task->lists[0].pop_front();
        for (uint32_t i = 1; i < task->lists.size(); i++) {
            while (true) {
                if (task->lists[i].empty()) {
                    flag = true;
                    break;
                }
                if (task->lists[i].front() < tmp) {
                    task->lists[i].pop_front();
                } else if (task->lists[i].front() == tmp) {
                    task->lists[i].pop_front();
                    cnt++;
                } else {
                    break;
                }
            }
            if (flag) {
                break;
            }
        }
        if (cnt == task->lists.size()) {
            local_res.insert(tmp);
        }
        sort(task->lists.begin(), task->lists.end(), cmp);
    }

    pthread_mutex_lock(&res_mutex);
    global_res.insert(local_res.begin(), local_res.end());
    pthread_mutex_unlock(&res_mutex);
    return nullptr;
}

void processQuery(const string& line, int thread_num) {
    stringstream ss(line);
    vector<uint32_t> queryIndices;
    uint32_t row_idx;
    while (ss >> row_idx) {
        queryIndices.push_back(row_idx);
    }
    sort(queryIndices.begin(), queryIndices.end(), compareBySize);

    vector<list<uint32_t>> lists(queryIndices.size());
    for (uint32_t i = 0; i < queryIndices.size(); i++) {
        for (uint32_t j = 0; j < arrays[queryIndices[i]].size(); j++) {
            lists[i].push_back(arrays[queryIndices[i]][j]);
        }
    }
    list<uint32_t>& first_list = lists[0];
    vector<list<uint32_t>> sublists(thread_num);
    auto it = first_list.begin();
    size_t sublist_size = first_list.size() / thread_num;
    for (int i = 0; i < thread_num; i++) {
        auto sublist_start = next(it, i * sublist_size);
        auto sublist_end = (i == thread_num - 1) ? first_list.end() : next(it, (i + 1) * sublist_size);
        sublists[i] = list<uint32_t>(sublist_start, sublist_end);
    }

    vector<SubTask> tasks(thread_num);
    for (int i = 0; i < thread_num; i++) {
        tasks[i].lists = lists;
        tasks[i].lists[0] = sublists[i]; // Use only the sublist for the first list
    }
    QueryPerformanceCounter(&start);
    vector<pthread_t> threads(thread_num);
    for (int i = 0; i < thread_num; i++) {
        pthread_create(&threads[i], nullptr, processSublist, (void*)&tasks[i]);
    }

    for (int i = 0; i < thread_num; i++) {
        pthread_join(threads[i], nullptr);
    }
    QueryPerformanceCounter(&end0);
    double elapsedSeconds = static_cast<double>(end0.QuadPart - start.QuadPart) / freq.QuadPart;
    resultFile << elapsedSeconds << endl;
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



    pthread_mutex_init(&res_mutex, nullptr);
    QueryPerformanceFrequency(&freq);
    QueryPerformanceCounter(&start1);

    string line;
    while (getline(queryFile, line)) {
        global_res.clear();
        processQuery(line, 2); // Adjust the number of threads here
       // cout << global_res.size() << endl;
    }
    QueryPerformanceCounter(&end1);
    double elapsedSeconds = static_cast<double>(end1.QuadPart - start1.QuadPart) / freq.QuadPart;
    resultFile << elapsedSeconds << endl;
    queryFile.close();
    resultFile.close();
    pthread_mutex_destroy(&res_mutex);

    return 0;
}
