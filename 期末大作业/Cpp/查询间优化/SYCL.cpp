%% writefile lab / simple.cpp
#include <sycl/sycl.hpp>
#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <set>
#include <list>
#include <algorithm>
#include <chrono>
#include <cstring>

using namespace sycl;
using namespace std;
using namespace chrono;

vector<vector<uint32_t>> arrays;
vector<string> queries;
vector<set<uint32_t>> results;

bool compareBySize(uint32_t& a, uint32_t& b) {
    return arrays[a].size() != arrays[b].size() ? arrays[a].size() < arrays[b].size() : a < b;
}
bool cmp(list<uint32_t>& a, list<uint32_t>& b) {
    return a.size() < b.size();
}

void process_query(const string& line, set<uint32_t>& res) {
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
}

int main() {
    ifstream file("ExpIndex", ios::binary);
    uint32_t arrayLength;
    while (file.read(reinterpret_cast<char*>(&arrayLength), sizeof(arrayLength))) {
        vector<uint32_t> array(arrayLength);
        file.read(reinterpret_cast<char*>(array.data()), arrayLength * sizeof(uint32_t));
        arrays.push_back(array);
    }
    file.close();
    ifstream queryFile("ExpQuery");
    string line;
    while (getline(queryFile, line)) {
        queries.push_back(line);
    }
    queryFile.close();
    ofstream resultFile("GPU");

    results.resize(queries.size());
    queue myQueue(default_selector_v);
    auto start1 = high_resolution_clock::now();

    // 处理逻辑
    vector<const char*> query_cstrs(queries.size());
    for (size_t i = 0; i < queries.size(); ++i) {
        query_cstrs[i] = queries[i].c_str();
    }

    buffer arrays_buffer(arrays.data(), range<1>(arrays.size()));
    buffer queries_buffer(query_cstrs.data(), range<1>(query_cstrs.size()));
    buffer results_buffer(results.data(), range<1>(results.size()));

    myQueue.submit([&](handler& h) {
        accessor arrays_accessor(arrays_buffer, h, read_only);
        accessor queries_accessor(queries_buffer, h, read_only);
        accessor results_accessor(results_buffer, h, write_only, no_init);

        h.parallel_for(range<1>(queries.size()), [=](id<1> i) {
            set<uint32_t> res;
            process_query(queries_accessor[i], res);
            results_accessor[i] = res;
            });
        }).wait();

        auto end1 = high_resolution_clock::now();
        double elapsedSeconds = duration<double>(end1 - start1).count();

        for (const auto& res : results) {
            resultFile << res.size() << std::endl;  // 输出结果大小
        }
        resultFile << elapsedSeconds << std::endl;
        resultFile.close();

        return 0;
}