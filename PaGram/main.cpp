#include <iostream>
#include <fstream>
#include <vector>
#include <set>
#include <sstream>
#include <string>
#include <Windows.h>
//baseline-1测试文件
using namespace std;

vector<vector<uint32_t>> arrays;
vector<uint32_t> res;
vector<bool> Flag;

bool compareBySize(uint32_t& a,uint32_t& b) {
    return arrays[a].size() != arrays[b].size()?arrays[a].size() < arrays[b].size():a<b;
}
int main() {
    ifstream file("ExpIndex", ios::binary);
    if (!file) {
        cerr << "Failed to open the file." <<endl;
        return 1;
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
        return 1;
    }

    ofstream resultFile("Baseline");
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
        uint32_t col_nums = 0;
        uint32_t l1_size;
        bool flag = true;

        // Read and sort the indices for this query
        while (ss >> row_idx) {
            col_nums += arrays[row_idx].size();
            queryIndices.push_back(row_idx);
            k++;
        }
        sort(queryIndices.begin(), queryIndices.end(),compareBySize);
        l1_size=arrays[queryIndices[0]].size();


        QueryPerformanceCounter(&start); // Record the start time of the query
        // Compute intersection of arrays based on sorted indices
        for (auto idx : queryIndices) {
            if (flag) {
                for (auto elem : arrays[idx]) {
                    res.push_back(elem);
                    Flag.push_back(true);
                }
                flag = false;
            } else {
                vector<bool> found(res.size(), false);
                for (auto elem : arrays[idx]) {
                    for (size_t i = 0; i < res.size(); i++) {
                        if (res[i] == elem) {
                            found[i] = true;
                            break;
                        }
                    }
                }
                for (size_t i = 0; i < res.size(); i++) {
                    Flag[i] = Flag[i] && found[i];
                }
            }
        }
        QueryPerformanceCounter(&end0); // Record the end time of the query

        resultFile << endl;
        double elapsedSeconds = static_cast<double>(end0.QuadPart - start.QuadPart) / freq.QuadPart;
        //resultFile << "第" << queryCount << "条查询结果：" << endl;
        /*
        uint32_t cnt = 0;
        for (uint32_t iter = 0; iter < res.size(); iter++) {
            if (Flag[iter]){
                    resultFile<<res[iter]<<" ";
                    cnt++;
            }
        }
        resultFile <<endl<< "结果列表长度：" << cnt << endl;
*/

        resultFile << "第" << queryCount << "条查询耗时：" << elapsedSeconds << "秒" << endl;
        uint32_t cnt = 0;
        for (uint32_t iter = 0; iter < res.size(); iter++) {
            if (Flag[iter])
                cnt++;
        }
        resultFile << "输入列表个数：" << k << endl;
        resultFile << "L1列表长度" <<l1_size<< endl;
        resultFile << "输入列表平均长度" << col_nums / k << endl;
        resultFile << "结果列表长度：" << cnt << endl;

        queryCount++;
        res.clear();
        Flag.clear();
    }

    queryFile.close();
    resultFile.close();

    return 0;
}
