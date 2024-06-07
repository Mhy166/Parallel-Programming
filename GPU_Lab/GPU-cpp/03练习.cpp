%% writefile lab / usm_lab.cpp
//==============================================================
// Copyright © Intel Corporation
//
// SPDX-License-Identifier: MIT
// =============================================================
#include <sycl/sycl.hpp>
using namespace sycl;

static const int N = 1024;

int main() {
    queue q;
    std::cout << "Device : " << q.get_device().get_info<info::device::name>() << "\n";

    // 初始化主机上的两个数组
    int* data1 = static_cast<int*>(malloc(N * sizeof(int)));
    int* data2 = static_cast<int*>(malloc(N * sizeof(int)));
    for (int i = 0; i < N; i++) {
        data1[i] = 25;
        data2[i] = 49;
    }

    // STEP 1: 创建 USM 设备分配
    int* device_data1 = malloc_device<int>(N, q);
    int* device_data2 = malloc_device<int>(N, q);

    // STEP 2: 将 data1 和 data2 复制到 USM 设备分配中
    q.memcpy(device_data1, data1, N * sizeof(int)).wait();
    q.memcpy(device_data2, data2, N * sizeof(int)).wait();

    // STEP 3: 编写内核代码，将 device_data1 的每个元素更新为其平方根
    q.parallel_for(N, [=](auto i) {
        device_data1[i] = std::sqrt(device_data1[i]);
        });

    // STEP 4: 编写内核代码，将 device_data2 的每个元素更新为其平方根
    q.parallel_for(N, [=](auto i) {
        device_data2[i] = std::sqrt(device_data2[i]);
        });

    // STEP 5: 编写内核代码，将 device_data2 的每个元素加到 device_data1 中
    q.parallel_for(N, [=](auto i) {
        device_data1[i] += device_data2[i];
        }).wait();

        // STEP 6: 将 device_data1 复制回主机上的 data1
        q.memcpy(data1, device_data1, N * sizeof(int)).wait();

        // 验证结果
        int fail = 0;
        for (int i = 0; i < N; i++) if (data1[i] != 12) { fail = 1; break; }
        if (fail == 1) std::cout << " FAIL"; else std::cout << " PASS";
        std::cout << "\n";

        // STEP 7: 释放 USM 设备分配
        free(device_data1, q);
        free(device_data2, q);
        free(data1);
        free(data2);

        return 0;
}
