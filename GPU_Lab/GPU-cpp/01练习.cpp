%% writefile lab / simple.cpp
//==============================================================
// Copyright © Intel Corporation
//
// SPDX-License-Identifier: MIT
// =============================================================
#include <sycl/sycl.hpp>  // 包含SYCL的头文件
using namespace sycl;  // 使用sycl命名空间
static const int N = 20;  // 定义一个常量N，表示数组的大小
int main() {
    queue q;//# 定义一个队列，其关联了一个默认设备用于计算卸载
    std::cout << "Device: " << q.get_device().get_info<info::device::name>() << "\n";// \
          输出使用的设备名称
          //通过上面这两行，我们可以看到当前使用的设备名称，主机协调和控制设备上执行的计算任务。\
          这是平台模型的体现！
          //# 统一共享内存分配，允许在主机和设备上访问数据，这是内存模型的体现！
    int* data = malloc_shared<int>(N, q);
    //# 初始化数组
    for (int i = 0; i < N; i++) data[i] = i;
    //# 将并行计算卸载到设备上
    q.parallel_for(range<1>(N), [=](id<1> i) {
        data[i] *= 2;  // 将每个元素乘以2
        }).wait();  // 等待计算完成
        //这里的内核代码是一个lambda表达式，定义了如何处理数据，每个元素都乘以2。\
        wait()函数确保主机等待设备完成计算。这是执行模型和内核编程模型的体现！
        for (int i = 0; i < N; i++) std::cout << data[i] << "\n"; // 打印数组的每个元素
        free(data, q); // 释放分配的内存
}