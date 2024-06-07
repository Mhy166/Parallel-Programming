%% writefile lab / reduction_lab.cpp
//==============================================================
// Copyright © Intel Corporation
//
// SPDX-License-Identifier: MIT
// =============================================================
#include <sycl/sycl.hpp>

using namespace sycl;

static constexpr size_t N = 1024; // global size
static constexpr size_t B = 128; // work-group size

int main() {
    //# setup queue with default selector
    queue q;
    std::cout << "Device : " << q.get_device().get_info<info::device::name>() << "\n";

    //# initialize data array using usm
    auto data = malloc_shared<int>(N, q);
    for (int i = 0; i < N; i++) data[i] = i;

    //# implicit USM for writing min and max value
    int* min = malloc_shared<int>(1, q);
    int* max = malloc_shared<int>(1, q);
    *min = 0;
    *max = 0;

    //# STEP 1 : Create reduction objects for computing min and max

    //# YOUR CODE GOES HERE
    auto min_reduction = reduction(min, sycl::ext::oneapi::minimum<>());
    auto max_reduction = reduction(max, sycl::ext::oneapi::maximum<>());



    //# Reduction Kernel get min and max
    q.submit([&](handler& h) {
        // 第二步：添加带有最小值和最大值归约对象的parallel_for
        h.parallel_for(nd_range<1>{N, B}, min_reduction, max_reduction,
        [=](nd_item<1> item, auto& min, auto& max) {
                int idx = item.get_global_id(0);
                min.combine(data[idx]);
                max.combine(data[idx]);
            });
        }).wait();

        //# STEP 3 : Compute mid_range from min and max
        int mid_range = (*min + *max) / 2;
        std::cout << "Mid-Range = " << mid_range << "\n";
        free(data, q);
        free(min, q);
        free(max, q);
        return 0;
}
