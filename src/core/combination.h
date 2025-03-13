#ifndef METAEC_COMBINATION_H
#define METAEC_COMBINATION_H

#include <iostream>
#include <vector>

using namespace std;

// generate combinations
static void generate_combinations(const std::vector<int>& elements, int n, int start, std::vector<int>& current_combination, std::vector<std::vector<int>>& all_combinations) {
    if (current_combination.size() == n) {
        all_combinations.push_back(current_combination);
        return;
    }

    for (int i = start; i < elements.size(); ++i) {
        current_combination.push_back(elements[i]);
        generate_combinations(elements, n, i + 1, current_combination, all_combinations);
        current_combination.pop_back();
    }
}

// get combination main function
static std::vector<std::vector<int>> get_combinations(const std::vector<int>& elements, int n) {
    std::vector<std::vector<int>> all_combinations;
    std::vector<int> current_combination;
    generate_combinations(elements, n, 0, current_combination, all_combinations);
    return all_combinations;
}

// get index from multidimensional
static vector<uint8_t> get_multidimensional_index(const vector<uint8_t>& a, int t) {
    int m = a.size();
    vector<uint8_t> b(m, 0);
    for (int i = m - 1; i >= 0; --i) {
        b[i] = t % a[i];
        t /= a[i];
    }
    return b;
}

// get one dimensional index
static int get_one_dimensional_index(const vector<uint8_t>& a, const vector<uint8_t>& b) {
    int t = 0;
    int multiplier = 1;
    for (int i = a.size() - 1; i >= 0; --i) {
        t += b[i] * multiplier;
        multiplier *= a[i];
    }
    return t;
}

#endif