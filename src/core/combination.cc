#include "combination.h"

int main() {
    std::vector<int> elements = {1, 2, 3, 4, 5};
    int n = 2;
    std::vector<std::vector<int>> combinations = get_combinations(elements, n);
    for (const auto& combination : combinations) {
        for (int num : combination) {
            std::cout << num << " ";
        }
        std::cout << std::endl;
    }
    return 0;
}
