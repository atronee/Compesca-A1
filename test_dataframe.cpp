#include <iostream>
#include <vector>
#include <any>
#include "src/DataFrame.h"
using namespace std;
int main() {
    DataFrame df;
    vector<int> v = {1, 2, 3, 4, 5};
    df.add_column("column1", v);
    auto v2 = df.get_column<int>("column1");
    for (int i : v2) {
        cout << i << endl;
    }

    df.add_column("column2", vector<int>{10, 20, 30, 40, 50});
    auto v3 = df.get_column<int>("column2");
    for (int i : v3) {
        cout << i << endl;
    }

    return 0;
}