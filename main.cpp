#include <iostream>
#include <vector>
#include "src/DataFrame.h"
using namespace std;
int main() {
    DataFrame<int> df;
    vector<int> v = {1, 2, 3, 4, 5};
    df.add_column("column1", v);
    vector<int> v2 = df.get_column("column1");
    for (int i : v2) {
        cout << i << endl;
    }
    df.add_column("column2", {6, 7, 8, 9, 10});
    vector<int> v3 = df.get_column("column2");
    for (int i : v3) {
        cout << i << endl;
    }

    return 0;
}