#include <iostream>
#include <vector>
#include <any>
#include "src/DataFrame.h"
using namespace std;
int main() {
    DataFrame df;
    vector<int> v = {1, 2, 3, 4, 5};
    df.add_column("column1", v);
    df.add_row({6});
    auto v2 = df.get_column<int>("column1");
    for (int i : v2) {
        cout << i << endl;
    }

    df.add_column("column2", vector<int>{10, 20, 30, 40, 50, 60});
    auto v3 = df.get_column<int>("column2");
    for (int i : v3) {
        cout << i << endl;
    }
    df.add_column("column3", vector<string>{"a", "b", "c", "d", "e", "f"});
    auto v4 = df.get_column<string>("column3");
    for (const string& s : v4) {
        cout << s << endl;
    }
    df.remove_column("column1");
    try{
        df.get_column<int>("column1");
    } catch (const invalid_argument& e) {
        cout << e.what() << endl;
    }
    df.remove_row(2);
    auto v5 = df.get_column<int>("column2");
    for (int i : v5) {
        cout << i << endl;
    }

    return 0;
}