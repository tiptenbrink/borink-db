#include <iostream>

#include <pqxx/pqxx>

int main() {
    auto a = 3;

    auto c = "my_string";

    auto d = 4;

    std::cout << "borink-db (libpqxx " << PQXX_VERSION << ")\n";
    return 0;
}
