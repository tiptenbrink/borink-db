#include <iostream>

#include <pqxx/pqxx>

int main() {
    std::cout << "borink-db (libpqxx " << PQXX_VERSION << ")\n";
    return 0;
}
