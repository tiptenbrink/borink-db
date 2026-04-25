#include <iostream>
#include <string_view>

#include <llfio/llfio.hpp>
#include <pqxx/pqxx>

namespace {
bool smoke_test_llfio(std::string_view executable_path) {
    namespace llfio = LLFIO_V2_NAMESPACE;

    auto file = llfio::file_handle::file(
        {},
        llfio::path_view(executable_path.data(), executable_path.size(), llfio::path_view::not_zero_terminated),
        llfio::file_handle::mode::read,
        llfio::file_handle::creation::open_existing
    );
    if (!file) {
        std::cerr << "llfio smoke-test open failed: " << file.error().message() << '\n';
        return false;
    }

    llfio::byte buffer[4] = {};
    llfio::file_handle::buffer_type read_buffer{buffer, sizeof(buffer)};
    auto read_result = file.value().read({llfio::file_handle::buffers_type{&read_buffer, 1}, 0});
    if (!read_result) {
        std::cerr << "llfio smoke-test read failed: " << read_result.error().message() << '\n';
        return false;
    }

    const bool has_elf_header = buffer[0] == static_cast<llfio::byte>(0x7f)
        && buffer[1] == static_cast<llfio::byte>('E')
        && buffer[2] == static_cast<llfio::byte>('L')
        && buffer[3] == static_cast<llfio::byte>('F');
    if (!has_elf_header) {
        std::cerr << "llfio smoke-test failed: executable does not start with ELF magic\n";
        return false;
    }

    std::cout << "llfio smoke-test OK: executable starts with ELF magic\n";
    return true;
}
}

int main(int argc, char **argv) {
    std::cout << "borink-db (libpqxx " << PQXX_VERSION << ")\n";

    if (argc < 1 || argv[0] == nullptr || !smoke_test_llfio(argv[0])) {
        return 1;
    }
    return 0;
}
