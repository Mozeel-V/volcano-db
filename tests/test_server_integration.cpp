#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

#include <chrono>
#include <array>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <sstream>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#ifdef _WIN32
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <winsock2.h>
#include <ws2tcpip.h>
#include <windows.h>
#else
#include <arpa/inet.h>
#include <netinet/in.h>
#include <signal.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#endif

namespace {

#ifdef _WIN32
using SocketHandle = SOCKET;
static constexpr SocketHandle kInvalidSocket = INVALID_SOCKET;
#else
using SocketHandle = int;
static constexpr SocketHandle kInvalidSocket = -1;
#endif

#ifdef _WIN32
class WinSockScope {
public:
    WinSockScope() {
        WSADATA data;
        ok_ = (WSAStartup(MAKEWORD(2, 2), &data) == 0);
    }

    ~WinSockScope() {
        if (ok_) {
            WSACleanup();
        }
    }

    bool ok() const { return ok_; }

private:
    bool ok_ = false;
};
#endif

static void close_socket_handle(SocketHandle sock) {
#ifdef _WIN32
    closesocket(sock);
#else
    close(sock);
#endif
}

static void cleanup_durability_files() {
    std::remove("sqp.wal");
    std::remove("sqp.checkpoint");
}

class DurabilityFileScope {
public:
    DurabilityFileScope() { cleanup_durability_files(); }
    ~DurabilityFileScope() { cleanup_durability_files(); }
};

class ServerProcess {
public:
    explicit ServerProcess(uint16_t port, bool password_auth = false) {
#ifdef _WIN32
        std::string cmd = ".\\vdb.exe --server --host 127.0.0.1 --port " + std::to_string(port);
        if (password_auth) {
            cmd += " --auth-mode password";
        }

        STARTUPINFOA si{};
        PROCESS_INFORMATION pi{};
        si.cb = sizeof(si);

        std::vector<char> mutable_cmd(cmd.begin(), cmd.end());
        mutable_cmd.push_back('\0');

        if (CreateProcessA(
                nullptr,
                mutable_cmd.data(),
                nullptr,
                nullptr,
                FALSE,
                CREATE_NO_WINDOW,
                nullptr,
                nullptr,
                &si,
                &pi)) {
            proc_ = pi.hProcess;
            thread_ = pi.hThread;
            running_ = true;
        }
#else
        pid_ = fork();
        if (pid_ == 0) {
            std::string p = std::to_string(port);
            if (password_auth) {
                execl("./vdb", "vdb", "--server", "--host", "127.0.0.1", "--port", p.c_str(), "--auth-mode", "password", (char*)nullptr);
            } else {
                execl("./vdb", "vdb", "--server", "--host", "127.0.0.1", "--port", p.c_str(), (char*)nullptr);
            }
            _exit(127);
        }
        running_ = (pid_ > 0);
#endif

        if (running_) {
            std::this_thread::sleep_for(std::chrono::milliseconds(300));
        }
    }

    ~ServerProcess() { stop(); }

    bool running() const { return running_; }

private:
    void stop() {
        if (!running_) return;

#ifdef _WIN32
        TerminateProcess(proc_, 0);
        WaitForSingleObject(proc_, 3000);
        CloseHandle(thread_);
        CloseHandle(proc_);
        proc_ = nullptr;
        thread_ = nullptr;
#else
        kill(pid_, SIGTERM);
        waitpid(pid_, nullptr, 0);
        pid_ = -1;
#endif
        running_ = false;
    }

#ifdef _WIN32
    HANDLE proc_ = nullptr;
    HANDLE thread_ = nullptr;
#else
    pid_t pid_ = -1;
#endif
    bool running_ = false;
};

static uint16_t reserve_ephemeral_port() {
    SocketHandle sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock == kInvalidSocket) return 0;

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = 0;

    if (bind(sock, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
        close_socket_handle(sock);
        return 0;
    }

    sockaddr_in bound{};
#ifdef _WIN32
    int len = static_cast<int>(sizeof(bound));
#else
    socklen_t len = sizeof(bound);
#endif
    if (getsockname(sock, reinterpret_cast<sockaddr*>(&bound), &len) != 0) {
        close_socket_handle(sock);
        return 0;
    }

    uint16_t port = ntohs(bound.sin_port);
    close_socket_handle(sock);
    return port;
}

static SocketHandle connect_with_retry(uint16_t port, int attempts = 30, int sleep_ms = 100) {
    for (int i = 0; i < attempts; ++i) {
        SocketHandle sock = socket(AF_INET, SOCK_STREAM, 0);
        if (sock == kInvalidSocket) return kInvalidSocket;

        sockaddr_in server{};
        server.sin_family = AF_INET;
        server.sin_port = htons(port);
        inet_pton(AF_INET, "127.0.0.1", &server.sin_addr);

        if (connect(sock, reinterpret_cast<sockaddr*>(&server), sizeof(server)) == 0) {
            return sock;
        }

        close_socket_handle(sock);
        std::this_thread::sleep_for(std::chrono::milliseconds(sleep_ms));
    }

    return kInvalidSocket;
}

static bool send_all(SocketHandle sock, const std::string& payload) {
    size_t sent = 0;
    while (sent < payload.size()) {
        int n = send(sock, payload.data() + sent, static_cast<int>(payload.size() - sent), 0);
        if (n <= 0) return false;
        sent += static_cast<size_t>(n);
    }
    return true;
}

static bool read_line(SocketHandle sock, std::string& out) {
    out.clear();
    char c = '\0';
    while (true) {
        int n = recv(sock, &c, 1, 0);
        if (n <= 0) return false;
        if (c == '\n') break;
        if (c != '\r') out.push_back(c);
    }
    return true;
}

static std::vector<std::string> read_until_end(SocketHandle sock) {
    std::vector<std::string> lines;
    std::string line;
    while (read_line(sock, line)) {
        if (line == "END") break;
        lines.push_back(line);
    }
    return lines;
}

static bool read_handshake(SocketHandle sock, std::string& session_line) {
    std::string hello;
    if (!read_line(sock, hello)) return false;
    if (hello != "HELLO VDB") return false;
    if (!read_line(sock, session_line)) return false;
    return true;
}

static std::pair<std::string, std::vector<std::string>> read_status_and_body(SocketHandle sock) {
    std::string status;
    if (!read_line(sock, status)) {
        return {"", {}};
    }
    return {status, read_until_end(sock)};
}

static std::string to_hex(const std::vector<uint8_t>& bytes) {
    static const char* kHex = "0123456789abcdef";
    std::string out;
    out.reserve(bytes.size() * 2);
    for (uint8_t b : bytes) {
        out.push_back(kHex[(b >> 4) & 0xF]);
        out.push_back(kHex[b & 0xF]);
    }
    return out;
}

static std::array<uint8_t, 32> sha256_bytes(const std::string& data) {
    auto rotr = [](uint32_t x, uint32_t n) { return (x >> n) | (x << (32 - n)); };
    auto ch = [](uint32_t x, uint32_t y, uint32_t z) { return (x & y) ^ (~x & z); };
    auto maj = [](uint32_t x, uint32_t y, uint32_t z) { return (x & y) ^ (x & z) ^ (y & z); };
    auto bsig0 = [&](uint32_t x) { return rotr(x, 2) ^ rotr(x, 13) ^ rotr(x, 22); };
    auto bsig1 = [&](uint32_t x) { return rotr(x, 6) ^ rotr(x, 11) ^ rotr(x, 25); };
    auto ssig0 = [&](uint32_t x) { return rotr(x, 7) ^ rotr(x, 18) ^ (x >> 3); };
    auto ssig1 = [&](uint32_t x) { return rotr(x, 17) ^ rotr(x, 19) ^ (x >> 10); };

    static const uint32_t k[64] = {
        0x428a2f98,0x71374491,0xb5c0fbcf,0xe9b5dba5,0x3956c25b,0x59f111f1,0x923f82a4,0xab1c5ed5,
        0xd807aa98,0x12835b01,0x243185be,0x550c7dc3,0x72be5d74,0x80deb1fe,0x9bdc06a7,0xc19bf174,
        0xe49b69c1,0xefbe4786,0x0fc19dc6,0x240ca1cc,0x2de92c6f,0x4a7484aa,0x5cb0a9dc,0x76f988da,
        0x983e5152,0xa831c66d,0xb00327c8,0xbf597fc7,0xc6e00bf3,0xd5a79147,0x06ca6351,0x14292967,
        0x27b70a85,0x2e1b2138,0x4d2c6dfc,0x53380d13,0x650a7354,0x766a0abb,0x81c2c92e,0x92722c85,
        0xa2bfe8a1,0xa81a664b,0xc24b8b70,0xc76c51a3,0xd192e819,0xd6990624,0xf40e3585,0x106aa070,
        0x19a4c116,0x1e376c08,0x2748774c,0x34b0bcb5,0x391c0cb3,0x4ed8aa4a,0x5b9cca4f,0x682e6ff3,
        0x748f82ee,0x78a5636f,0x84c87814,0x8cc70208,0x90befffa,0xa4506ceb,0xbef9a3f7,0xc67178f2
    };

    uint32_t h0 = 0x6a09e667, h1 = 0xbb67ae85, h2 = 0x3c6ef372, h3 = 0xa54ff53a;
    uint32_t h4 = 0x510e527f, h5 = 0x9b05688c, h6 = 0x1f83d9ab, h7 = 0x5be0cd19;

    std::vector<uint8_t> msg(data.begin(), data.end());
    uint64_t bit_len = static_cast<uint64_t>(msg.size()) * 8ULL;
    msg.push_back(0x80);
    while ((msg.size() % 64) != 56) msg.push_back(0);
    for (int i = 7; i >= 0; --i) msg.push_back(static_cast<uint8_t>((bit_len >> (i * 8)) & 0xFF));

    for (size_t off = 0; off < msg.size(); off += 64) {
        uint32_t w[64] = {0};
        for (int i = 0; i < 16; i++) {
            w[i] = (static_cast<uint32_t>(msg[off + i * 4]) << 24) |
                   (static_cast<uint32_t>(msg[off + i * 4 + 1]) << 16) |
                   (static_cast<uint32_t>(msg[off + i * 4 + 2]) << 8) |
                   (static_cast<uint32_t>(msg[off + i * 4 + 3]));
        }
        for (int i = 16; i < 64; i++) {
            w[i] = ssig1(w[i - 2]) + w[i - 7] + ssig0(w[i - 15]) + w[i - 16];
        }

        uint32_t a = h0, b = h1, c = h2, d = h3;
        uint32_t e = h4, f = h5, g = h6, h = h7;
        for (int i = 0; i < 64; i++) {
            uint32_t t1 = h + bsig1(e) + ch(e, f, g) + k[i] + w[i];
            uint32_t t2 = bsig0(a) + maj(a, b, c);
            h = g; g = f; f = e; e = d + t1;
            d = c; c = b; b = a; a = t1 + t2;
        }
        h0 += a; h1 += b; h2 += c; h3 += d;
        h4 += e; h5 += f; h6 += g; h7 += h;
    }

    std::array<uint8_t, 32> out{};
    uint32_t hv[8] = {h0,h1,h2,h3,h4,h5,h6,h7};
    for (int i = 0; i < 8; i++) {
        out[i * 4] = static_cast<uint8_t>((hv[i] >> 24) & 0xFF);
        out[i * 4 + 1] = static_cast<uint8_t>((hv[i] >> 16) & 0xFF);
        out[i * 4 + 2] = static_cast<uint8_t>((hv[i] >> 8) & 0xFF);
        out[i * 4 + 3] = static_cast<uint8_t>(hv[i] & 0xFF);
    }
    return out;
}

static std::string sha256_hex(const std::string& data) {
    const auto digest = sha256_bytes(data);
    return to_hex(std::vector<uint8_t>(digest.begin(), digest.end()));
}

static bool authenticate(SocketHandle sock, const std::string& user, const std::string& password) {
    if (!send_all(sock, "AUTH_START " + user + "\n")) return false;
    std::string challenge;
    if (!read_line(sock, challenge)) return false;
    if (challenge.rfind("AUTH_CHALLENGE ", 0) != 0) return false;

    std::istringstream iss(challenge);
    std::string tag, salt_hex, nonce, algo;
    iss >> tag >> salt_hex >> nonce >> algo;
    if (tag != "AUTH_CHALLENGE" || salt_hex.empty() || nonce.empty()) return false;
    if (algo != "sha256") return false;

    const std::string verifier = sha256_hex(salt_hex + password);
    const std::string proof = sha256_hex(verifier + nonce);
    if (!send_all(sock, "AUTH_PROOF " + proof + "\n")) return false;

    std::string result;
    if (!read_line(sock, result)) return false;
    return result.rfind("AUTH_OK ", 0) == 0;
}

static bool start_auth_challenge(SocketHandle sock,
                                 const std::string& user,
                                 std::string& salt_hex,
                                 std::string& nonce_hex,
                                 std::string& response_line) {
    if (!send_all(sock, "AUTH_START " + user + "\n")) return false;
    if (!read_line(sock, response_line)) return false;
    if (response_line.rfind("AUTH_CHALLENGE ", 0) != 0) return false;

    std::istringstream iss(response_line);
    std::string tag, algo;
    iss >> tag >> salt_hex >> nonce_hex >> algo;
    if (tag != "AUTH_CHALLENGE") return false;
    if (salt_hex.empty() || nonce_hex.empty()) return false;
    return algo == "sha256";
}

} // namespace

TEST_CASE("Server protocol: handshake, ping, quit", "[server][integration]") {
    DurabilityFileScope durability_scope;
#ifdef _WIN32
    WinSockScope ws;
    REQUIRE(ws.ok());
#endif

    const uint16_t port = reserve_ephemeral_port();
    REQUIRE(port != 0);

    ServerProcess server(port);
    REQUIRE(server.running());

    SocketHandle client = connect_with_retry(port);
    REQUIRE(client != kInvalidSocket);

    std::string line;
    REQUIRE(read_line(client, line));
    CHECK(line == "HELLO VDB");

    REQUIRE(read_line(client, line));
    CHECK_THAT(line, Catch::Matchers::StartsWith("SESSION 127.0.0.1:"));

    REQUIRE(send_all(client, "PING\n"));
    REQUIRE(read_line(client, line));
    CHECK(line == "PONG");

    REQUIRE(send_all(client, "QUIT\n"));
    REQUIRE(read_line(client, line));
    CHECK(line == "BYE");

    close_socket_handle(client);
}

TEST_CASE("Server protocol: SQL roundtrip", "[server][integration]") {
    DurabilityFileScope durability_scope;
#ifdef _WIN32
    WinSockScope ws;
    REQUIRE(ws.ok());
#endif

    const uint16_t port = reserve_ephemeral_port();
    REQUIRE(port != 0);

    ServerProcess server(port);
    REQUIRE(server.running());

    SocketHandle client = connect_with_retry(port);
    REQUIRE(client != kInvalidSocket);

    std::string line;
    REQUIRE(read_line(client, line)); // HELLO VDB
    REQUIRE(read_line(client, line)); // SESSION ...

    REQUIRE(send_all(client, "CREATE TABLE smoke_it (id INT);\n"));
    REQUIRE(read_line(client, line));
    CHECK(line == "OK");
    (void)read_until_end(client);

    REQUIRE(send_all(client, "INSERT INTO smoke_it VALUES (7);\n"));
    REQUIRE(read_line(client, line));
    CHECK(line == "OK");
    (void)read_until_end(client);

    REQUIRE(send_all(client, "SELECT id FROM smoke_it;\n"));
    REQUIRE(read_line(client, line));
    CHECK(line == "OK");
    auto rows = read_until_end(client);

    std::string body;
    for (const auto& r : rows) {
        body += r;
        body.push_back('\n');
    }

    CHECK_THAT(body, Catch::Matchers::ContainsSubstring("7"));
    CHECK_THAT(body, Catch::Matchers::ContainsSubstring("(1 rows)"));

    REQUIRE(send_all(client, "QUIT\n"));
    REQUIRE(read_line(client, line));
    CHECK(line == "BYE");

    close_socket_handle(client);
}

TEST_CASE("Server protocol: concurrent clients get distinct endpoint sessions", "[server][integration]") {
    DurabilityFileScope durability_scope;
#ifdef _WIN32
    WinSockScope ws;
    REQUIRE(ws.ok());
#endif

    const uint16_t port = reserve_ephemeral_port();
    REQUIRE(port != 0);

    ServerProcess server(port);
    REQUIRE(server.running());

    SocketHandle client_a = connect_with_retry(port);
    SocketHandle client_b = connect_with_retry(port);
    REQUIRE(client_a != kInvalidSocket);
    REQUIRE(client_b != kInvalidSocket);

    std::string session_a;
    std::string session_b;
    REQUIRE(read_handshake(client_a, session_a));
    REQUIRE(read_handshake(client_b, session_b));

    CHECK_THAT(session_a, Catch::Matchers::StartsWith("SESSION 127.0.0.1:"));
    CHECK_THAT(session_b, Catch::Matchers::StartsWith("SESSION 127.0.0.1:"));
    CHECK(session_a != session_b);

    std::string status_a;
    std::string status_b;
    bool send_ok_a = false;
    bool send_ok_b = false;
    bool read_ok_b = false;
    std::mutex result_mu;

    std::thread t1([&]() {
        send_ok_a = send_all(client_a, "CREATE TABLE cc_sessions (id INT);\n");
        if (!send_ok_a) return;
        auto [status, _] = read_status_and_body(client_a);
        std::lock_guard<std::mutex> g(result_mu);
        status_a = status;
    });

    std::thread t2([&]() {
        send_ok_b = send_all(client_b, "PING\n");
        if (!send_ok_b) return;
        std::string line;
        read_ok_b = read_line(client_b, line);
        if (!read_ok_b) return;
        std::lock_guard<std::mutex> g(result_mu);
        status_b = line;
    });

    t1.join();
    t2.join();

    CHECK(send_ok_a);
    CHECK(send_ok_b);
    CHECK(read_ok_b);
    CHECK(status_a == "OK");
    CHECK(status_b == "PONG");

    REQUIRE(send_all(client_a, "QUIT\n"));
    REQUIRE(send_all(client_b, "QUIT\n"));
    std::string bye;
    REQUIRE(read_line(client_a, bye));
    CHECK(bye == "BYE");
    REQUIRE(read_line(client_b, bye));
    CHECK(bye == "BYE");

    close_socket_handle(client_a);
    close_socket_handle(client_b);
}

TEST_CASE("Server protocol: malformed SQL returns ERROR with END", "[server][integration]") {
    DurabilityFileScope durability_scope;
#ifdef _WIN32
    WinSockScope ws;
    REQUIRE(ws.ok());
#endif

    const uint16_t port = reserve_ephemeral_port();
    REQUIRE(port != 0);

    ServerProcess server(port);
    REQUIRE(server.running());

    SocketHandle client = connect_with_retry(port);
    REQUIRE(client != kInvalidSocket);

    std::string session;
    REQUIRE(read_handshake(client, session));

    REQUIRE(send_all(client, "SELECT FROM broken_sql;\n"));
    auto [status, body] = read_status_and_body(client);

    CHECK(status == "ERROR");
    std::string text;
    for (const auto& line : body) {
        text += line;
        text.push_back('\n');
    }
    const bool has_parse_error = text.find("Parse error") != std::string::npos;
    const bool has_failed_parse = text.find("failed to parse query") != std::string::npos;
    CHECK((has_parse_error || has_failed_parse));

    REQUIRE(send_all(client, "QUIT\n"));
    std::string line;
    REQUIRE(read_line(client, line));
    CHECK(line == "BYE");
    close_socket_handle(client);
}

TEST_CASE("Server protocol: partial SQL yields CONTINUE and supports disconnect", "[server][integration]") {
    DurabilityFileScope durability_scope;
#ifdef _WIN32
    WinSockScope ws;
    REQUIRE(ws.ok());
#endif

    const uint16_t port = reserve_ephemeral_port();
    REQUIRE(port != 0);

    ServerProcess server(port);
    REQUIRE(server.running());

    SocketHandle client1 = connect_with_retry(port);
    REQUIRE(client1 != kInvalidSocket);
    std::string session;
    REQUIRE(read_handshake(client1, session));

    REQUIRE(send_all(client1, "SELECT\n"));
    std::string line;
    REQUIRE(read_line(client1, line));
    CHECK(line == "CONTINUE");

    // Disconnect mid-statement; server should handle it gracefully.
    close_socket_handle(client1);

    SocketHandle client2 = connect_with_retry(port);
    REQUIRE(client2 != kInvalidSocket);
    REQUIRE(read_handshake(client2, session));

    REQUIRE(send_all(client2, "PING\n"));
    REQUIRE(read_line(client2, line));
    CHECK(line == "PONG");

    REQUIRE(send_all(client2, "QUIT\n"));
    REQUIRE(read_line(client2, line));
    CHECK(line == "BYE");
    close_socket_handle(client2);
}

TEST_CASE("Server protocol: dot commands are supported over network", "[server][integration]") {
    DurabilityFileScope durability_scope;
#ifdef _WIN32
    WinSockScope ws;
    REQUIRE(ws.ok());
#endif

    const uint16_t port = reserve_ephemeral_port();
    REQUIRE(port != 0);

    ServerProcess server(port);
    REQUIRE(server.running());

    SocketHandle client = connect_with_retry(port);
    REQUIRE(client != kInvalidSocket);

    std::string session;
    REQUIRE(read_handshake(client, session));

    REQUIRE(send_all(client, "CREATE TABLE net_dot(id INT);\n"));
    auto [create_status, create_body] = read_status_and_body(client);
    CHECK(create_status == "OK");

    REQUIRE(send_all(client, ".tables\n"));
    auto [tables_status, tables_body] = read_status_and_body(client);
    CHECK(tables_status == "OK");
    std::string tables_text;
    for (const auto& line : tables_body) {
        tables_text += line;
        tables_text.push_back('\n');
    }
    CHECK_THAT(tables_text, Catch::Matchers::ContainsSubstring("net_dot"));

    REQUIRE(send_all(client, ".schema net_dot\n"));
    auto [schema_status, schema_body] = read_status_and_body(client);
    CHECK(schema_status == "OK");
    std::string schema_text;
    for (const auto& line : schema_body) {
        schema_text += line;
        schema_text.push_back('\n');
    }
    CHECK_THAT(schema_text, Catch::Matchers::ContainsSubstring("Table: net_dot"));
    CHECK_THAT(schema_text, Catch::Matchers::ContainsSubstring("id INT"));

    REQUIRE(send_all(client, "QUIT\n"));
    std::string bye;
    REQUIRE(read_line(client, bye));
    CHECK(bye == "BYE");
    close_socket_handle(client);
}

TEST_CASE("Server protocol: password auth requires login", "[server][integration][auth]") {
    DurabilityFileScope durability_scope;
#ifdef _WIN32
    WinSockScope ws;
    REQUIRE(ws.ok());
#endif

    const uint16_t port = reserve_ephemeral_port();
    REQUIRE(port != 0);

    ServerProcess server(port, true);
    REQUIRE(server.running());

    SocketHandle client = connect_with_retry(port);
    REQUIRE(client != kInvalidSocket);

    std::string session;
    REQUIRE(read_handshake(client, session));

    REQUIRE(send_all(client, ".help\n"));
    std::string help_status;
    REQUIRE(read_line(client, help_status));
    CHECK(help_status == "OK");
    auto help_body = read_until_end(client);
    std::string help_text;
    for (const auto& line : help_body) {
        help_text += line;
        help_text.push_back('\n');
    }
    CHECK_THAT(help_text, Catch::Matchers::ContainsSubstring(".tables"));

    REQUIRE(send_all(client, ".tables\n"));
    std::string dot_status;
    REQUIRE(read_line(client, dot_status));
    CHECK(dot_status == "ERROR");
    auto dot_body = read_until_end(client);
    std::string dot_text;
    for (const auto& line : dot_body) {
        dot_text += line;
        dot_text.push_back('\n');
    }
    CHECK_THAT(dot_text, Catch::Matchers::ContainsSubstring("auth_required"));

    REQUIRE(send_all(client, "SELECT 1;\n"));
    std::string status;
    REQUIRE(read_line(client, status));
    CHECK(status == "ERROR");
    auto body = read_until_end(client);
    std::string body_text;
    for (const auto& line : body) {
        body_text += line;
        body_text.push_back('\n');
    }
    CHECK_THAT(body_text, Catch::Matchers::ContainsSubstring("auth_required"));

    REQUIRE(send_all(client, "AUTH_START admin\n"));
    std::string challenge;
    REQUIRE(read_line(client, challenge));
    CHECK_THAT(challenge, Catch::Matchers::StartsWith("AUTH_CHALLENGE "));

    REQUIRE(send_all(client, "AUTH_PROOF deadbeef\n"));
    std::string auth_err;
    REQUIRE(read_line(client, auth_err));
    CHECK_THAT(auth_err, Catch::Matchers::StartsWith("AUTH_ERROR "));

    REQUIRE(send_all(client, "AUTH_PROOF deadbeef\n"));
    std::string stale_err;
    REQUIRE(read_line(client, stale_err));
    CHECK(stale_err == "AUTH_ERROR auth_nonce_expired");

    REQUIRE(send_all(client, "QUIT\n"));
    std::string bye;
    REQUIRE(read_line(client, bye));
    CHECK(bye == "BYE");
    close_socket_handle(client);
}

TEST_CASE("Server protocol: authenticated principal sees only authorized metadata", "[server][integration][auth]") {
    DurabilityFileScope durability_scope;
#ifdef _WIN32
    WinSockScope ws;
    REQUIRE(ws.ok());
#endif

    const uint16_t port = reserve_ephemeral_port();
    REQUIRE(port != 0);

    ServerProcess server(port, true);
    REQUIRE(server.running());

    SocketHandle admin = connect_with_retry(port);
    REQUIRE(admin != kInvalidSocket);
    std::string session;
    REQUIRE(read_handshake(admin, session));
    REQUIRE(authenticate(admin, "admin", "admin"));

    auto exec_ok = [&](const std::string& sql) {
        REQUIRE(send_all(admin, sql + "\n"));
        auto [status, body] = read_status_and_body(admin);
        CHECK(status == "OK");
        (void)body;
    };

    exec_ok("CREATE TABLE auth_t1(id INT);");
    exec_ok("CREATE TABLE auth_t2(id INT);");
    exec_ok("CREATE VIEW auth_v1 AS SELECT id FROM auth_t1;");
    exec_ok("CREATE FUNCTION auth_fn(x INT) RETURNS INT AS 'x + 1';");
    exec_ok("CREATE USER alice IDENTIFIED BY 'alicepw';");
    exec_ok("GRANT SELECT ON TABLE auth_t1 TO alice;");
    exec_ok("GRANT SELECT ON VIEW auth_v1 TO alice;");

    REQUIRE(send_all(admin, "QUIT\n"));
    std::string bye;
    REQUIRE(read_line(admin, bye));
    CHECK(bye == "BYE");
    close_socket_handle(admin);

    SocketHandle alice = connect_with_retry(port);
    REQUIRE(alice != kInvalidSocket);
    REQUIRE(read_handshake(alice, session));
    REQUIRE(authenticate(alice, "alice", "alicepw"));

    REQUIRE(send_all(alice, ".tables\n"));
    auto [tables_status, tables_body] = read_status_and_body(alice);
    CHECK(tables_status == "OK");
    std::string tables_text;
    for (const auto& line : tables_body) {
        tables_text += line;
        tables_text.push_back('\n');
    }
    CHECK_THAT(tables_text, Catch::Matchers::ContainsSubstring("auth_t1"));
    CHECK_THAT(tables_text, Catch::Matchers::ContainsSubstring("auth_v1"));
    CHECK(tables_text.find("auth_t2") == std::string::npos);

    REQUIRE(send_all(alice, ".schema auth_t2\n"));
    auto [schema2_status, schema2_body] = read_status_and_body(alice);
    CHECK(schema2_status == "OK");
    std::string schema2_text;
    for (const auto& line : schema2_body) {
        schema2_text += line;
        schema2_text.push_back('\n');
    }
    CHECK_THAT(schema2_text, Catch::Matchers::ContainsSubstring("Permission denied"));

    REQUIRE(send_all(alice, ".schema auth_v1\n"));
    auto [schema_v_status, schema_v_body] = read_status_and_body(alice);
    CHECK(schema_v_status == "OK");
    std::string schema_v_text;
    for (const auto& line : schema_v_body) {
        schema_v_text += line;
        schema_v_text.push_back('\n');
    }
    CHECK_THAT(schema_v_text, Catch::Matchers::ContainsSubstring("View: auth_v1"));

    REQUIRE(send_all(alice, ".functions udf\n"));
    auto [fn_status, fn_body] = read_status_and_body(alice);
    CHECK(fn_status == "OK");
    std::string fn_text;
    for (const auto& line : fn_body) {
        fn_text += line;
        fn_text.push_back('\n');
    }
    CHECK(fn_text.find("auth_fn") == std::string::npos);

    REQUIRE(send_all(alice, "QUIT\n"));
    REQUIRE(read_line(alice, bye));
    CHECK(bye == "BYE");
    close_socket_handle(alice);
}

TEST_CASE("Server protocol: replayed proof is rejected after rechallenge", "[server][integration][auth]") {
    DurabilityFileScope durability_scope;
#ifdef _WIN32
    WinSockScope ws;
    REQUIRE(ws.ok());
#endif

    const uint16_t port = reserve_ephemeral_port();
    REQUIRE(port != 0);

    ServerProcess server(port, true);
    REQUIRE(server.running());

    SocketHandle client = connect_with_retry(port);
    REQUIRE(client != kInvalidSocket);
    std::string session;
    REQUIRE(read_handshake(client, session));

    std::string salt1, nonce1, line;
    REQUIRE(start_auth_challenge(client, "admin", salt1, nonce1, line));
    const std::string verifier1 = sha256_hex(salt1 + std::string("admin"));
    const std::string proof1 = sha256_hex(verifier1 + nonce1);
    REQUIRE(send_all(client, "AUTH_PROOF " + proof1 + "\n"));
    std::string ok;
    REQUIRE(read_line(client, ok));
    CHECK_THAT(ok, Catch::Matchers::StartsWith("AUTH_OK "));

    std::string salt2, nonce2;
    REQUIRE(start_auth_challenge(client, "admin", salt2, nonce2, line));
    REQUIRE(send_all(client, "AUTH_PROOF " + proof1 + "\n"));
    std::string replay_err;
    REQUIRE(read_line(client, replay_err));
    CHECK(replay_err == "AUTH_ERROR auth_failed");

    REQUIRE(send_all(client, "QUIT\n"));
    std::string bye;
    REQUIRE(read_line(client, bye));
    CHECK(bye == "BYE");
    close_socket_handle(client);
}

TEST_CASE("Server protocol: repeated failed auth triggers lockout", "[server][integration][auth]") {
    DurabilityFileScope durability_scope;
#ifdef _WIN32
    WinSockScope ws;
    REQUIRE(ws.ok());
#endif

    const uint16_t port = reserve_ephemeral_port();
    REQUIRE(port != 0);

    ServerProcess server(port, true);
    REQUIRE(server.running());

    SocketHandle client = connect_with_retry(port);
    REQUIRE(client != kInvalidSocket);
    std::string session;
    REQUIRE(read_handshake(client, session));

    for (int i = 0; i < 3; i++) {
        std::string salt, nonce, line;
        REQUIRE(start_auth_challenge(client, "admin", salt, nonce, line));
        REQUIRE(send_all(client, "AUTH_PROOF deadbeef\n"));
        std::string err;
        REQUIRE(read_line(client, err));
        CHECK(err == "AUTH_ERROR auth_failed");
    }

    REQUIRE(send_all(client, "AUTH_START admin\n"));
    std::string locked;
    REQUIRE(read_line(client, locked));
    CHECK(locked == "AUTH_ERROR auth_locked");

    REQUIRE(send_all(client, "QUIT\n"));
    std::string bye;
    REQUIRE(read_line(client, bye));
    CHECK(bye == "BYE");
    close_socket_handle(client);
}

TEST_CASE("Server protocol: grant revoke matrix and ownership override", "[server][integration][auth]") {
    DurabilityFileScope durability_scope;
#ifdef _WIN32
    WinSockScope ws;
    REQUIRE(ws.ok());
#endif

    const uint16_t port = reserve_ephemeral_port();
    REQUIRE(port != 0);

    ServerProcess server(port, true);
    REQUIRE(server.running());

    SocketHandle admin = connect_with_retry(port);
    REQUIRE(admin != kInvalidSocket);
    std::string session;
    REQUIRE(read_handshake(admin, session));
    REQUIRE(authenticate(admin, "admin", "admin"));

    auto exec_admin = [&](const std::string& sql) {
        REQUIRE(send_all(admin, sql + "\n"));
        auto [status, body] = read_status_and_body(admin);
        CHECK(status == "OK");
        return body;
    };

    exec_admin("CREATE USER bob IDENTIFIED BY 'bobpw';");
    exec_admin("CREATE TABLE g_t(id INT);");
    exec_admin("INSERT INTO g_t VALUES (1);");
    exec_admin("CREATE VIEW g_v AS SELECT id FROM g_t;");
    exec_admin("CREATE FUNCTION g_fn(x INT) RETURNS INT AS 'x + 1';");

    exec_admin("GRANT SELECT ON TABLE g_t TO bob;");
    exec_admin("GRANT SELECT ON VIEW g_v TO bob;");
    exec_admin("GRANT EXECUTE ON FUNCTION g_fn TO bob;");

    REQUIRE(send_all(admin, "QUIT\n"));
    std::string bye;
    REQUIRE(read_line(admin, bye));
    CHECK(bye == "BYE");
    close_socket_handle(admin);

    SocketHandle bob = connect_with_retry(port);
    REQUIRE(bob != kInvalidSocket);
    REQUIRE(read_handshake(bob, session));
    REQUIRE(authenticate(bob, "bob", "bobpw"));

    REQUIRE(send_all(bob, "SELECT id FROM g_t;\n"));
    auto [sel_ok_status, sel_ok_body] = read_status_and_body(bob);
    CHECK(sel_ok_status == "OK");

    REQUIRE(send_all(bob, ".functions udf\n"));
    auto [fn_status_before, fn_body_before] = read_status_and_body(bob);
    CHECK(fn_status_before == "OK");
    std::string fn_before_text;
    for (const auto& line : fn_body_before) {
        fn_before_text += line;
        fn_before_text.push_back('\n');
    }
    CHECK_THAT(fn_before_text, Catch::Matchers::ContainsSubstring("g_fn"));

    REQUIRE(send_all(bob, "CREATE TABLE bob_own(id INT);\n"));
    auto [own_create_status, own_create_body] = read_status_and_body(bob);
    CHECK(own_create_status == "OK");
    REQUIRE(send_all(bob, "DROP TABLE bob_own;\n"));
    auto [own_drop_status, own_drop_body] = read_status_and_body(bob);
    CHECK(own_drop_status == "OK");

    REQUIRE(send_all(bob, "QUIT\n"));
    REQUIRE(read_line(bob, bye));
    CHECK(bye == "BYE");
    close_socket_handle(bob);

    admin = connect_with_retry(port);
    REQUIRE(admin != kInvalidSocket);
    REQUIRE(read_handshake(admin, session));
    REQUIRE(authenticate(admin, "admin", "admin"));
    exec_admin("REVOKE SELECT ON TABLE g_t FROM bob;");
    exec_admin("REVOKE EXECUTE ON FUNCTION g_fn FROM bob;");
    REQUIRE(send_all(admin, "QUIT\n"));
    REQUIRE(read_line(admin, bye));
    CHECK(bye == "BYE");
    close_socket_handle(admin);

    bob = connect_with_retry(port);
    REQUIRE(bob != kInvalidSocket);
    REQUIRE(read_handshake(bob, session));
    REQUIRE(authenticate(bob, "bob", "bobpw"));

    REQUIRE(send_all(bob, "SELECT id FROM g_t;\n"));
    auto [sel_denied_status, sel_denied_body] = read_status_and_body(bob);
    CHECK(sel_denied_status == "ERROR");
    std::string sel_denied_text;
    for (const auto& line : sel_denied_body) {
        sel_denied_text += line;
        sel_denied_text.push_back('\n');
    }
    CHECK_THAT(sel_denied_text, Catch::Matchers::ContainsSubstring("permission_denied"));

    REQUIRE(send_all(bob, ".functions udf\n"));
    auto [fn_status_after, fn_body_after] = read_status_and_body(bob);
    CHECK(fn_status_after == "OK");
    std::string fn_after_text;
    for (const auto& line : fn_body_after) {
        fn_after_text += line;
        fn_after_text.push_back('\n');
    }
    CHECK(fn_after_text.find("g_fn") == std::string::npos);

    REQUIRE(send_all(bob, "QUIT\n"));
    REQUIRE(read_line(bob, bye));
    CHECK(bye == "BYE");
    close_socket_handle(bob);
}
