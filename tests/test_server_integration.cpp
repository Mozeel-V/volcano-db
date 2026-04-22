#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
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

    REQUIRE(send_all(client, "QUIT\n"));
    std::string bye;
    REQUIRE(read_line(client, bye));
    CHECK(bye == "BYE");
    close_socket_handle(client);
}
