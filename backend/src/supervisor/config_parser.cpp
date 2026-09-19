/**
 * supervisor 配置文件解析器
 *
 * 简洁的行式配置格式（# 开头为注释）：
 *
 *   global {
 *       backoff_start_ms  = 100
 *       backoff_cap_ms    = 5000
 *       stable_uptime_ms  = 3000
 *   }
 *
 *   service "web" {
 *       command         = "/usr/bin/myapp" "--port" "8080"
 *       stop_signal     = TERM
 *       stop_timeout_ms = 2000
 *       max_restarts    = 3
 *       cwd             = "/var/lib/myapp"
 *       env LANG        = "C.UTF-8"
 *   }
 *
 * 解析要么整体成功、要么整体失败（错误带行号），
 * 调用方据此保证 SIGHUP 热加载时“解析失败则旧服务不受影响”。
 */

#include "supervisor.h"
#include "signal_util.h"

#include <cctype>
#include <cerrno>
#include <cstring>
#include <fstream>
#include <sstream>
#include <string>

namespace ipc {

namespace {

struct Token {
    enum Kind { IDENT, STRING, NUMBER, LBRACE, RBRACE, EQ, END } kind;
    std::string text;
    int line;
};

struct Lexer {
    const std::string& src;
    size_t pos = 0;
    int line = 1;
    std::string err;

    explicit Lexer(const std::string& s) : src(s) {}

    [[noreturn]] void fail(const std::string& msg) {
        std::ostringstream os;
        os << "line " << line << ": " << msg;
        err = os.str();
        throw std::runtime_error(err);
    }

    void skip_ws() {
        while (pos < src.size()) {
            char c = src[pos];
            if (c == '#') {  // 行注释
                while (pos < src.size() && src[pos] != '\n') ++pos;
            } else if (std::isspace(static_cast<unsigned char>(c))) {
                if (c == '\n') ++line;
                ++pos;
            } else {
                break;
            }
        }
    }

    Token next() {
        skip_ws();
        if (pos >= src.size()) return {Token::END, "", line};
        char c = src[pos];
        int start_line = line;
        if (c == '{') { ++pos; return {Token::LBRACE, "{", start_line}; }
        if (c == '}') { ++pos; return {Token::RBRACE, "}", start_line}; }
        if (c == '=') { ++pos; return {Token::EQ, "=", start_line}; }
        if (c == '"') return lex_string(start_line);
        if (std::isdigit(static_cast<unsigned char>(c)) ||
            (c == '-' && pos + 1 < src.size() &&
             std::isdigit(static_cast<unsigned char>(src[pos + 1])))) {
            return lex_number(start_line);
        }
        if (std::isalpha(static_cast<unsigned char>(c)) || c == '_') {
            size_t start = pos;
            while (pos < src.size() &&
                   (std::isalnum(static_cast<unsigned char>(src[pos])) ||
                    src[pos] == '_' || src[pos] == '.')) {
                ++pos;
            }
            return {Token::IDENT, src.substr(start, pos - start), start_line};
        }
        fail(std::string("unexpected character '") + c + "'");
    }

    Token lex_string(int start_line) {
        ++pos;  // 跳过开引号
        std::string value;
        while (pos < src.size() && src[pos] != '"') {
            char ch = src[pos];
            if (ch == '\n') fail("unterminated string");
            if (ch == '\\') {
                ++pos;
                if (pos >= src.size()) fail("unterminated escape");
                switch (src[pos]) {
                    case 'n':  value.push_back('\n'); break;
                    case 't':  value.push_back('\t'); break;
                    case 'r':  value.push_back('\r'); break;
                    case '\\': value.push_back('\\'); break;
                    case '"':  value.push_back('"');  break;
                    default:
                        fail(std::string("invalid escape '\\") + src[pos] + "'");
                }
            } else {
                value.push_back(ch);
            }
            ++pos;
        }
        if (pos >= src.size()) fail("unterminated string");
        ++pos;  // 跳过闭引号
        return {Token::STRING, value, start_line};
    }

    Token lex_number(int start_line) {
        size_t start = pos;
        if (src[pos] == '-') ++pos;
        while (pos < src.size() && std::isdigit(static_cast<unsigned char>(src[pos]))) ++pos;
        return {Token::NUMBER, src.substr(start, pos - start), start_line};
    }
};

struct Parser {
    Lexer lex;
    Token cur;

    explicit Parser(const std::string& src) : lex(src), cur(lex.next()) {}

    [[noreturn]] void fail(const std::string& msg) { lex.fail(msg); }

    void advance() { cur = lex.next(); }

    bool accept(Token::Kind kind) {
        if (cur.kind == kind) { advance(); return true; }
        return false;
    }

    void expect(Token::Kind kind, const std::string& what) {
        if (cur.kind != kind) fail("expected " + what + " but got '" + cur.text + "'");
        advance();
    }

    // '=' 可选，降低格式负担
    void accept_eq() {
        if (cur.kind == Token::EQ) advance();
    }

    int expect_int(const std::string& what) {
        if (cur.kind != Token::NUMBER) fail(what + " expects an integer");
        int value;
        try {
            size_t used = 0;
            value = std::stoi(cur.text, &used);
            if (used != cur.text.size()) throw std::exception();
        } catch (...) {
            fail(what + " has invalid integer value '" + cur.text + "'");
        }
        advance();
        return value;
    }

    std::string expect_stringish(const std::string& what) {
        if (cur.kind != Token::STRING && cur.kind != Token::IDENT)
            fail(what + " expects a string");
        std::string v = cur.text;
        advance();
        return v;
    }

    void parse_global(SupervisorConfig& cfg) {
        expect(Token::LBRACE, "'{' after global");
        while (cur.kind != Token::RBRACE) {
            if (cur.kind != Token::IDENT) fail("expected global setting name");
            std::string key = cur.text;
            advance();
            accept_eq();
            int v = expect_int(key);
            if (key == "backoff_start_ms") {
                if (v < 1) fail("backoff_start_ms must be >= 1");
                cfg.backoff_start_ms = v;
            } else if (key == "backoff_cap_ms") {
                if (v < 1) fail("backoff_cap_ms must be >= 1");
                cfg.backoff_cap_ms = v;
            } else if (key == "stable_uptime_ms") {
                if (v < 0) fail("stable_uptime_ms must be >= 0");
                cfg.stable_uptime_ms = v;
            } else {
                fail("unknown global setting '" + key + "'");
            }
        }
        expect(Token::RBRACE, "'}'");
        if (cfg.backoff_cap_ms < cfg.backoff_start_ms)
            fail("backoff_cap_ms must be >= backoff_start_ms");
    }

    void parse_service(SupervisorConfig& cfg) {
        if (cur.kind != Token::STRING) fail("service name must be a quoted string");
        std::string name = cur.text;
        if (name.empty()) fail("service name must not be empty");
        advance();
        expect(Token::LBRACE, "'{' after service name");

        ServiceConfig svc;
        svc.name = name;
        bool seen_command = false, seen_stop = false, seen_timeout = false;
        bool seen_restarts = false, seen_cwd = false;

        while (cur.kind != Token::RBRACE) {
            if (cur.kind != Token::IDENT) fail("expected a setting name in service block");
            std::string key = cur.text;
            advance();

            if (key == "command") {
                if (seen_command) fail("duplicate command");
                seen_command = true;
                accept_eq();
                if (cur.kind != Token::STRING) fail("command requires at least one quoted argument");
                while (cur.kind == Token::STRING) {
                    svc.argv.push_back(cur.text);
                    advance();
                }
                if (svc.argv[0].empty()) fail("command path must not be empty");
            } else if (key == "stop_signal") {
                if (seen_stop) fail("duplicate stop_signal");
                seen_stop = true;
                accept_eq();
                svc.stop_signal = expect_stringish("stop_signal");
                if (svp::signal_from_name(svc.stop_signal) < 0)
                    fail("unknown signal '" + svc.stop_signal + "'");
            } else if (key == "stop_timeout_ms") {
                if (seen_timeout) fail("duplicate stop_timeout_ms");
                seen_timeout = true;
                accept_eq();
                svc.stop_timeout_ms = expect_int("stop_timeout_ms");
                if (svc.stop_timeout_ms < 0) fail("stop_timeout_ms must be >= 0");
            } else if (key == "max_restarts") {
                if (seen_restarts) fail("duplicate max_restarts");
                seen_restarts = true;
                accept_eq();
                svc.max_restarts = expect_int("max_restarts");
                if (svc.max_restarts < 0) fail("max_restarts must be >= 0");
            } else if (key == "cwd") {
                if (seen_cwd) fail("duplicate cwd");
                seen_cwd = true;
                accept_eq();
                if (cur.kind != Token::STRING) fail("cwd expects a quoted string");
                svc.cwd = cur.text;
                advance();
            } else if (key == "env") {
                if (cur.kind != Token::IDENT)
                    fail("env expects a variable name, e.g. env FOO = \"bar\"");
                std::string var = cur.text;
                advance();
                accept_eq();
                if (cur.kind != Token::STRING) fail("env " + var + " expects a quoted value");
                std::string val = cur.text;
                advance();
                for (const auto& e : svc.env)
                    if (e.first == var) fail("duplicate env variable '" + var + "'");
                svc.env.emplace_back(var, val);
            } else {
                fail("unknown service setting '" + key + "'");
            }
        }
        expect(Token::RBRACE, "'}'");

        if (!seen_command || svc.argv.empty())
            fail("service '" + name + "' is missing a command");
        if (cfg.services.count(name))
            fail("duplicate service name '" + name + "'");
        cfg.services.emplace(name, std::move(svc));
    }

    SupervisorConfig parse() {
        SupervisorConfig cfg;
        while (cur.kind != Token::END) {
            if (cur.kind == Token::IDENT && cur.text == "service") {
                advance();
                parse_service(cfg);
            } else if (cur.kind == Token::IDENT && cur.text == "global") {
                advance();
                parse_global(cfg);
            } else {
                fail("expected 'service' or 'global' but got '" + cur.text + "'");
            }
            // parse_service/parse_global 已消费结尾的 '}'，cur 指向下一个 token
        }
        return cfg;
    }
};

}  // namespace

std::string ServiceConfig::runtime_signature() const {
    std::ostringstream os;
    os << "cmd=";
    for (const auto& a : argv) os << a << '\x1f';
    os << "|stop=" << stop_signal
       << "|timeout=" << stop_timeout_ms
       << "|restarts=" << max_restarts
       << "|cwd=" << cwd << "|env=";
    for (const auto& e : env) os << e.first << '=' << e.second << '\x1f';
    return os.str();
}

bool parse_supervisor_config(const std::string& path,
                             SupervisorConfig& out,
                             std::string& err) {
    std::ifstream f(path);
    if (!f) {
        err = "cannot open config file '" + path + "': " + std::strerror(errno);
        return false;
    }
    std::ostringstream ss;
    ss << f.rdbuf();
    std::string src = ss.str();

    // 空文件（或仅含空白/注释）合法：由词法器跳过注释后自然得到空配置
    try {
        Parser parser(src);
        out = parser.parse();
    } catch (const std::exception& e) {
        err = std::string("config parse error: ") + e.what();
        return false;
    }
    return true;
}

}  // namespace ipc
