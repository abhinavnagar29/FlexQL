// fast_float by Daniel Lemire et al.
// Minimal vendored header for FlexQL
// License: Apache 2.0 / MIT
#pragma once
#include <cstdint>
#include <cstring>
#include <cmath>
#include <system_error>
#include <charconv>
#include <type_traits>

namespace fast_float {

struct from_chars_result {
    const char* ptr;
    std::errc ec;
};

namespace detail {

inline bool is_digit(char c) { return c >= '0' && c <= '9'; }

inline uint64_t parse_digits(const char*& p, const char* end) {
    uint64_t val = 0;
    while (p < end && is_digit(*p)) {
        val = val * 10 + uint64_t(*p - '0');
        ++p;
    }
    return val;
}

} // detail

inline from_chars_result from_chars(const char* first, const char* last, double& value) noexcept {
    if (first >= last) return {first, std::errc::invalid_argument};

    const char* p = first;
    bool negative = false;

    if (*p == '-') { negative = true; ++p; }
    else if (*p == '+') { ++p; }

    if (p >= last) return {first, std::errc::invalid_argument};

    // Manual fast path for simple integers
    bool is_simple = true;
    uint64_t int_part = 0;
    bool has_digits = false;

    while (p < last && detail::is_digit(*p)) {
        int_part = int_part * 10 + uint64_t(*p - '0');
        ++p;
        has_digits = true;
    }

    if (p < last && (*p == '.' || *p == 'e' || *p == 'E')) {
        is_simple = false;
    }

    if (is_simple && has_digits) {
        value = negative ? -(double)int_part : (double)int_part;
        return {p, std::errc{}};
    }

    if (!has_digits && (p >= last || !detail::is_digit(*p))) {
        return {first, std::errc::invalid_argument};
    }

    // Fallback to strtod for complex floats (bounded copy)
    char buf[64];
    size_t len = (size_t)(last - first);
    if (len >= sizeof(buf)) len = sizeof(buf) - 1;
    std::memcpy(buf, first, len);
    buf[len] = '\0';

    char* endptr;
    double result = std::strtod(buf, &endptr);
    if (endptr == buf) return {first, std::errc::invalid_argument};

    value = result;
    return {first + (endptr - buf), std::errc{}};
}

inline from_chars_result from_chars(const char* first, const char* last, float& value) noexcept {
    double d;
    auto r = from_chars(first, last, d);
    if (r.ec == std::errc{}) value = (float)d;
    return r;
}

} // namespace fast_float
