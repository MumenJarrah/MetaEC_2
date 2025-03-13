#ifndef DDCKV_CITY_HASH_H
#define DDCKV_CITY_HASH_H

#include <stdint.h>
#include <map>
#include <memory.h>
#include <iostream>
using namespace std;

typedef std::pair<uint64_t, uint64_t> uint128_t;
#define STATIC_INLINE static inline

STATIC_INLINE uint64_t uint128_low64(const uint128_t& x) { return x.first; }
STATIC_INLINE uint64_t uint128_high64(const uint128_t& x) { return x.second; }

static const uint64_t kSeed0 = 1234567;
static const uint64_t kSeed1 = 0xc3a5c85c97cb3127ULL;
static const uint128_t kSeed128(kSeed0, kSeed1);

STATIC_INLINE uint64_t unaligned_load64(const char *p) {
  uint64_t result;
  memcpy(&result, p, sizeof(result));
  return result;
}

STATIC_INLINE uint32_t unaligned_load32(const char *p) {
  uint32_t result;
  memcpy(&result, p, sizeof(result));
  return result;
}

#define uint32_in_expected_order(x) (x)
#define uint64_in_expected_order(x) (x)

STATIC_INLINE uint32_t bswap_32(const uint32_t x) {
  uint32_t y = x;
  for (size_t i = 0; i<sizeof(uint32_t)>> 1; ++i) {
    uint32_t d = sizeof(uint32_t) - i - 1;
    uint32_t mh = ((uint32_t)0xff) << (d << 3);
    uint32_t ml = ((uint32_t)0xff) << (i << 3);
    uint32_t h = x & mh;
    uint32_t l = x & ml;
    uint64_t t = (l << ((d - i) << 3)) | (h >> ((d - i) << 3));
    y = t | (y & ~(mh | ml));
  }
  return y;
}

STATIC_INLINE uint64_t bswap_64(const uint64_t x) {
  uint64_t y = x;
  for (size_t i = 0; i<sizeof(uint64_t)>> 1; ++i) {
    uint64_t d = sizeof(uint64_t) - i - 1;
    uint64_t mh = ((uint64_t)0xff) << (d << 3);
    uint64_t ml = ((uint64_t)0xff) << (i << 3);
    uint64_t h = x & mh;
    uint64_t l = x & ml;
    uint64_t t = (l << ((d - i) << 3)) | (h >> ((d - i) << 3));
    y = t | (y & ~(mh | ml));
  }
  return y;
}

// Hash 128 input bits down to 64 bits of output.
// This is intended to be a reasonably good hash function.
STATIC_INLINE uint64_t hash128_to_64(const uint128_t& x) {
  // Murmur-inspired hashing.
  const uint64_t kMul = 0x9ddfea08eb382d69ULL;
  uint64_t a = (uint128_low64(x) ^ uint128_high64(x)) * kMul;
  a ^= (a >> 47);
  uint64_t b = (uint128_high64(x) ^ a) * kMul;
  b ^= (b >> 47);
  b *= kMul;
  return b;
}

STATIC_INLINE uint64_t fetch_64(const char *p) {
  return uint64_in_expected_order(unaligned_load64(p));
}

STATIC_INLINE uint32_t fetch_32(const char *p) {
  return uint32_in_expected_order(unaligned_load32(p));
}

// Bitwise right rotate.  Normally this will compile to a single
// instruction, especially if the shift is a manifest constant.
STATIC_INLINE uint64_t rotate(uint64_t val, int shift) {
  // Avoid shifting by 64: doing so yields an undefined result.
  return shift == 0 ? val : ((val >> shift) | (val << (64 - shift)));
}

STATIC_INLINE uint64_t shift_mix(uint64_t val) {
  return val ^ (val >> 47);
}

STATIC_INLINE uint64_t hash_len_16(uint64_t u, uint64_t v) {
  return hash128_to_64(uint128_t(u, v));
}

STATIC_INLINE uint64_t hash_len_16(uint64_t u, uint64_t v, uint64_t mul) {
  // Murmur-inspired hashing.
  uint64_t a = (u ^ v) * mul;
  a ^= (a >> 47);
  uint64_t b = (v ^ a) * mul;
  b ^= (b >> 47);
  b *= mul;
  return b;
}

// Some primes between 2^63 and 2^64 for various uses.
static const uint64_t k0 = 0xc3a5c85c97cb3127ULL;
static const uint64_t k1 = 0xb492b66fbe98f273ULL;
static const uint64_t k2 = 0x9ae16a3b2f90404fULL;

STATIC_INLINE uint64_t hash_len_0_to_16(const char *s, size_t len) {
  if (len >= 8) {
    uint64_t mul = k2 + len * 2;
    uint64_t a = fetch_64(s) + k2;
    uint64_t b = fetch_64(s + len - 8);
    uint64_t c = rotate(b, 37) * mul + a;
    uint64_t d = (rotate(a, 25) + b) * mul;
    return hash_len_16(c, d, mul);
  }
  if (len >= 4) {
    uint64_t mul = k2 + len * 2;
    uint64_t a = fetch_32(s);
    return hash_len_16(len + (a << 3), fetch_32(s + len - 4), mul);
  }
  if (len > 0) {
    uint8_t a = s[0];
    uint8_t b = s[len >> 1];
    uint8_t c = s[len - 1];
    uint32_t y = static_cast<uint32_t>(a) + (static_cast<uint32_t>(b) << 8);
    uint32_t z = len + (static_cast<uint32_t>(c) << 2);
    return shift_mix(y * k2 ^ z * k0) * k2;
  }
  return k2;
}

// This probably works well for 16-byte strings as well, but it may be overkill
// in that case.
STATIC_INLINE uint64_t hash_len_17_to_32(const char *s, size_t len) {
  uint64_t mul = k2 + len * 2;
  uint64_t a = fetch_64(s) * k1;
  uint64_t b = fetch_64(s + 8);
  uint64_t c = fetch_64(s + len - 8) * mul;
  uint64_t d = fetch_64(s + len - 16) * k2;
  return hash_len_16(rotate(a + b, 43) + rotate(c, 30) + d,
                   a + rotate(b + k2, 18) + c, mul);
}

// Return a 16-byte hash for 48 bytes.  Quick and dirty.
// Callers do best to use "random-looking" values for a and b.
STATIC_INLINE pair<uint64_t, uint64_t> weak_hash_len_32_with_seeds(
    uint64_t w, uint64_t x, uint64_t y, uint64_t z, uint64_t a, uint64_t b) {
  a += w;
  b = rotate(b + a + z, 21);
  uint64_t c = a;
  a += x;
  a += y;
  b += rotate(a, 44);
  return make_pair(a + z, b + c);
}

// Return a 16-byte hash for s[0] ... s[31], a, and b.  Quick and dirty.
STATIC_INLINE pair<uint64_t, uint64_t> weak_hash_len_32_with_seeds(
    const char* s, uint64_t a, uint64_t b) {
  return weak_hash_len_32_with_seeds(fetch_64(s),
                                fetch_64(s + 8),
                                fetch_64(s + 16),
                                fetch_64(s + 24),
                                a,
                                b);
}

// Return an 8-byte hash for 33 to 64 bytes.
STATIC_INLINE uint64_t hash_len_33_to_64(const char *s, size_t len) {
  uint64_t mul = k2 + len * 2;
  uint64_t a = fetch_64(s) * k2;
  uint64_t b = fetch_64(s + 8);
  uint64_t c = fetch_64(s + len - 24);
  uint64_t d = fetch_64(s + len - 32);
  uint64_t e = fetch_64(s + 16) * k2;
  uint64_t f = fetch_64(s + 24) * 9;
  uint64_t g = fetch_64(s + len - 8);
  uint64_t h = fetch_64(s + len - 16) * mul;
  uint64_t u = rotate(a + g, 43) + (rotate(b, 30) + c) * 9;
  uint64_t v = ((a + g) ^ d) + f + 1;
  uint64_t w = bswap_64((u + v) * mul) + h;
  uint64_t x = rotate(e + f, 42) + c;
  uint64_t y = (bswap_64((v + w) * mul) + g) * mul;
  uint64_t z = e + f + c;
  a = bswap_64((x + z) * mul + y) + b;
  b = shift_mix((z + a) * mul + d + h) * mul;
  return b + x;
}

STATIC_INLINE uint64_t city_hash_64(const char *s, size_t len) {
  if (len <= 32) {
    if (len <= 16) {
      return hash_len_0_to_16(s, len);
    } else {
      return hash_len_17_to_32(s, len);
    }
  } else if (len <= 64) {
    return hash_len_33_to_64(s, len);
  }

  // For strings over 64 bytes we hash the end first, and then as we
  // loop we keep 56 bytes of state: v, w, x, y, and z.
  uint64_t x = fetch_64(s + len - 40);
  uint64_t y = fetch_64(s + len - 16) + fetch_64(s + len - 56);
  uint64_t z = hash_len_16(fetch_64(s + len - 48) + len, fetch_64(s + len - 24));
  pair<uint64_t, uint64_t> v = weak_hash_len_32_with_seeds(s + len - 64, len, z);
  pair<uint64_t, uint64_t> w = weak_hash_len_32_with_seeds(s + len - 32, y + k1, x);
  x = x * k1 + fetch_64(s);

  // Decrease len to the nearest multiple of 64, and operate on 64-byte chunks.
  len = (len - 1) & ~static_cast<size_t>(63);
  do {
    x = rotate(x + y + v.first + fetch_64(s + 8), 37) * k1;
    y = rotate(y + v.second + fetch_64(s + 48), 42) * k1;
    x ^= w.second;
    y += v.first + fetch_64(s + 40);
    z = rotate(z + w.first, 33) * k1;
    v = weak_hash_len_32_with_seeds(s, v.second * k1, x + w.first);
    w = weak_hash_len_32_with_seeds(s + 32, z + w.second, y + fetch_64(s + 16));
    std::swap(z, x);
    s += 64;
    len -= 64;
  } while (len != 0);
  return hash_len_16(hash_len_16(v.first, w.first) + shift_mix(y) * k1 + z,
                   hash_len_16(v.second, w.second) + x);
}

#endif