#ifndef DDCKV_RS_CODING_H
#define DDCKV_RS_CODING_H

#include "core/logging.hpp"

#include<isa-l.h>
#include<iostream>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <boost/foreach.hpp>

#define MMAX 255

using namespace std;

typedef unsigned char u8;

class RsCoding{
private:
    int k;
    int m;
    int k_m;
    int len;

public:

    // Coefficient matrices
	u8 *g_tbls_encoding;
    u8 *g_tbls_decoding;

    u8 *encode_matrix;
    u8 *decode_matrix;
    u8 *invert_matrix;
    u8 *temp_matrix;

    RsCoding(const char * fname, int k_, int m_, int len_);
    ~RsCoding();

    void load_config(const char *fname);

    void encode_data(unsigned char **data, int encoding_len);

    void decode_data(unsigned char **data, int encoding_len, u8 * frag_err_list, int nerrs);

};

int gf_gen_decode_matrix_simple(u8 * encode_matrix,
				       u8 * decode_matrix,
				       u8 * invert_matrix,
				       u8 * temp_matrix,
				       u8 * frag_err_list, int nerrs, int k,
				       int m);

#endif