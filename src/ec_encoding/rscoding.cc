#include "rscoding.h"

RsCoding::RsCoding(const char * fname, int k_, int m_, int len_){
    if(fname){
        cout << "load_config..." << endl;
        load_config(fname);
    }
    else {
        k = k_;
        m = m_;
        len = len_;
    }

    k_m = k + m;

    encode_matrix = (u8 *)malloc(sizeof(u8) * k_m * k);
    decode_matrix = (u8 *)malloc(sizeof(u8) * k_m * k);
    invert_matrix = (u8 *)malloc(sizeof(u8) * k_m * k);
    temp_matrix = (u8 *)malloc(sizeof(u8) * k_m * k);
    g_tbls_encoding = (u8 *)malloc(sizeof(u8) * k * m * 32);
    g_tbls_decoding = (u8 *)malloc(sizeof(u8) * k * m * 32);

    if (encode_matrix == NULL || decode_matrix == NULL || g_tbls_encoding == NULL || g_tbls_decoding == NULL) {
		printf("Test failure! Error with malloc\n");
	}
    
    gf_gen_cauchy1_matrix(encode_matrix, k_m, k);
    ec_init_tables(k, m, &encode_matrix[k * k], g_tbls_encoding);
}

RsCoding::~RsCoding(){
    free(g_tbls_encoding);
}

void RsCoding::load_config(const char * fname){
    std::fstream config_fs(fname);

    boost::property_tree::ptree pt;
    try {
        boost::property_tree::read_json(config_fs, pt);
    } catch (boost::property_tree::ptree_error & e) {
        printf("read json error!\n");
    }

    try {
        k = pt.get<int>("k");
        m = pt.get<int>("m");
        len = pt.get<int>("len");

    } catch (boost::property_tree::ptree_error & e) {
        printf("load error!\n");
    }
}

void RsCoding::encode_data(unsigned char **data, int encoding_len){
    ec_encode_data(encoding_len, k, m, g_tbls_encoding, data, &data[k]);
}

void RsCoding::decode_data(unsigned char **data, int encoding_len, u8 *frag_err_list, int nerrs){
    gf_gen_decode_matrix_simple(encode_matrix, decode_matrix,
					  invert_matrix, temp_matrix,
					  frag_err_list, nerrs, k, k_m);

    ec_init_tables(k, nerrs, decode_matrix, g_tbls_decoding);
    ec_encode_data(encoding_len, k, nerrs, g_tbls_decoding, data, &data[k]);

}

int gf_gen_decode_matrix_simple(u8 * encode_matrix,
				       u8 * decode_matrix,
				       u8 * invert_matrix,
				       u8 * temp_matrix,
				       u8 * frag_err_list, int nerrs, int k,
				       int m)
{
	int i, j, p, r;
	int nsrcerrs = 0;
	u8 s, *b = temp_matrix;
	u8 frag_in_err[MMAX];

	memset(frag_in_err, 0, sizeof(frag_in_err));

	// Order the fragments in erasure for easier sorting
	for (i = 0; i < nerrs; i++) {
		if (frag_err_list[i] < k)
			nsrcerrs++;
		frag_in_err[frag_err_list[i]] = 1;
	}

	// Construct b (matrix that encoded remaining frags) by removing erased rows
	for (i = 0, r = 0; i < k; i++, r++) {
		while (frag_in_err[r])
			r++;
		for (j = 0; j < k; j++)
			b[k * i + j] = encode_matrix[k * r + j];
	}

	// Invert matrix to get recovery matrix
	if (gf_invert_matrix(b, invert_matrix, k) < 0)
		return -1;

	// Get decode matrix with only wanted recovery rows
	for (i = 0; i < nerrs; i++) {
		if (frag_err_list[i] < k)	// A src err
			for (j = 0; j < k; j++)
				decode_matrix[k * i + j] =
				    invert_matrix[k * frag_err_list[i] + j];
	}

	// For non-src (parity) erasures need to multiply encode matrix * invert
	for (p = 0; p < nerrs; p++) {
		if (frag_err_list[p] >= k) {	// A parity err
			for (i = 0; i < k; i++) {
				s = 0;
				for (j = 0; j < k; j++)
					s ^= gf_mul(invert_matrix[j * k + i],
						    encode_matrix[k * frag_err_list[p] + j]);
				decode_matrix[k * p + i] = s;
			}
		}
	}
	return 0;
}