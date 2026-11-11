#include "catch.hpp"
#include "duckdb/common/typedefs.hpp"
#include "utf8proc_wrapper.hpp"

#include <string>
#include <utility>
#include <vector>

using namespace duckdb;

static std::string FromHex(const std::string &hex) {
	std::string result;
	for (idx_t i = 0; i + 1 < hex.size(); i += 2) {
		result += static_cast<char>(std::stoi(hex.substr(i, 2), nullptr, 16));
	}
	return result;
}

// Extensions call MakeValid expecting the length to stay; it marks every byte that decode(b, 'replace') replaces.
TEST_CASE("MakeValid marks each byte of each maximal invalid subpart in place", "[utf8]") {
	// input and expected output in hex, from Python's UTF-8 decoder with one '?' per byte of each invalid subpart
	std::vector<std::pair<std::string, std::string>> cases = {
	    {"41C342", "413F42"},
	    {"F09F98F09F9880", "3F3F3FF09F9880"},
	    {"C342", "3F42"},
	    {"C3C3A9", "3FC3A9"},
	    {"41C3", "413F"},
	    {"E242", "3F42"},
	    {"E28242", "3F3F42"},
	    {"E282C3A9", "3F3FC3A9"},
	    {"41E282", "413F3F"},
	    {"F09F9842", "3F3F3F42"},
	    {"F0F09F9880", "3FF09F9880"},
	    {"F09FE282AC", "3F3FE282AC"},
	    {"41F09F98", "413F3F3F"},
	    {"418042", "413F42"},
	    {"41FF42", "413F42"},
	    {"C08041", "3F3F41"},
	    {"E0808041", "3F3F3F41"},
	    {"EDA08041", "3F3F3F41"},
	    {"F490808041", "3F3F3F3F41"},
	    {"41E2FF42C08043", "413F3F423F3F43"},
	    {"4141414141414141414141414141414141414141E2824242424242424242424242424242424242424242",
	     "41414141414141414141414141414141414141413F3F4242424242424242424242424242424242424242"},
	    {"", ""},
	    {"41C3A942", "41C3A942"},
	    {"3FC342", "3F3F42"},
	    {"E282F09F9841", "3F3F3F3F3F41"},
	    {"C3E282F09F98", "3F3F3F3F3F3F"},
	    {"E282E282", "3F3F3F3F"},
	    {"41F09F", "413F3F"},
	    {"E0A041", "3F3F41"},
	    {"E08041", "3F3F41"},
	    {"ED9F41", "3F3F41"},
	    {"EDA041", "3F3F41"},
	    {"F0908041", "3F3F3F41"},
	    {"F08F41", "3F3F41"},
	    {"F48FBF41", "3F3F3F41"},
	    {"F49041", "3F3F41"},
	    {"F5808080", "3F3F3F3F"},
	    {"C1BF", "3F3F"},
	    {"F09F98F09F98F09F98F09F98F09F98", "3F3F3F3F3F3F3F3F3F3F3F3F3F3F3F"},
	    {"4141414141414141414141E282", "41414141414141414141413F3F"},
	};
	for (auto &test_case : cases) {
		auto input = FromHex(test_case.first);
		auto expected = FromHex(test_case.second);
		auto value = input;
		Utf8Proc::MakeValid(&value[0], value.size());
		INFO("input " << test_case.first);
		REQUIRE(value == expected);
		REQUIRE(Utf8Proc::IsValid(value.c_str(), value.size()));
	}
	auto value = FromHex("41E28242");
	Utf8Proc::MakeValid(&value[0], value.size(), '.');
	REQUIRE(value == "A..B");
}
