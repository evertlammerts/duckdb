//===----------------------------------------------------------------------===//
//                         DuckDB
//
// json_reader_options.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "json_common.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/enums/file_compression_type.hpp"
#include "json_enums.hpp"
#include "duckdb/common/types/type_map.hpp"
#include "duckdb/function/scalar/strftime_format.hpp"

namespace duckdb {

struct DateFormatMap {
	friend class MutableDateFormatMap;

public:
	DateFormatMap(type_id_map_t<vector<StrpTimeFormat>> candidate_formats_p, type_id_set_t cast_candidates_p)
	    : candidate_formats(std::move(candidate_formats_p)), cast_candidates(std::move(cast_candidates_p)) {
	}

	bool HasFormats(LogicalTypeId type) const {
		return HasFormats(candidate_formats, type);
	}

	const vector<StrpTimeFormat> &GetFormats(LogicalTypeId type) const {
		D_ASSERT(candidate_formats.find(type) != candidate_formats.end());
		return candidate_formats.find(type)->second;
	}

	//! Whether the plain cast is a candidate for type too
	bool CastIsCandidate(LogicalTypeId type) const {
		return cast_candidates.find(type) != cast_candidates.end();
	}

	//! The format at format_index, or nullptr for the plain cast when format_index is invalid
	optional_ptr<const StrpTimeFormat> GetCandidate(LogicalTypeId type, optional_idx format_index) const {
		if (!format_index.IsValid()) {
			return nullptr;
		}
		return &GetFormats(type)[format_index.GetIndex()];
	}

	//! Whether the candidate of type, a format or the plain cast when format is nullptr, reads input
	bool CandidateReads(optional_ptr<const StrpTimeFormat> format, const string_t &input, date_t &result,
	                    optional_ptr<string> error_message) const;
	bool CandidateReads(optional_ptr<const StrpTimeFormat> format, const string_t &input, timestamp_t &result,
	                    optional_ptr<string> error_message) const;

private:
	bool HasTwoDigitYear(LogicalTypeId type, const string_t &input) const;
	bool FourDigitYearReads(LogicalTypeId type, optional_ptr<const StrpTimeFormat> format, const string_t &input,
	                        optional_ptr<string> error_message) const;

public:
	static void AddFormat(type_id_map_t<vector<StrpTimeFormat>> &candidate_formats, LogicalTypeId type,
	                      const string &format_string) {
		auto &formats = candidate_formats[type];
		formats.emplace_back();
		formats.back().format_specifier = format_string;
		const auto error = StrpTimeFormat::ParseFormatSpecifier(formats.back().format_specifier, formats.back());
		if (!error.empty()) {
			formats.pop_back();
			throw InvalidInputException(error);
		}
	}

	static bool HasFormats(const type_id_map_t<vector<StrpTimeFormat>> &candidate_formats, LogicalTypeId type) {
		return candidate_formats.find(type) != candidate_formats.end();
	}

private:
	type_id_map_t<vector<StrpTimeFormat>> candidate_formats;
	type_id_set_t cast_candidates;
};

class MutableDateFormatMap {
public:
	explicit MutableDateFormatMap(DateFormatMap &date_format_map) : date_format_map(date_format_map) {
	}

	bool HasFormats(LogicalTypeId type) {
		lock_guard<mutex> lock(format_lock);
		return date_format_map.HasFormats(type);
	}

	idx_t NumberOfFormats(LogicalTypeId type) {
		lock_guard<mutex> lock(format_lock);
		return date_format_map.candidate_formats.at(type).size();
	}

	bool CastIsCandidate(LogicalTypeId type) {
		lock_guard<mutex> lock(format_lock);
		return date_format_map.CastIsCandidate(type);
	}

	//! The formats do not change once InitializeFormats built them, so matching values needs no lock
	const DateFormatMap &GetDateFormatMap() const {
		return date_format_map;
	}

private:
	mutex format_lock;
	DateFormatMap &date_format_map;
};

struct JSONReaderOptions {
	//! Scan type
	JSONScanType type = JSONScanType::READ_JSON;
	//! The format of the JSON
	JSONFormat format = JSONFormat::AUTO_DETECT;
	//! Whether record types in the JSON
	JSONRecordType record_type = JSONRecordType::AUTO_DETECT;
	//! Whether file is compressed or not, and if so which compression type
	FileCompressionType compression = FileCompressionType::AUTO_DETECT;
	//! Whether or not we should ignore malformed JSON (default to NULL)
	bool ignore_errors = false;
	//! Maximum JSON object size (defaults to 16MB minimum)
	idx_t maximum_object_size = 16777216;
	//! Whether we auto-detect a schema
	bool auto_detect = false;
	//! Whether "format" was explicitly specified
	bool format_specified = false;
	//! Whether "auto_detect" was explicitly specified
	bool auto_detect_specified = false;
	//! Sample size for detecting schema
	idx_t sample_size = idx_t(STANDARD_VECTOR_SIZE) * 10;
	//! Max depth we go to detect nested JSON schema (defaults to unlimited)
	idx_t max_depth = NumericLimits<idx_t>::Maximum();
	//! We divide the number of appearances of each JSON field by the auto-detection sample size
	//! If the average over the fields of an object is less than this threshold,
	//! we default to the MAP type with value type of merged field types
	double field_appearance_threshold = 0.1;
	//! The maximum number of files we sample to sample sample_size rows
	idx_t maximum_sample_files = 32;
	//! Whether we auto-detect and convert JSON strings to integers
	bool convert_strings_to_integers = false;
	//! Whether object keys that differ only in ASCII case are the same key, when detecting the schema and when
	//! matching keys to the columns
	bool case_insensitive_keys = false;
	//! Whether GeoJSON geometry fragments are inferred as the GEOMETRY type, and Features are unnested into
	//! geometry + property columns. Unset means "decide from the file name" (.geojson / .geojsonl)
	optional<bool> geojson;
	//! If a struct contains more fields than this threshold with at least 80% similar types,
	//! we infer it as MAP type
	idx_t map_inference_threshold = 200;
	//! User-provided list of names (in order)
	vector<Identifier> name_list;
	//! User-provided list of types (in order)
	vector<LogicalType> sql_type_list;
	//! Forced date/timestamp formats
	string date_format;
	string timestamp_format;
};

} // namespace duckdb
