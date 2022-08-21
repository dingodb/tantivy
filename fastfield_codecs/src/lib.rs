#[cfg(test)]
#[macro_use]
extern crate more_asserts;

use std::io;

use ownedbytes::OwnedBytes;

pub mod bitpacked;
pub mod dynamic;
pub mod gcd;
pub mod linearinterpol;
pub mod multilinearinterpol;

// Unify with FastFieldReader

pub trait FastFieldCodecReader {
    /// reads the metadata and returns the CodecReader
    fn get_u64(&self, doc: u64) -> u64;
    fn min_value(&self) -> u64;
    fn max_value(&self) -> u64;
}

/// The FastFieldSerializerEstimate trait is required on all variants
/// of fast field compressions, to decide which one to choose.
pub trait FastFieldCodec {
    /// A codex needs to provide a unique name used for debugging.
    const NAME: &'static str;

    type Reader: FastFieldCodecReader;

    /// Check if the Codec is able to compress the data
    fn is_applicable(vals: &[u64], stats: FastFieldStats) -> bool;

    /// Returns an estimate of the compression ratio.
    /// The baseline is uncompressed 64bit data.
    ///
    /// It could make sense to also return a value representing
    /// computational complexity.
    fn estimate(vals: &[u64], stats: FastFieldStats) -> f32;

    /// Serializes the data using the serializer into write.
    /// There are multiple iterators, in case the codec needs to read the data multiple times.
    /// The iterators should be preferred over using fastfield_accessor for performance reasons.
    fn serialize(
        &self,
        write: &mut impl io::Write,
        vals: &[u64],
        stats: FastFieldStats,
    ) -> io::Result<()>;

    fn open_from_bytes(bytes: OwnedBytes) -> io::Result<Self::Reader>;
}

/// Statistics are used in codec detection and stored in the fast field footer.
#[derive(Clone, Copy, Default, Debug)]
pub struct FastFieldStats {
    pub min_value: u64,
    pub max_value: u64,
    pub num_vals: u64,
}

impl FastFieldStats {
    pub fn compute(vals: &[u64]) -> Self {
        if vals.is_empty() {
            return FastFieldStats::default();
        }
        let first_val = vals[0];
        let mut fast_field_stats = FastFieldStats {
            min_value: first_val,
            max_value: first_val,
            num_vals: 1,
        };
        for &val in &vals[1..] {
            fast_field_stats.record(val);
        }
        fast_field_stats
    }

    pub fn record(&mut self, val: u64) {
        self.num_vals += 1;
        self.min_value = self.min_value.min(val);
        self.max_value = self.max_value.max(val);
    }
}

#[cfg(test)]
mod tests {
    use crate::bitpacked::BitpackedFastFieldCodec;
    use crate::linearinterpol::LinearInterpolCodec;
    use crate::multilinearinterpol::MultiLinearInterpolFastFieldCodec;

    pub fn create_and_validate<S: FastFieldCodec>(
        codec: &S,
        data: &[u64],
        name: &str,
    ) -> (f32, f32) {
        if !S::is_applicable(&data, crate::tests::stats_from_vec(data)) {
            return (f32::MAX, 0.0);
        }
        let estimation = S::estimate(&data, crate::tests::stats_from_vec(data));
        let mut out: Vec<u8> = Vec::new();
        codec
            .serialize(&mut out, &data, crate::tests::stats_from_vec(data))
            .unwrap();

        let actual_compression = out.len() as f32 / (data.len() as f32 * 8.0);

        let reader = S::open_from_bytes(OwnedBytes::new(out)).unwrap();
        for (doc, orig_val) in data.iter().enumerate() {
            let val = reader.get_u64(doc as u64);
            if val != *orig_val {
                panic!(
                    "val {:?} does not match orig_val {:?}, in data set {}, data {:?}",
                    val, orig_val, name, data
                );
            }
        }
        (estimation, actual_compression)
    }
    pub fn get_codec_test_data_sets() -> Vec<(Vec<u64>, &'static str)> {
        let mut data_and_names = vec![];

        let data = (10..=20_u64).collect::<Vec<_>>();
        data_and_names.push((data, "simple monotonically increasing"));

        data_and_names.push((
            vec![5, 6, 7, 8, 9, 10, 99, 100],
            "offset in linear interpol",
        ));
        data_and_names.push((vec![5, 50, 3, 13, 1, 1000, 35], "rand small"));
        data_and_names.push((vec![10], "single value"));

        data_and_names
    }

    fn test_codec<C: FastFieldCodec>(codec: &C) {
        let codec_name = C::NAME;
        for (data, data_set_name) in get_codec_test_data_sets() {
            let (estimate, actual) = crate::tests::create_and_validate(codec, &data, data_set_name);
            let result = if estimate == f32::MAX {
                "Disabled".to_string()
            } else {
                format!("Estimate {:?} Actual {:?} ", estimate, actual)
            };
            println!(
                "Codec {}, DataSet {}, {}",
                codec_name, data_set_name, result
            );
        }
    }
    #[test]
    fn test_codec_bitpacking() {
        test_codec(&BitpackedFastFieldCodec);
    }
    #[test]
    fn test_codec_interpolation() {
        test_codec(&LinearInterpolCodec);
    }
    #[test]
    fn test_codec_multi_interpolation() {
        test_codec(&MultiLinearInterpolFastFieldCodec);
    }

    use super::*;
    pub fn stats_from_vec(data: &[u64]) -> FastFieldStats {
        let min_value = data.iter().cloned().min().unwrap_or(0);
        let max_value = data.iter().cloned().max().unwrap_or(0);
        FastFieldStats {
            min_value,
            max_value,
            num_vals: data.len() as u64,
        }
    }

    #[test]
    fn estimation_good_interpolation_case() {
        let data = (10..=20000_u64).collect::<Vec<_>>();

        let linear_interpol_estimation =
            LinearInterpolCodec::estimate(&data, stats_from_vec(&data));
        assert_le!(linear_interpol_estimation, 0.01);

        let multi_linear_interpol_estimation =
            MultiLinearInterpolFastFieldCodec::estimate(&&data[..], stats_from_vec(&data));
        assert_le!(multi_linear_interpol_estimation, 0.2);
        assert_le!(linear_interpol_estimation, multi_linear_interpol_estimation);

        let bitpacked_estimation = BitpackedFastFieldCodec::estimate(&data, stats_from_vec(&data));
        assert_le!(linear_interpol_estimation, bitpacked_estimation);
    }
    #[test]
    fn estimation_test_bad_interpolation_case() {
        let data = vec![200, 10, 10, 10, 10, 1000, 20];

        let linear_interpol_estimation =
            LinearInterpolCodec::estimate(&data, stats_from_vec(&data));
        assert_le!(linear_interpol_estimation, 0.32);

        let bitpacked_estimation = BitpackedFastFieldCodec::estimate(&data, stats_from_vec(&data));
        assert_le!(bitpacked_estimation, linear_interpol_estimation);
    }
    #[test]
    fn estimation_test_bad_interpolation_case_monotonically_increasing() {
        let mut data = (200..=20000_u64).collect::<Vec<_>>();
        data.push(1_000_000);

        // in this case the linear interpolation can't in fact not be worse than bitpacking,
        // but the estimator adds some threshold, which leads to estimated worse behavior
        let linear_interpol_estimation =
            LinearInterpolCodec::estimate(&data, stats_from_vec(&data));
        assert_le!(linear_interpol_estimation, 0.35);

        let bitpacked_estimation = BitpackedFastFieldCodec::estimate(&data, stats_from_vec(&data));
        assert_le!(bitpacked_estimation, 0.32);
        assert_le!(bitpacked_estimation, linear_interpol_estimation);
    }
}
