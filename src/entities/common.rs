use serde::{Deserialize, Deserializer};
use serde::de::Unexpected;

pub fn date_deserialize<'de, D>(deserializer: D) -> Result<Option<u64>, D::Error>
    where
        D: Deserializer<'de>,
{
    let v: Option<Vec<u64>> = Deserialize::deserialize(deserializer)?;
    if v.is_none() {
        return Ok(None);
    }
    let d = v.unwrap();
    if d.len() != 3 {
        return Err(serde::de::Error::invalid_value(Unexpected::Seq, &"subcategory operation code"));
    }
    return Ok(Some(d[0] * 10000 + d[1] * 100 + d[2]));
}
