use std::io::Error;

pub trait CryptoProcessor {
    fn encode(data: &Vec<u8>) -> Result<Vec<u8>, Error>;
    fn decode(data: &Vec<u8>) -> Result<Vec<u8>, Error>;
}