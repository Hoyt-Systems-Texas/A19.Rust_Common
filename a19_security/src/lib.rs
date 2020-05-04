use chrono::Duration;
use rand::{thread_rng, RngCore};
use chrono::Utc;
use byteorder::{ ByteOrder, BigEndian };
use a19_concurrent::buffer::align;
use hmac::{ Hmac, Mac };
use sha2::Sha512;
use thiserror::Error;
use crypto_mac::{ InvalidKeyLength, MacResult };
use generic_array::{ GenericArray };

const SIG_START: usize = 0;
const SIG_END: usize = 64;
const SALT_START: usize = 64;
const SALT_END: usize = 72;
const EXPIRE_START: usize = 72;
const EXPIRE_END: usize = 80;
const MSG_SIZE_START: usize = 80;
const MSG_SIZE_END: usize = 84;
const MSG_START: usize = 84;
const SIGNED_START: usize = SALT_START;
pub const HEADER_SIZE: usize = 84;

#[derive(Debug, Error)]
pub enum SignDataError {
    #[error("Invalid key size error")]
    KeySize(InvalidKeyLength),
    #[error("Message invalid size")]
    InvalidSize,
    #[error("Message expired")]
    MessageExpired,
    #[error("Signatures don't match")]
    InvalidSignature,
}

impl From<InvalidKeyLength> for SignDataError {

    fn from(s: InvalidKeyLength) -> Self {
        SignDataError::KeySize(InvalidKeyLength)
    }
}

/// Used to signed the data.
pub fn sign_data_hmac(
    message: &[u8],
    key: &[u8],
    expires_after: Duration) -> Result<Vec<u8>, SignDataError> {
    let mut thread = thread_rng();
    let salt = thread.next_u64();
    // Align at 64 bytes
    let mut out_buffer = vec![0; align(HEADER_SIZE + message.len(), 64)];
    BigEndian::write_u64(&mut out_buffer[SALT_START..SALT_END], salt);
    let expires = Utc::now().checked_add_signed(expires_after).unwrap().timestamp_millis() as u64;
    BigEndian::write_u64(&mut out_buffer[EXPIRE_START..EXPIRE_END], expires);
    let msg_size = message.len() as u32;
    BigEndian::write_u32(&mut out_buffer[MSG_SIZE_START..MSG_SIZE_END], msg_size);
    // Copy over the buffer
    for (d, s) in  out_buffer[MSG_START..(MSG_START + message.len())]
        .iter_mut()
        .zip(message.iter()) {
            *d=*s
    }
    let mut mac = Hmac::<Sha512>::new_varkey(key)?;
    mac.input(&out_buffer[SIGNED_START..]);
    let result = mac.result();
    for (d, s) in out_buffer[SIG_START..SIG_END]
        .iter_mut()
        .zip(result.code().iter()) {
            *d=*s
        }
    Ok(out_buffer)
}

pub fn verify_data<'a>(
    key: &[u8],
    message: &'a [u8]) ->  Result<&'a [u8], SignDataError> {
    if message.len() > HEADER_SIZE {
        let sig = MacResult::new(GenericArray::clone_from_slice(&message[SIG_START..SIG_END]));
        let mut mac = Hmac::<Sha512>::new_varkey(key)?;
        mac.input(&message[SIGNED_START..]);
        let result = mac.result();
        // Make sure to use the crypto compare function to prevent timing attacks.
        if result == sig {
            let current_time = Utc::now().timestamp_millis() as u64;
            let expires = BigEndian::read_u64(&message[EXPIRE_START..EXPIRE_END]);
            if current_time < expires {
                let length = BigEndian::read_u32(&message[MSG_SIZE_START..MSG_SIZE_END]) as usize;
                Ok(&message[MSG_START..(MSG_START + length)])
            } else {
                Err(SignDataError::MessageExpired)
            }
        } else {
            Err(SignDataError::InvalidSignature)
        }
    } else {
        Err(SignDataError::InvalidSize)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }

    #[test]
    fn sign_data_test() {
        let key: [u8; 64] = [3; 64];
        let data: [u8; 25] = [1; 25];
        let result = sign_data_hmac(&data, &key, Duration::seconds(10)).unwrap();
        assert_eq!(result.len(), 128)
    }

    #[test]
    fn sign_data_verify_test() {
        let key: [u8; 64] = [3; 64];
        let data: [u8; 25] = [1; 25];
        let result = sign_data_hmac(&data, &key, Duration::seconds(10)).unwrap();
        let verify_result = verify_data(&key, &result).unwrap();
    }

    #[test]
    fn sign_data_verify_invalid_keytest() {
        let key: [u8; 64] = [3; 64];
        let data: [u8; 25] = [1; 25];
        let result = sign_data_hmac(&data, &key, Duration::seconds(10)).unwrap();
        let key: [u8; 64] = [2; 64];
        match verify_data(&key, &result) {
            Ok(_) => assert!(false, "Key should have failed"),
            Err(e) => {
                match e {
                    SignDataError::InvalidSignature => {},
                    _ => assert!(false, "Different error returned!")
                }
            }
        }
    }

    #[test]
    fn sign_data_verify_invalid_key_expire_test() {
        let key: [u8; 64] = [3; 64];
        let data: [u8; 25] = [1; 25];
        let mut result = sign_data_hmac(&data, &key, Duration::seconds(0)).unwrap();
        match verify_data(&key, &result) {
            Ok(_) => assert!(false, "Key should have failed"),
            Err(e) => {
                match e {
                    SignDataError::MessageExpired => {},
                    _ => assert!(false, "Different error returned!")
                }
            }
        }
    }
}
