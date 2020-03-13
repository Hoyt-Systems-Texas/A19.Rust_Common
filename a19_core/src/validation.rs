/// Potential validation errors that we might have.
#[derive(Debug)]
pub enum ValidationError {
    /// The value is required.
    Required,
    /// Expected an email address.
    Email,
    /// Exceeds the maximum length.
    MaxLength(usize),
    /// Doesn't meet the minimum length.
    MinLength(usize),
    /// Doesn't meet the minimum value.
    MinValueU32(u32),
    /// Doesn't meet the maximum value.
    MaxValueU32(u32),
    MinValueI32(i32),
    MaxValueI32(i32),
    MinValueUsize(usize),
    MaxValueUsize(usize),
    MaxValueU64(u64),
    MinValueU64(u64),
}
