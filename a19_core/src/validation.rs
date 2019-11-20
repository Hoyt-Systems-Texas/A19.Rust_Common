pub enum ValidationError {
    Required,
    MaxLength(u32),
    MinLength(u32)
}
