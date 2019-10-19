pub enum KeyCode {
    Alt,
    Control,
    Meta,
    Shift,
    Tab,
    Space,
    Enter,
    ArrowDown,
    ArrowLeft,
    ArrowUp,
    ArrowRight,
    End,
    Home,
    PageDown,
    PageUp,
    Backspace,
    Clear,
    Copy,
    Cut,
    Delete,
    Insert,
    Paste,
    Redo,
    Undo,
    Other(String)
}

pub trait KeyboardEvent {
    fn shift(&self) -> bool;
    fn key(&self) -> &KeyCode;
    fn ctrl_key(&self) -> bool;
    fn meta_key(&self) -> bool;
}