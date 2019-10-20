pub mod canvas;
pub mod draw_engine;

/// The different type of cursors.
pub enum Cursor {
    Auto,
    Default,
    None,
    ContextMenu,
    Help,
    Pointer,
    Progress,
    Wait,
    Cell,
    Crosshair,
    Text,
    VerticalText,
    Alia,
    Copy,
    Move,
    NoDrop,
    NotAllowed,
    Grab,
    Grabbing,
    AllScroll,
    ColResize,
    RowResize,
    NorthResize,
    EastResize,
    SouthResize,
    WestResize,
    NorthWestResize,
    NorthEastResize,
    SouthEastResize,
    SouthWestResize,
    EastWestResize,
    NorthSouthResize,
    NorthEastSouthWestResize,
    NorthWestSouthEastReize,
    ZoomIn,
    ZoomOut
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
