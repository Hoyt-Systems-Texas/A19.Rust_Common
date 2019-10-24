use a19_data_structure::graphic:: {
    Point,
    BoundingBox
};

use crate::canvas:: {
    DrawingCanvas,
};

use crate::draw_engine::event:: {
    KeyboardEvent
};

/// Used to represent a drawing tool.
pub trait DrawTool {

    /// Called when the mouse is moved.
    fn mouse_move<TCanvas: DrawingCanvas>(
        &self,
        top_canvas: &mut TCanvas,
        main_canvas: &mut TCanvas,
        point: Point);

    /// Called when the mouse is released.
    fn mouse_up<TCanvas: DrawingCanvas>(
        &self,
        top_canvas: &mut TCanvas,
        main_canvas: &mut TCanvas,
        point: Point);

    /// Callend when the mouse is pressed down.
    fn mouse_down<TCanvas: DrawingCanvas>(
        &self,
        top_canvas: &mut TCanvas,
        main_canvas: &mut TCanvas,
        point: Point);

    /// Called when a key is pressed down.
    fn key_down<TCanvas: DrawingCanvas>(
        &self,
        top_canvas: &mut TCanvas,
        main_canvas: &mut TCanvas,
        key_event: dyn KeyboardEvent);

    /// Called when the key is up.
    fn key_up<TCanvas: DrawingCanvas>(
        &self,
        top_canvas: &mut TCanvas,
        main_canvas: &mut TCanvas,
        key_event: dyn KeyboardEvent);

}
