use a19_data_structure::graphic:: {
    BoundingBox
};

use crate::draw_engine:: {
    DrawingCanvas
};

pub trait Shape {

    /// Used to draw a shape on a canvas.
    fn draw<TCanvas: DrawingCanvas>(
        &self,
        canvas: &mut TCanvas);

    /// Used to select this shape on the gui.
    fn select<TCanvas: DrawingCanvas>(
        &mut self,
        top_canvas: &mut TCanvas);

    fn deselect<TCanvas: DrawingCanvas>(
        &mut self,
        top_canvas: &mut TCanvas);

    /// Used to get the bound box of the shape.
    fn get_bounding_box(&self) -> BoundingBox;
}