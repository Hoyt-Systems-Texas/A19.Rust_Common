use a19_data_structure::graphic:: {
    BoundingBox,
    Point
};

use crate::canvas:: {
    DrawingCanvas
};

// Represents anything that is a shape that can be drawn on the screen.
pub trait Shape {

    /// Used to draw a shape on a canvas.
    /// # Arguments
    /// * `canvas` - The canvas to draw on.
    fn draw<TCanvas: DrawingCanvas>(
        &self,
        canvas: &mut TCanvas);

    /// Used to select this shape on the gui.
    /// # Arguments
    /// * `canvas` - The top canvas used to draw the selection.
    fn select<TCanvas: DrawingCanvas>(
        &mut self,
        top_canvas: &mut TCanvas);

    /// Used to deslect the component.
    /// # Arguments
    /// * `canvas` - The top canvas for a deslection.
    fn deselect<TCanvas: DrawingCanvas>(
        &mut self,
        top_canvas: &mut TCanvas);

    /// Used to get the bound box of the shape.  This is just the boundaries.
    fn get_bounding_box(&self) -> BoundingBox;

    /// Used to move a shape.
    /// # Arguments
    /// * `top_canvas` - The top canvas to drag the shape on.
    /// * `amount` - The amount to move the shape.
    fn drag_shape<TCanvas: DrawingCanvas>(&self, top_canvas: TCanvas, amount: &Point);

    /// Used to get the current position for drawing the shape.
    fn get_draw_position(&self) -> Point;

    /// Used to set the drawing position.
    /// # Arguments
    /// * `point` - The point at which to draw the component.
    fn set_draw_position(&self, point: &Point);

    ///  Checks to see if a point is inside.
    /// # Arguments
    /// * `point` - The point to check ot see if it's inside.
    fn is_inside(&self, point: &Point) -> bool;
}
