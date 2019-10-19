use a19_data_structure::graphic:: {
    Point
};

pub trait DrawingCanvas {
    
    /// Moves the current drawing location.
    fn move_to(&mut self, point: &Point);

    /// TUsed to draw a like to.
    fn line_to(&mut self, point: &Point);

    /// Used to draw an arc.
    fn arc(&mut self, point: &Point, radius: &f64, start_angle: &f64, end_angle: &f64);

    /// Used to close a path.
    fn close_path(&mut self);

    /// Fills in a shape.
    fn fill(&mut self);

    /// Strokes a shape.
    fn stroke(&mut self);

    /// Used to fill in a style.
    fn file_style(&mut self, value: &str);

    /// Draws text on the screen at a position.
    fn draw_text(&mut self, value: &str, point: &Point);

    /// Used to mesaure the text.
    fn measure_text(&mut self, value: &str) -> Option<f64>;

    /// Used to set the current font for the canvas
    fn font(&mut self, font_name: &str);

    /// Used to stroke text.
    fn stroke_text(&mut self, value: &str, start: &Point);

    /// Sets the color to use when making a stroke, fill etc.
    fn set_fill_color(&mut self, color: &str);
}
