pub mod event;
pub mod shape;

use crate::draw_engine::event:: {
    KeyboardEvent
};

use crate::canvas:: {
    DrawingCanvas
};

use a19_data_structure::graphic:: {
    Point
};

/// Represents the engine for drawing on the canvas.
pub struct DrawEngine<TCanvasContext: 'static>
    where TCanvasContext: DrawingCanvas {
    top_canvas: TCanvasContext,
    primary_canvas: TCanvasContext,
}

pub fn create_draw_engine<TCanvasContext>(
    top_canvas: TCanvasContext,
    primary_canvas: TCanvasContext
) -> DrawEngine<TCanvasContext> where TCanvasContext:DrawingCanvas {
    DrawEngine {
        top_canvas,
        primary_canvas,
    }
}

impl<TCanvasContext: DrawingCanvas> DrawEngine<TCanvasContext> {

    pub fn mouse_move(&self, point: Point) {
    }

    pub fn mouse_down(&self, point: Point) {

    }

    pub fn mouse_up(&self, point: Point) {

    }

    pub fn key_down<TEvent: KeyboardEvent>(&self, key_event: TEvent) {

    }

    pub fn key_up<TEvent: KeyboardEvent>(&self, key_event: TEvent) {

    }

}