pub struct Point {
    pub x: f64,
    pub y: f64
}

impl Point {

    /// Used to calculate the distance between 2 points.
    pub fn distance(&self, end_point: &Point) -> f64 {
        ((end_point.x - self.x).powi(2) + (end_point.y -self.y).powi(2)).sqrt()
    }

}

pub struct BoundingBox {
    top_left: Point,
    bottom_right: Point
}

impl BoundingBox {

    /// Used to get the width of a bounding box.
    fn width(&self) -> f64 {
        self.bottom_right.x - self.top_left.x
    }

    /// Used to get the height of the bounding box.
    fn height(&self) -> f64 {
        self.bottom_right.y - self.top_left.y
    }

    /// Checks to see if a box is inside of another box.
    fn inside_box(&self, other_box: BoundingBox) -> bool {
        self.top_left.x <= other_box.top_left.x &&
        self.top_left.y <= other_box.top_left.y &&
        self.bottom_right.x >= other_box.bottom_right.x &&
        self.bottom_right.y >= other_box.bottom_right.y
    }

    /// Checks to see if a point is inside of a box.
    fn inside_point(&self, point: Point) -> bool {
        self.top_left.x <= point.x &&
        self.top_left.y <= point.y &&
        self.bottom_right.x >= point.x &&
        self.bottom_right.y >= point.y
    }

}

#[cfg(test)]
mod tests {

    use crate::graphic::Point;

    #[test]
    fn distance_test() {
        let p1 = Point {
            x: 1.0,
            y: 1.0
        };

        let p2 = Point {
            x: 5.0,
            y: 4.0
        };

        let result = p1.distance(&p2);
        assert_eq!(result, 5.0);
    }

}
