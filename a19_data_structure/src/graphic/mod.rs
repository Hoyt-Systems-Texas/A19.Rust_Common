pub mod matrix_2d;
pub mod shape;

#[derive(Clone)]
#[derive(Debug)]
pub struct Point {
    pub x: f64,
    pub y: f64,
}

#[derive(Debug)]
#[derive(Clone)]
#[derive(PartialEq)]
pub enum LineSegmentOrientation {
    /// The points are on the same line.
    Collinear,
    Clockwise,
    Counterclockwise,
}

pub trait Graphic {

    /// Used to normalize the values between 0 and 1.
    /// # Arguments
    /// `value` - The value to normalize by.
    fn normalize(&mut self, value: &f64);

    /// Translate the shape by the specified amount and updates it.
    /// # Arguments
    /// `x_amount` - The amount to translate by in the x direction.
    /// `y_amount` - The amount to translate by in the y direction.
    fn translate_up(&mut self, x_amount: &f64, y_amount: &f64);

    /// Scales the point by the factor.
    /// # Arguments
    /// `factor` - The factor to scale by.
    fn scale(&mut self, factor: &f64);
}

impl Point {

    /// Used to create a new point.
    /// `x` - The x coordinate.
    /// `y` - The y coordinate.
    pub fn new(x: f64, y: f64) -> Point {
        Point {
            x,
            y
        }
    }
    /// Used to calculate the distance between 2 points.
    /// # Arguments
    /// `end_point` - The ending point to calculate the distance.
    pub fn distance(&self, end_point: &Point) -> f64 {
        ((end_point.x - self.x).powi(2) + (end_point.y - self.y).powi(2)).sqrt()
    }

    /// Used to calculate the centroid of one axis.
    /// @ Arguments
    /// `start` - The starting point to calculate.
    /// `end` - The ending point.
    pub fn centroid(start: &f64, end: &f64) -> f64 {
        start + (end - start)
    }

}

impl Graphic for Point {

    fn normalize(&mut self, value: &f64) {
        self.x = self.x / value;
        self.y = self.x / value;
    }

    fn translate_up(&mut self, x_amount: &f64, y_amount: &f64) {
        self.x = self.x + x_amount;
        self.y = self.y + y_amount;
    }

    fn scale(&mut self, factor: &f64) {
        self.x = self.x * factor;
        self.y = self.y * factor;
    }
}

#[derive(Debug)]
#[derive(Clone)]
pub struct BoundingBox {
    top_left: Point,
    bottom_right: Point,
}

impl BoundingBox {

    pub fn new(top_left: Point, bottom_right: Point) -> BoundingBox {
        BoundingBox {
            top_left,
            bottom_right
        }
    }

    pub fn top_left_edit(&mut self) -> &mut Point {
        &mut self.top_left
    }

    pub fn bottom_right_edit(&mut self) -> &mut Point {
        &mut self.bottom_right
    }

    pub fn top_left(&self) -> &Point {
        &self.top_left
    }

    pub fn bottom_right(&self) -> &Point {
        &self.bottom_right
    }

    /// Used to get the width of a bounding box.
    pub fn width(&self) -> f64 {
        self.bottom_right.x - self.top_left.x
    }

    /// Used to get the height of the bounding box.
    pub fn height(&self) -> f64 {
        self.bottom_right.y - self.top_left.y
    }

    /// Used to find the center of a bounding box.
    pub fn center(&self) -> Point {
        Point::new(
            Point::centroid(&self.bottom_right.x, &self.top_left.x),
            Point::centroid(&self.bottom_right.y, &self.top_left.y))
    }

    /// Checks to see if a box is inside of another box.
    /// # Arguments
    /// `other_box` - The other box to check.
    pub fn inside_box(&self, other_box: &BoundingBox) -> bool {
        self.top_left.x <= other_box.top_left.x &&
            self.top_left.y <= other_box.top_left.y &&
            self.bottom_right.x >= other_box.bottom_right.x &&
            self.bottom_right.y >= other_box.bottom_right.y
    }

    /// Checks to see if a point is inside of a box.
    /// # Arguments
    /// `point` - The point to check and see if the box is inside.
    pub fn inside_point(&self, point: &Point) -> bool {
        self.top_left.x <= point.x &&
            self.top_left.y <= point.y &&
            self.bottom_right.x >= point.x &&
            self.bottom_right.y >= point.y
    }

    /// Checks to see if 2 bounding boxes overlap each other.
    /// https://gamedev.stackexchange.com/questions/586/what-is-the-fastest-way-to-work-out-2d-bounding-box-intersection
    /// # Arguments 
    /// `other_box` - The other box to check.
    pub fn is_intersected(&self, other_box: &BoundingBox) -> bool {
        !(other_box.top_left.x > self.bottom_right.x
            || other_box.bottom_right.x < self.top_left.x
            || other_box.top_left.y > self.bottom_right.y
            || other_box.bottom_right.y < self.top_left.y)
    }

    /// Checks to see if the bounding boxes are touching.
    /// # Arguments
    /// `other_box` - The other box we are seeing if they are touching.
    pub fn is_touching(&self, other_box: &BoundingBox) -> bool {
        self.inside_box(other_box)
            || other_box.inside_box(self)
            || self.is_intersected(other_box)
    }
}

#[cfg(test)]
mod tests {
    use crate::graphic::Point;

    #[test]
    fn distance_test() {
        let p1 = Point {
            x: 1.0,
            y: 1.0,
        };

        let p2 = Point {
            x: 5.0,
            y: 4.0,
        };

        let result = p1.distance(&p2);
        assert_eq!(result, 5.0);
    }
}
