use crate::graphic:: {
    Point,
    Graphic,
    BoundingBox,
    LineSegmentOrientation
};

use std::slice::Iter;

#[derive(Clone)]
#[derive(Debug)]
pub struct Line {
    start: Point,
    end: Point,
}

impl Line {

    /// Used to create a new line.
    /// # Arguments
    /// `start` - The starting point of the line.
    /// `end` - The ending point of the line.
    fn new(start: Point, end: Point) -> Line {
        Line {
            start,
            end,
        }
    }

    /// Checks to see if a point lies on a line.  Note that that the points already need to be
    /// check to see if they are collinear.
    /// # Arguments
    /// `start_point` - The starting point of the first line.
    /// `end_point` - The ending point of the first line.
    /// `point` - The point to check and see if it falls on the line.
    pub fn on_segment(
        start_point: &Point,
        end_point: &Point,
        point: &Point) -> bool {
        point.x <= start_point.x.max(end_point.x) && point.x >= start_point.x.min(end_point.x) &&
        point.y <= start_point.y.max(end_point.y) && point.y >= start_point.y.min(end_point.y)
    }

    /// Checks the orientation on a line.
    pub fn orientation(
        start_point: &Point,
        end_point: &Point,
        point: &Point) -> LineSegmentOrientation {
        let val =
            (point.y - start_point.y) * (end_point.x - point.x) -
            (point.x - start_point.x) * (end_point.y - point.y);
        if val == 0.0 {
            LineSegmentOrientation::Collinear
        } else if val > 0.0 {
            LineSegmentOrientation::Clockwise
        } else {
            LineSegmentOrientation::Counterclockwise
        }
    }

    /// Checks to see if the orientation.
    /// https://www.geeksforgeeks.org/how-to-check-if-a-given-point-lies-inside-a-polygon/
    /// # Arguments
    /// `p1` - The starting point of the first line.
    /// `q1` - The ending point of the first line.
    /// `p2` - The starting point of the second line.
    /// `q2` - The ending point of the second line.
    pub fn insertection(p1: &Point, q1: &Point, p2: &Point, q2: &Point) -> bool {
        let o1 = Line::orientation(p1, q1, p2);
        let o2 = Line::orientation(p1, q1, q2);
        let o3 = Line::orientation(p2, q2, p1);
        let o4 = Line::orientation(p2, q2, q1);
        if o1 != o2 && o3 != o4 {
            true
        } else {
            match o1 {
                LineSegmentOrientation::Collinear => {
                    Line::on_segment(p1, p2, q1)
                },
                _ => {
                    match o2 {
                        LineSegmentOrientation::Collinear => {
                            Line::on_segment(p1, q2, q1)
                        },
                        _ => {
                            match o3 {
                                LineSegmentOrientation::Collinear => {
                                    Line::on_segment(p2, p1, q2)
                                },
                                _ => {
                                    match o4 {
                                        LineSegmentOrientation::Collinear => {
                                            Line::on_segment(p2, q1, q2)
                                        }
                                        _ => {
                                            false
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

pub struct Polygon {

    points: Vec<Point>,
    bounding_box: BoundingBox
}

pub trait Shape: Graphic {

    /// Used to get the bouding box.
    fn get_bounding_box(&self) -> &BoundingBox;

    /// Checks to see if a bounding boxes are touching.
    /// # Arguments
    /// `other_box` - The other box to check to see if they are touching.
    fn is_touching_box(&self, other_box: &BoundingBox) -> bool;

    /// Checks to see if a point is inside the shape.
    /// # Arguments
    /// `point` - The point to check and see if it's inside.
    fn is_inside(&self, point: &Point) -> bool;
}

impl Polygon {

    pub fn new(initial_size: usize) -> Polygon {
        Polygon {
            points: Vec::with_capacity(initial_size),
            bounding_box: BoundingBox::new(
                Point::new(0.0, 0.0),
                Point::new(0.0, 0.0))
        }
    }

    /// Used to add a point to the polygon.
    pub fn add(&mut self, point: Point) {
        self.points.push(point);
    }

    pub fn iter(&self) -> Iter<Point> {
        self.points.iter()
    }

    pub fn is_valid(&self) -> bool {
        self.points.len() >= 3
    }

    pub fn num_of_points(&self) -> usize {
        self.points.len()
    }

    fn change(&mut self) {
        self.update_bounding_box();
    }

    /// Used to update the bounding box to the correct size.
    fn update_bounding_box(&mut self) {
        for point in self.points.iter() {
            let mut top_left = self.bounding_box.top_left_edit();
            top_left.x = top_left.x.min(point.x);
            top_left.y = top_left.y.min(point.y);
        }
        for point in self.points.iter() {
            let mut bottom_right = self.bounding_box.bottom_right_edit();
            bottom_right.x = bottom_right.x.max(point.x);
            bottom_right.y = bottom_right.y.max(point.y);
        }
    }
}

impl Graphic for Polygon {

    fn normalize(&mut self, value: &f64) {
        for point in self.points.iter_mut() {
            point.normalize(value);
        }
    }

    fn translate_up(&mut self, x_amount: &f64, y_amount: &f64) {
        for point in self.points.iter_mut() {
            point.translate_up(x_amount, y_amount);
        }
        self.change();
    }

    fn scale(&mut self, factor: &f64) {
        for point in self.points.iter_mut() {
            point.scale(factor);
        }
    }
}

impl Shape for Polygon {

    fn get_bounding_box(&self) -> &BoundingBox {
        &self.bounding_box
    }

    fn is_touching_box(&self, other_box: &BoundingBox) -> bool {
        self.bounding_box.is_touching(other_box)
    }

    fn is_inside(&self, point: &Point) -> bool {
        if !self.is_valid() {
            false
        } else {
            let extreme = Point::new(std::f64::MAX, point.y);
            let mut count = 0;
            let mut i = 0;
            loop {
                let next = (i+1) % self.points.len();

                let line_start = &self.points[i];
                let line_end = &self.points[next];
                if Line::insertection(line_start, line_end, &point, &extreme) {
                    match Line::orientation(line_start, point, line_end) {
                        LineSegmentOrientation::Collinear => {
                            if Line::on_segment(line_start, line_end, point) {
                                count = 1;
                                break;
                            }
                        }
                        _ => {
                            count = count + 1;
                        }
                    }
                }
                i = next;
                if i == 0 {
                    break;
                }
            }
            count % 2 == 1
        }
            
    }
}

pub struct Circle {
    center: Point,
    radius: f64,
    bouding_box: BoundingBox
}

impl Circle {

    fn new(center: Point, radius: f64) -> Circle {
        let mut c = Circle{
            center,
            radius,
            bouding_box: BoundingBox {
                top_left: Point::new(0.0, 0.0),
                bottom_right: Point::new(0.0, 0.0)
            }
        };
        c.update();
        c
    }

    fn update(&mut self) {
        // Do a zero copy.
        self.bouding_box.top_left.x = self.center.x - self.radius;
        self.bouding_box.top_left.y = self.center.y - self.radius;
        self.bouding_box.bottom_right.x = self.center.x + self.radius;
        self.bouding_box.bottom_right.y = self.center.y + self.radius;
    }
}

impl Shape for Circle {
    /// Used to get the bouding box.
    fn get_bounding_box(&self) -> &BoundingBox {
        &self.bouding_box
    }

    /// Checks to see if a bounding boxes are touching.
    /// # Arguments
    /// `other_box` - The other box to check to see if they are touching.
    fn is_touching_box(&self, other_box: &BoundingBox) -> bool {
        self.bouding_box.is_touching(other_box)
    }

    fn is_inside(&self, point: &Point) -> bool {
        let r2 = self.radius.powi(2);
        (point.x - self.center.x).powi(2) <= r2
            && (point.y - self.center.y).powi(2) <= r2
    }
}

impl Graphic for Circle {
    /// Used to normalize the values between 0 and 1.
    /// # Arguments
    /// `value` - The value to normalize by.
    fn normalize(&mut self, value: &f64) {
        self.radius = self.radius / value;
        self.center.normalize(value);
        self.update();
    }

    /// Translate the shape by the specified amount and updates it.
    /// # Arguments
    /// `x_amount` - The amount to translate by in the x direction.
    /// `y_amount` - The amount to translate by in the y direction.
    fn translate_up(&mut self, x_amount: &f64, y_amount: &f64) {
        self.center.translate_up(x_amount, y_amount);
        self.update();
    }

    /// Scales the point by the factor.
    /// # Arguments
    /// `factor` - The factor to scale by.
    fn scale(&mut self, factor: &f64) {
        self.radius = self.radius * factor;
        self.update();
    }
}

pub struct Rectangle {
    top_left: Point,
    width: f64,
    height: f64,
    bounding_box: BoundingBox
}

impl Rectangle {
    fn new(
        top_left: Point,
        width: f64,
        height: f64) -> Rectangle {
        let bottom_right = Point {
            x: top_left.x + width,
            y: top_left.y + height
        };
        let top_left_c = top_left.clone();

        Rectangle {
            top_left,
            width,
            height,
            bounding_box: BoundingBox {
                top_left: top_left_c,
                bottom_right
            }
        }
    }

    fn update(&mut self) {
        self.bounding_box.top_left.x = self.top_left.x;
        self.bounding_box.top_left.y = self.top_left.y;
        self.bounding_box.bottom_right.x = self.top_left.x + self.width;
        self.bounding_box.bottom_right.y = self.top_left.y + self.height;
    }
}

impl Rectangle {

    fn top_left(&self) -> &Point {
        &self.top_left
    }
}

impl Shape for Rectangle {

    /// Used to get the bouding box.
    fn get_bounding_box(&self) -> &BoundingBox {
        &self.bounding_box
    }

    /// Checks to see if a bounding boxes are touching.
    /// # Arguments
    /// `other_box` - The other box to check to see if they are touching.
    fn is_touching_box(&self, other_box: &BoundingBox) -> bool {
        self.bounding_box.is_touching(other_box)
    }

    /// Checks to see if a point is inside the shape.
    /// # Arguments
    /// `point` - The point to check and see if it's inside.
    fn is_inside(&self, point: &Point) -> bool {
        self.top_left.x <= point.x &&
            self.bounding_box.bottom_right.x >= point.x &&
            self.top_left.y <= point.y &&
            self.bounding_box.bottom_right.y >= point.y
    }
}

impl Graphic for Rectangle {

    /// Used to normalize the values between 0 and 1.
    /// # Arguments
    /// `value` - The value to normalize by.
    fn normalize(&mut self, value: &f64) {
        self.top_left.normalize(value);
        self.width = self.width / value;
        self.height = self.height / value;
        self.update();
    }

    /// Translate the shape by the specified amount and updates it.
    /// # Arguments
    /// `x_amount` - The amount to translate by in the x direction.
    /// `y_amount` - The amount to translate by in the y direction.
    fn translate_up(&mut self, x_amount: &f64, y_amount: &f64) {
        self.top_left.translate_up(x_amount, y_amount);
        self.update();
    }

    /// Scales the point by the factor.
    /// # Arguments
    /// `factor` - The factor to scale by.
    fn scale(&mut self, factor: &f64) {
        self.top_left.scale(factor);
        self.height = self.height * factor;
        self.width = self.width * factor;
        self.update();
    }
}


#[cfg(test)]
pub mod tests {
    use crate::graphic::shape:: {
        Line,
        Polygon,
        Shape,
        Circle,
        Rectangle
    };
    use crate::graphic:: {
        Point,
        LineSegmentOrientation
    };

    #[test]
    pub fn orientation_collinear_x() {
        let p1 = Point::new(10.0, 10.0);
        let q1 = Point::new(10.0, 20.0);

        let p2 = Point::new(10.0, 15.0);
        let p3 = Point::new(10.0, 25.0);
        
        let or1 = Line::orientation(&p1, &q1, &p2);
        let or2 = Line::orientation(&p1, &q1, &p3);

        assert_eq!(or1, LineSegmentOrientation::Collinear);
        assert_eq!(or2, LineSegmentOrientation::Collinear);
    }

    #[test]
    pub fn orientation_collinear_y() {
        let p1 = Point::new(10.0, 20.0);
        let q1 = Point::new(20.0, 20.0);

        let p2 = Point::new(15.0, 20.0);
        let p3 = Point::new(25.0, 20.0);
        
        let or1 = Line::orientation(&p1, &q1, &p2);
        let or2 = Line::orientation(&p1, &q1, &p3);

        assert_eq!(or1, LineSegmentOrientation::Collinear);
        assert_eq!(or2, LineSegmentOrientation::Collinear);
    }

    #[test]
    pub fn orientation_clockwise() {
        let p1 = Point::new(10.0, 10.0);
        let q1 = Point::new(10.0, 20.0);

        let p2 = Point::new(9.0, 9.0);

        let or1 = Line::orientation(&p1, &q1, &p2);
        assert_eq!(or1, LineSegmentOrientation::Clockwise);
    }

    #[test]
    pub fn orientation_counterclockwise() {
        let p1 = Point::new(10.0, 10.0);
        let q1 = Point::new(10.0, 20.0);

        let p2 = Point::new(12.0, 15.0);

        let or1 = Line::orientation(&p1, &q1, &p2);
        assert_eq!(or1, LineSegmentOrientation::Counterclockwise);
    }

    #[test]
    pub fn insertection_test1() {
        let p1 = Point::new(10.0, 10.0);
        let q1 = Point::new(10.0, 20.0);

        let p2 = Point::new(9.0, 13.0);
        let q2 = Point::new(11.0, 16.0);

        assert!(Line::insertection(&p1, &q1, &p2, &q2));
    }

    #[test]
    pub fn insertection_test2() {
        let p1 = Point::new(10.0, 10.0);
        let q1 = Point::new(10.0, 20.0);

        let p2 = Point::new(11.0, 13.0);
        let q2 = Point::new(11.0, 16.0);

        assert!(!Line::insertection(&p1, &q1, &p2, &q2));
    }

    #[test]
    pub fn on_segment_test1() {
        let p1 = Point::new(10.0, 10.0);
        let q1 = Point::new(10.0, 20.0);

        let p2 = Point::new(10.0, 13.0);

        assert!(Line::on_segment(&p1, &q1, &p2));
    }

    #[test]
    pub fn on_segment_test2() {
        let p1 = Point::new(10.0, 10.0);
        let q1 = Point::new(10.0, 20.0);

        let p2 = Point::new(11.0, 13.0);

        assert!(!Line::on_segment(&p1, &q1, &p2));
    }

    #[test]
    pub fn polygon_add_test() {
        let mut polygon = Polygon::new(20);
        polygon.add(Point {
            x: 10.0,
            y: 10.0
        });
        polygon.add(Point {
            x: 15.0,
            y: 15.0
        });
        polygon.add(Point {
            x: 10.0,
            y: 10.0
        });
        assert!(polygon.is_valid())
    }

    #[test]
    pub fn polygon_overlap_test() {
        let mut polygon = Polygon::new(20);
        polygon.add(Point {
            x: 10.0,
            y: 10.0
        });
        polygon.add(Point {
            x: 15.0,
            y: 15.0
        });
        polygon.add(Point {
            x: 10.0,
            y: 10.0
        });
        let r = polygon.is_inside(&Point::new(11.0, 11.0));
        assert!(r);
    }

    #[test]
    pub fn polygon_not_inside_test() {
        let mut polygon = Polygon::new(20);
        polygon.add(Point {
            x: 10.0,
            y: 10.0
        });
        polygon.add(Point {
            x: 15.0,
            y: 15.0
        });
        polygon.add(Point {
            x: 10.0,
            y: 10.0
        });
        let r = polygon.is_inside(&Point::new(9.0, 11.0));
        assert!(!r);
    }

    #[test]
    pub fn polygon_on_edget_test() {
        let mut polygon = Polygon::new(20);
        polygon.add(Point {
            x: 10.0,
            y: 10.0
        });
        polygon.add(Point {
            x: 15.0,
            y: 15.0
        });
        polygon.add(Point {
            x: 10.0,
            y: 10.0
        });
        let r = polygon.is_inside(&Point::new(11.0, 11.0));
        assert!(r);
    }

    #[test]
    pub fn circle_point_inside_test() {
        let circle = Circle::new(Point::new(10.0, 10.0), 5.0);
        assert!(circle.is_inside(&Point::new(11.0, 11.0)));
        assert!(circle.is_inside(&Point::new(14.5, 14.5)));
        assert!(circle.is_inside(&Point::new(14.9, 14.9)));
        assert!(circle.is_inside(&Point::new(5.1, 5.1)));
    }

    #[test]
    pub fn circle_bounding_box_test() {
        let circle = Circle::new(Point::new(10.0, 10.0), 5.0);
        assert!(circle.is_touching_box(circle.get_bounding_box()));
        let c2 = Circle::new(Point::new(14.0, 14.0), 2.0);
        assert!(circle.is_touching_box(c2.get_bounding_box()));
        assert!(c2.is_touching_box(circle.get_bounding_box()));
    }

    #[test]
    pub fn rectangle_box_check() {
        let rect = Rectangle::new(Point::new(10.0, 10.0), 5.0, 5.0);
        let bouding_box = rect.get_bounding_box();
        assert_eq!(rect.top_left().x, bouding_box.top_left.x);
        assert_eq!(rect.top_left().y, bouding_box.top_left.y);
        assert_eq!(rect.width + rect.top_left.x, bouding_box.bottom_right.x);
        assert_eq!(rect.height + rect.top_left.y, bouding_box.bottom_right.y);
    }

    #[test]
    pub fn rectangle_inside_test() {
        let rect = Rectangle::new(Point::new(10.0, 10.0), 5.0, 5.0);
        assert!(rect.is_inside(&Point::new(11.0, 11.0)));
        assert!(rect.is_inside(&Point::new(10.0, 15.0)));
    }

    #[test]
    pub fn rectangle_outside_test() {
        let rect = Rectangle::new(Point::new(10.0, 10.0), 5.0, 5.0);
        assert!(!rect.is_inside(&Point::new(9.9, 9.9)));
    }
}
