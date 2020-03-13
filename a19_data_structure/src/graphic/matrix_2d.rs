use std::cell::RefCell;
use std::cmp::{max, min};
use std::rc::Rc;
use std::vec::Vec;

/// Uses row major layout.
pub struct Matrix2d<TNode> {
    rows: usize,
    cols: usize,
    length: usize,
    nodes: Vec<TNode>,
}

impl<TNode> Matrix2d<TNode> {
    /// Used to create a matrix of the specified size.
    /// #arguments
    /// * `rows` - The number of rows in the matrix.
    /// * `columns` - The number of columns in a matrix.
    /// * `created_fn` - The function to use to create the node.
    pub fn new(rows: usize, cols: usize, create_fn: fn(usize, usize) -> TNode) -> Matrix2d<TNode> {
        let length = rows * cols;
        let mut nodes = Vec::with_capacity(length);

        for row in 0..rows {
            for col in 0..cols {
                nodes.push(create_fn(row, col))
            }
        }

        Matrix2d {
            rows,
            cols,
            length,
            nodes,
        }
    }

    /// Used to calculate the position of a node.
    /// # Arguments
    /// `row` - The position in the row to calculate.
    /// `col` - The position in the column to calculate.
    fn calculate_pos(&self, row: usize, col: usize) -> usize {
        self.rows * row + col
    }

    /// Used to get the mutable reference of the node.
    /// # Arguments
    /// `row` - The row to get the node for.
    /// `col` - The col to get the node for.
    pub fn get_node(&mut self, row: usize, col: usize) -> Option<&mut TNode> {
        let pos = self.calculate_pos(row, col) as usize;
        if pos < self.length {
            Some(&mut self.nodes[pos])
        } else {
            None
        }
    }

    /// Used to walk a nodes in a grid
    /// # Arguments
    /// `row_start` - The starting of the row.
    /// `row_end` - The ending row.
    /// `col_start` - The starting column.
    /// `col_end` - The ending column.
    /// `act` - The action to run on the nodes.
    pub fn walk_nodes(
        &mut self,
        row_start: usize,
        row_end: usize,
        col_start: usize,
        col_end: usize,
        act: fn(row: usize, col: usize, node: &mut TNode),
    ) {
        let row_end_c = min(self.rows, row_end + 1);
        let col_end_c = min(self.cols, col_end + 1);
        let col_count = col_end_c - col_start + 1;

        for row in row_start..row_end_c {
            for col in col_start..col_end_c {
                match self.get_node(row, col) {
                    Some(node) => act(row, col, node),
                    None => {}
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {

    use crate::graphic::matrix_2d::Matrix2d;

    struct Node {
        row: usize,
        col: usize,
        message: String,
    }

    #[test]
    fn create_test() {
        let mut m = Matrix2d::new(10, 10, |row, col| Node {
            row,
            col,
            message: String::from("Hello world!"),
        });
        let mut n = m.get_node(1, 1);
        match n {
            Some(node) => node.message = String::from("hello"),
            _ => {}
        }

        let other = m.get_node(1, 1);
        match other {
            Some(node) => assert_eq!(&node.message[0..], "hello"),
            _ => {}
        }
    }

    #[test]
    fn walk_nodes_test() {
        let mut m = Matrix2d::new(10, 10, |row, col| Node {
            row,
            col,
            message: String::from("Hello world!"),
        });
        m.walk_nodes(3, 5, 2, 4, |x, y, node| {
            assert!(x >= 3);
            assert!(x <= 5);
            assert!(y >= 2);
            assert!(y <= 4);
        });
    }

    #[test]
    fn walk_nodes_out_of_bounds() {
        let mut m = Matrix2d::new(10, 10, |row, col| Node {
            row,
            col,
            message: String::from("Hello world!"),
        });
        m.walk_nodes(8, 11, 9, 11, |x, y, node| {
            assert!(x >= 8);
            assert!(x <= 9);
            assert!(y >= 9);
            assert!(y <= 9);
        });
    }
}
