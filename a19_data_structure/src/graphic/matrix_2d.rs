use std::vec::{
    Vec,
};
use std::rc::Rc;
use std::cmp::{
    min,
    max
};
use std::cell::RefCell;

/// Uses row major layout.
pub struct Matrix2d<TNode> {
    rows: usize,
    cols: usize,
    length: usize,
    nodes: Vec<TNode>
}

pub fn create_matrix<TNode>(
    rows: usize,
    cols: usize,
    create_fn: fn(usize, usize) -> TNode) -> Matrix2d<TNode> {
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
        nodes
    }
}

impl<TNode> Matrix2d<TNode> {

    /// Used to calculate the position of a node.
    fn calculate_pos(&self, row: usize, col: usize) -> usize {
        self.rows * row + col
    }

    /// Used to get the mutable reference of the node.
    /// #arguments
    /// 'row' - The row to get the node for.
    /// 'col' - The col to get the node for.
    pub fn get_node(&mut self, row: usize, col: usize) -> Option<&mut TNode> {
        let pos = self.calculate_pos(row, col) as usize;
        if pos < self.length {
            Some(&mut self.nodes[pos])
        } else {
            None
        }
    }

    /// Used to walk a nodes in a grid
    pub fn walk_nodes(
        &mut self,
        row_start: usize, row_end: usize,
        col_start: usize, col_end: usize,
        act: fn(row:usize, col:usize, node: &mut TNode)) {
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

    use crate::graphic::matrix_2d::{
        Matrix2d,
        create_matrix
    };

    struct Node {
        row: usize,
        col: usize,
        message: String
    }

    #[test]
    fn create_test() {
        let mut m = create_matrix(10, 10, |row, col| {
            Node {
                row,
                col,
                message: String::from("Hello world!")
            }
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
        let mut m = create_matrix(10, 10, |row, col| {
            Node {
                row,
                col,
                message: String::from("Hello world!")
            }
        });
        m.walk_nodes(
            3, 5,
            2, 4,
            |x, y, node| {
                assert!(x >= 3);
                assert!(x <= 5);
                assert!(y >= 2);
                assert!(y <= 4);
            });
    }

    #[test]
    fn walk_nodes_out_of_bounds() {
        let mut m = create_matrix(10, 10, |row, col| {
            Node {
                row,
                col,
                message: String::from("Hello world!")
            }
        });
        m.walk_nodes(
            8, 11,
            9, 11,
            |x, y, node| {
                assert!(x >= 8);
                assert!(x <= 9);
                assert!(y >= 9);
                assert!(y <= 9);
            });
    }

}
