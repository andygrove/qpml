use serde::{Deserialize, Serialize};
use std::fs;
use std::path::PathBuf;
use structopt::StructOpt;

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Node {
    title: String,
    inputs: Vec<Box<Node>>,
}

impl Node {
    pub fn new(title: &str, inputs: Vec<Box<Node>>) -> Self {
        Self {
            title: title.to_owned(),
            inputs,
        }
    }
    pub fn new_leaf(title: &str) -> Self {
        Self {
            title: title.to_owned(),
            inputs: vec![],
        }
    }
}

#[derive(StructOpt)]
#[structopt(about = "Query Plan Markup Language")]
enum Opt {
    /// Print a textual representation
    Print {
        #[structopt(parse(from_os_str))]
        input: PathBuf,
    },
    /// Generate a DOT diagram
    Dot {
        #[structopt(parse(from_os_str))]
        input: PathBuf,
    },
}

fn main() {
    match Opt::from_args() {
        Opt::Print { input } => {
            let yaml = fs::read_to_string(&input).expect("Unable to read file");
            let plan: Node = serde_yaml::from_str(&yaml).unwrap();
            display(&plan, "");
        }
        Opt::Dot { input } => {
            todo!()
        }
    }
}

/// Show a text representation of the plan
pub fn display(node: &Node, indent: &str) {
    println!("{}{}", indent, node.title);
    let new_indent = indent.to_owned() + "  ";
    for child in &node.inputs {
        display(&child, &new_indent);
    }
}

pub fn read_yaml(path: &str) -> Node {
    let yaml = fs::read_to_string(path).expect("Unable to read file");
    serde_yaml::from_str(&yaml).unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_read_yaml() {
        let node = read_yaml("./examples/example1.yaml");
        println!("{:?}", node);
    }

    #[test]
    fn write_yaml() {
        let example = Box::new(Node::new(
            "Inner Join: cs_ship_date_sk = d3.d_date_sk",
            vec![
                Box::new(Node::new(
                    "Inner Join: inv_date_sk = d2.d_date_sk",
                    vec![
                        Box::new(Node::new(
                            "Inner Join: cs_sold_date_sk = d1.d_date_sk",
                            vec![
                                Box::new(Node::new(
                                    "Inner Join: cs_bill_hdemo_sk = hd_demo_sk",
                                    vec![
                                        Box::new(Node::new(
                                            "Inner Join: cs_bill_cdemo_sk = cd_demo_sk",
                                            vec![
                                                Box::new(Node::new(
                                                    "Inner Join: i_item_sk = cs_item_sk",
                                                    vec![
                                                        Box::new(Node::new(
                                                            "Inner Join: w_warehouse_sk = inv_warehouse_sk",
                                                            vec![
                                                                Box::new(Node::new(
                                                                    "Inner Join: cs_item_sk = inv_item_sk",
                                                                    vec![
                                                                        Box::new(Node::new_leaf("catalog_sales")),
                                                                        Box::new(Node::new_leaf("inventory")),
                                                                    ],
                                                                )),
                                                                Box::new(Node::new_leaf("warehouse")),
                                                            ],
                                                        )),
                                                        Box::new(Node::new_leaf("item")),
                                                    ],
                                                )),
                                                Box::new(Node::new_leaf("customer_demographics")),
                                            ],
                                        )),
                                        Box::new(Node::new_leaf("household_demographics")),
                                    ],
                                )),
                                Box::new(Node::new_leaf("d1")),
                            ],
                        )),
                        Box::new(Node::new_leaf("d2")),
                    ],
                )),
                Box::new(Node::new_leaf("d3")),
            ],
        ));
        let yaml = serde_yaml::to_string(&example).unwrap();
        println!("{}", yaml);
    }
}
