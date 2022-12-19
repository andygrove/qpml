use datafusion::logical_expr::LogicalPlan;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::fmt::Formatter;
use std::fs;

#[derive(Debug, PartialEq, Serialize, Deserialize, Default)]
#[allow(clippy::vec_box)]
pub struct Node {
    title: String,
    color: Option<String>,
    operator: Option<String>,
    inputs: Option<Vec<Box<Node>>>,
}

impl Node {
    pub fn new(title: &str, inputs: Vec<Box<Node>>) -> Self {
        Self {
            title: title.to_owned(),
            color: None,
            operator: None,
            inputs: Some(inputs),
        }
    }
    pub fn new_leaf(title: &str) -> Self {
        Self {
            title: title.to_owned(),
            color: None,
            operator: None,
            inputs: None,
        }
    }
}

pub struct DotNode {
    name: String,
    label: Option<String>,
    color: Option<String>,
    fill_color: Option<String>,
    style: Option<String>,
}

impl fmt::Display for DotNode {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name)?;

        // optional attributes
        let mut attr = "".to_string();

        attr.push_str("shape=box");

        if let Some(label) = &self.label {
            attr.push_str(&format!("; label=\"{}\"", wrap(label)));
        }
        if let Some(color) = &self.color {
            attr.push_str(&format!("; color=\"{}\"", color));
        }
        if let Some(color) = &self.fill_color {
            attr.push_str(&format!("; fillcolor=\"{}\"", color));
        }
        if let Some(style) = &self.style {
            attr.push_str(&format!("; style=\"{}\"", style));
        }
        write!(f, " [{}];", attr)
    }
}

fn wrap(s: &str) -> String {
    let mut ret = String::new();
    let mut i = 0;
    let line_len = 30;
    while i < s.len() {
        let end = (i + line_len).min(s.len());
        let line = &s[i..end];
        // escape quotes
        let line = line.replace("\"", "\\\"");
        ret.push_str(&line);
        if i + line_len < s.len() {
            ret.push_str("\\n");
        }
        i += line_len;
    }
    ret
}

/// Show a text representation of the plan
pub fn display(node: &Node, indent: &str) {
    println!("{}{}", indent, node.title);
    let new_indent = indent.to_owned() + "  ";
    if let Some(inputs) = &node.inputs {
        for child in inputs {
            display(child, &new_indent);
        }
    }
}

pub fn read_yaml(path: &str) -> Node {
    let yaml = fs::read_to_string(path).expect("Unable to read file");
    serde_yaml::from_str(&yaml).unwrap()
}

pub fn generate_dot(node: &Node, inverted: bool) {
    println!("digraph G {{\n");
    _generate_dot("node0".to_owned(), node, inverted);
    println!("}}\n");
}

fn _generate_dot(id: String, node: &Node, inverted: bool) {
    let mut dot_node = DotNode {
        name: id.clone(),
        label: Some(node.title.clone()),
        color: None,
        fill_color: None,
        style: None,
    };

    if let Some(c) = &node.color {
        dot_node.color = Some(c.clone());
        dot_node.style = Some("filled".to_owned());
    } else if let Some(operator) = &node.operator {
        let c = match operator.as_str() {
            "scan" => Some("lightblue"),
            "join" => Some("lightyellow"),
            "filter" => Some("aquamarine"),
            "project" => None,
            _ => None,
        };
        if let Some(c) = c {
            dot_node.fill_color = Some(c.to_string());
            dot_node.style = Some("filled".to_owned());
        }
    }

    println!("{}", dot_node);

    if let Some(inputs) = &node.inputs {
        for (i, p) in inputs.iter().enumerate() {
            let child_id = format!("{}_{}", id, i);
            if inverted {
                println!("\t{} -> {};", child_id, id);
            } else {
                println!("\t{} -> {};", id, child_id);
            }
            _generate_dot(child_id.clone(), p, inverted);
        }
    }
}

/// Create QPML from a DataFusion logical plan
pub fn from_datafusion(plan: &LogicalPlan) -> Box<Node> {
    let children = plan.inputs().iter().map(|x| from_datafusion(x)).collect();
    let mut node = Node::new("unknown", children);
    match plan {
        LogicalPlan::TableScan(scan) => {
            node.title = scan.table_name.clone();
            node.operator = Some("scan".to_owned());
        }
        LogicalPlan::Filter(filter) => {
            node.title = format!("Filter: {}", filter.predicate());
            node.operator = Some("filter".to_owned());
        }
        LogicalPlan::Projection(projection) => {
            let expr: Vec<String> = projection.expr.iter().map(|e| format!("{}", e)).collect();
            node.title = format!("Projection: {}", expr.join(", "));
            node.operator = Some("projection".to_owned());
        }
        LogicalPlan::Join(join) => {
            let join_cols: Vec<String> = join
                .on
                .iter()
                .map(|(l, r)| format!("{} = {}", l, r))
                .collect();
            node.title = format!("Join: {}", join_cols.join(" AND "));
            node.operator = Some("join".to_owned());
        }
        _ => {}
    }
    Box::new(node)
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
