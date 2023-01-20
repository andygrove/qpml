use datafusion::logical_expr::LogicalPlan;
use serde::{Deserialize, Serialize};
use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt;
use std::fmt::Formatter;
use std::fs;
use std::fs::File;
use std::io::BufRead;
use std::io::{BufReader, Error};
use std::path::PathBuf;
use std::rc::Rc;

#[derive(Debug, PartialEq, Serialize, Deserialize, Default)]
#[allow(clippy::vec_box)]
pub struct Document {
    diagram: Box<Node>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    styles: Vec<Style>,
}

impl Document {
    pub fn new(diagram: Box<Node>, styles: Vec<Style>) -> Self {
        Self { diagram, styles }
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Default, Clone)]
#[allow(clippy::vec_box)]
pub struct Node {
    title: String,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    style: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    inputs: Vec<Box<Node>>,
}

impl Node {
    pub fn new(title: &str, inputs: Vec<Box<Node>>) -> Self {
        Self {
            title: title.to_owned(),
            style: None,
            inputs,
        }
    }
    pub fn new_leaf(title: &str, style: Option<&str>) -> Self {
        Self {
            title: title.to_owned(),
            style: style.map(|name| name.to_owned()),
            inputs: vec![],
        }
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Default, Clone)]
#[allow(clippy::vec_box)]
pub struct Style {
    name: String,
    color: String,
    shape: String,
}

impl Style {
    pub fn new(name: &str, color: &str, shape: &str) -> Self {
        Self {
            name: name.to_owned(),
            color: color.to_owned(),
            shape: shape.to_owned(),
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
        let line = line.replace('\"', "\\\"");
        ret.push_str(&line);
        if i + line_len < s.len() {
            ret.push_str("\\n");
        }
        i += line_len;
    }
    ret
}

/// Show a text representation of the plan
pub fn generate_text(node: &Document) {
    _display(&node.diagram, "");
}

pub fn _display(node: &Node, indent: &str) {
    println!("{}{}", indent, node.title);
    let new_indent = indent.to_owned() + "  ";
    for child in &node.inputs {
        _display(child.as_ref(), &new_indent);
    }
}

pub fn read_yaml(path: &str) -> Node {
    let yaml = fs::read_to_string(path).expect("Unable to read file");
    serde_yaml::from_str(&yaml).unwrap()
}

pub fn generate_dot(doc: &Document, inverted: bool) {
    // build styles
    let mut styles: HashMap<String, Style> = HashMap::new();
    for style in &doc.styles {
        styles.insert(style.name.clone(), style.to_owned());
    }

    println!("digraph G {{\n");
    _generate_dot("node0".to_owned(), &doc.diagram, &styles, inverted);
    println!("}}\n");
}

fn _generate_dot(id: String, node: &Node, styles: &HashMap<String, Style>, inverted: bool) {
    let mut dot_node = DotNode {
        name: id.clone(),
        label: Some(node.title.clone()),
        color: None,
        fill_color: None,
        style: None,
    };

    if let Some(s) = &node.style {
        if let Some(def) = styles.get(s) {
            dot_node.color = Some(def.color.clone());
            dot_node.fill_color = Some(def.color.clone());
            dot_node.style = Some("filled".to_owned());
        }
    }
    println!("{}", dot_node);

    for (i, p) in node.inputs.iter().enumerate() {
        let child_id = format!("{}_{}", id, i);
        if inverted {
            println!("\t{} -> {};", child_id, id);
        } else {
            println!("\t{} -> {};", id, child_id);
        }
        _generate_dot(child_id.clone(), p, styles, inverted);
    }
}

pub fn generate_mermaid(doc: &Document, inverted: bool) {
    println!("```mermaid");
    println!("flowchart TD");
    _generate_mermaid("node0".to_owned(), &doc.diagram, inverted);
    println!("```");
}

fn _generate_mermaid(id: String, node: &Node, inverted: bool) {
    for (i, p) in node.inputs.iter().enumerate() {
        let child_id = format!("{}_{}", id, i);
        if inverted {
            println!("{}[{}] --> {}[{}]", child_id, p.title, id, node.title);
        } else {
            println!("{}[{}] --> {}[{}]", id, node.title, child_id, p.title);
        }
        _generate_mermaid(child_id.clone(), p, inverted);
    }
}

/// Create QPML document from a DataFusion logical plan
pub fn from_datafusion(plan: &LogicalPlan) -> Document {
    let node = _from_datafusion(plan);
    Document::new(node, vec![])
}

fn _from_datafusion(plan: &LogicalPlan) -> Box<Node> {
    let children = plan.inputs().iter().map(|x| _from_datafusion(x)).collect();
    let mut node = Node::new("unknown", children);
    match plan {
        LogicalPlan::TableScan(scan) => {
            node.title = scan.table_name.clone();
            node.style = Some("scan".to_owned());
        }
        LogicalPlan::Filter(filter) => {
            node.title = format!("Filter: {}", filter.predicate());
            node.style = Some("filter".to_owned());
        }
        LogicalPlan::Projection(projection) => {
            let expr: Vec<String> = projection.expr.iter().map(|e| format!("{}", e)).collect();
            node.title = format!("Projection: {}", expr.join(", "));
            node.style = Some("projection".to_owned());
        }
        LogicalPlan::Join(join) => {
            let join_cols: Vec<String> = join
                .on
                .iter()
                .map(|(l, r)| format!("{} = {}", l, r))
                .collect();
            node.title = format!("Join: {}", join_cols.join(" AND "));
            node.style = Some("join".to_owned());
        }
        _ => {}
    }
    Box::new(node)
}

#[derive(Clone)]
struct NodeWithIndent {
    indent: usize,
    text: String,
    inputs: Vec<Rc<RefCell<NodeWithIndent>>>,
}

impl NodeWithIndent {
    fn new(indent: usize, text: &str) -> Self {
        Self {
            indent,
            text: text.to_string(),
            inputs: vec![],
        }
    }

    fn add_child(&mut self, child: Rc<RefCell<NodeWithIndent>>) {
        self.inputs.push(child);
    }

    fn to_node(&self) -> Box<Node> {
        let inputs = self.inputs.iter().map(|n| n.borrow().to_node()).collect();
        Box::new(Node::new(&self.text, inputs))
    }
}

pub fn from_text_plan(filename: &PathBuf) -> Result<Document, Error> {
    let file = File::open(filename)?;
    let lines = BufReader::new(file).lines();
    let mut stack = vec![];
    for line in lines {
        let line = line?;
        if let Some(i) = line.find(|c: char| c.is_ascii_alphabetic()) {
            let node = Rc::new(RefCell::new(NodeWithIndent::new(i, &line[i..])));
            if stack.is_empty() {
                stack.push(node);
            } else if i > stack.last().unwrap().borrow().indent {
                stack.last().unwrap().borrow_mut().add_child(node.clone());
                stack.push(node.clone());
            } else {
                while i <= stack.last().unwrap().borrow().indent {
                    stack.pop();
                }
                stack.last().unwrap().borrow_mut().add_child(node);
            }
        }
    }
    let doc = Document {
        diagram: stack[0].borrow().to_node(),
        styles: vec![],
    };
    Ok(doc)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_read_yaml() {
        let node = read_yaml("./examples/example1.qpml");
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
                                                                        Box::new(Node::new_leaf("catalog_sales", Some("table"))),
                                                                        Box::new(Node::new_leaf("inventory", Some("table"))),
                                                                    ],
                                                                )),
                                                                Box::new(Node::new_leaf("warehouse", Some("table"))),
                                                            ],
                                                        )),
                                                        Box::new(Node::new_leaf("item", Some("table"))),
                                                    ],
                                                )),
                                                Box::new(Node::new_leaf("customer_demographics", Some("table"))),
                                            ],
                                        )),
                                        Box::new(Node::new_leaf("household_demographics", Some("table"))),
                                    ],
                                )),
                                Box::new(Node::new_leaf("d1", Some("table"))),
                            ],
                        )),
                        Box::new(Node::new_leaf("d2", Some("table"))),
                    ],
                )),
                Box::new(Node::new_leaf("d3", Some("table"))),
            ],
        ));

        let doc = Document {
            diagram: example,
            styles: vec![
                Style::new("table", "blue", "rectangle"),
                Style::new("operator", "green", "rectangle"),
            ],
        };

        let yaml = serde_yaml::to_string(&doc).unwrap();
        println!("{}", yaml);

        let doc2: Document = serde_yaml::from_str(&yaml).unwrap();
        println!("{:?}", doc2);
    }
}
