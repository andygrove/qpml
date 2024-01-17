use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::{AvroReadOptions, CsvReadOptions, DataFrame, NdJsonReadOptions, ParquetReadOptions, SessionContext};
use datafusion_substrait::logical_plan::consumer::from_substrait_plan;
use datafusion_substrait::serializer::deserialize;
use serde::{Deserialize, Serialize};
use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt;
use std::fmt::Formatter;
use std::fs;
use std::fs::File;
use std::io::{BufRead, ErrorKind};
use std::io::{BufReader, Error};
use std::path::{Path, PathBuf};
use std::rc::Rc;
use datafusion::error::DataFusionError;
use datafusion::parquet::errors::ParquetError;

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
            ret.push_str(
                "\
",
            );
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

    println!(
        "digraph G {{
"
    );
    _generate_dot("node0".to_owned(), &doc.diagram, &styles, inverted);
    println!(
        "}}
"
    );
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
            println!(
                "\t{} -> {} [arrowhead=normal, arrowtail=none, dir=forward];",
                child_id, id
            );
        } else {
            println!(
                "\t{} -> {} [arrowhead=none, arrowtail=normal, dir=back];",
                id, child_id
            );
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
    let text = format!("{}", plan.display());
    let mut node = Node::new(&text, children);
    if let Some(i) = text.find(':') {
        let name = &text[0..i];
        node.style = Some(name.to_string().to_lowercase());
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
        // println!("adding '{}' (indent={}) to '{}' (indent={})", child.borrow().text, child.borrow().indent, self.text, self.indent);
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
    let mut stack_index = 0;
    for line in lines {
        let line = line?;
        if let Some(i) = line.find(|c: char| c == '*' || c.is_ascii_alphabetic()) {
            let node = Rc::new(RefCell::new(NodeWithIndent::new(i, &line[i..])));
            if stack.is_empty() {
                stack.push(node);
            } else if i > stack[stack_index].borrow().indent {
                stack[stack_index].borrow_mut().add_child(node.clone());
                stack.push(node.clone());
                stack_index += 1;
            } else {
                let mut parent_index = stack_index;
                while i <= stack[parent_index].borrow().indent {
                    parent_index -= 1;
                }
                stack[parent_index].borrow_mut().add_child(node.clone());
                stack.push(node.clone());
                stack_index += 1;
            }
        }
    }
    let doc = Document {
        diagram: stack[0].borrow().to_node(),
        styles: vec![],
    };
    Ok(doc)
}

pub async fn import_substrait(path: &PathBuf) -> Result<Document, Error> {
    let path = format!("{}", path.display());
    let proto = deserialize(&path).await?;
    let mut ctx = SessionContext::new();
    let plan = from_substrait_plan(&mut ctx, &proto).await?;
    let node = _from_datafusion(&plan);
    Ok(Document::new(node, vec![]))
}

pub async fn import_sql(path: &PathBuf, dir: &PathBuf) -> Result<Document, Error> {
    let path = format!("{}", path.display());
    let sql = fs::read_to_string(&path)?;
    let ctx = SessionContext::new();

    // register tables
    let paths = fs::read_dir(dir)?;
    for path in paths {
        let path = path?.path();
        let file_name =
            path.file_stem().unwrap().to_str().ok_or_else(|| {
                DataFusionError::Internal("Invalid filename".to_string())
            })?;
        let table_name = sanitize_table_name(file_name);
        println!("Registering table '{}' for {}", table_name, path.display());
        register_table(&ctx, &table_name, parse_filename(&path)?).await?;
    }


    let plan = ctx.sql(&sql).await?;
    let node = _from_datafusion(&plan.into_optimized_plan()?);
    Ok(Document::new(node, vec![]))
}

// following code copied from bdt

pub fn sanitize_table_name(name: &str) -> String {
    let mut str = String::new();
    for ch in name.chars() {
        if ch.is_ascii_alphanumeric() || ch == '_' {
            str.push(ch);
        } else {
            str.push('_')
        }
    }
    str
}

pub async fn register_table(
    ctx: &SessionContext,
    table_name: &str,
    filename: &str,
) -> Result<DataFrame, Error> {
    match file_format(filename)? {
        FileFormat::Arrow => {
            unimplemented!()
        }
        FileFormat::Avro => {
            ctx.register_avro(table_name, filename, AvroReadOptions::default())
                .await?
        }
        FileFormat::Csv => {
            ctx.register_csv(table_name, filename, CsvReadOptions::default())
                .await?
        }
        FileFormat::Json => {
            ctx.register_json(table_name, filename, NdJsonReadOptions::default())
                .await?
        }
        FileFormat::Parquet => {
            ctx.register_parquet(
                table_name,
                filename,
                ParquetReadOptions {
                    file_extension: &file_ending(filename)?,
                    ..Default::default()
                },
            )
                .await?
        }
    }
    ctx.table(table_name).await.map_err(Error::from)
}

pub fn file_format(filename: &str) -> Result<FileFormat, QpmlError> {
    match file_ending(filename)?.as_str() {
        "avro" => Ok(FileFormat::Avro),
        "csv" => Ok(FileFormat::Csv),
        "json" => Ok(FileFormat::Json),
        "parquet" | "parq" => Ok(FileFormat::Parquet),
        other => Err(QpmlError::General(format!(
            "unsupported file extension '{}'",
            other
        ))),
    }
}

pub fn parse_filename(filename: &Path) -> Result<&str, QpmlError> {
    filename
        .to_str()
        .ok_or_else(|| /*&Error::General("Invalid filename".to_string())*/ todo!())
}

pub fn file_ending(filename: &str) -> Result<String, QpmlError> {
    if let Some(ending) = std::path::Path::new(filename).extension() {
        Ok(ending.to_string_lossy().to_string())
    } else {
        Err(QpmlError::General(
            "Could not determine file extension".to_string(),
        ))
    }
}


#[derive(Debug)]
pub enum FileFormat {
    Arrow,
    Avro,
    Csv,
    Json,
    Parquet,
}

#[derive(Debug)]
pub enum QpmlError {
    General(String),
    DataFusion(DataFusionError),
    Parquet(ParquetError),
    IoError(std::io::Error),
}

impl From<DataFusionError> for QpmlError {
    fn from(e: DataFusionError) -> Self {
        Self::DataFusion(e)
    }
}

impl From<ParquetError> for QpmlError {
    fn from(e: ParquetError) -> Self {
        Self::Parquet(e)
    }
}

impl From<std::io::Error> for QpmlError {
    fn from(e: std::io::Error) -> Self {
        Self::IoError(e)
    }
}

impl From<QpmlError> for std::io::Error {
    fn from(value: QpmlError) -> Self {
        Error::new(ErrorKind::Other, format!("{value:?}"))
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::{CsvReadOptions, SessionContext};

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

    #[tokio::test]
    async fn test_from_df() -> Result<(), Error> {
        let ctx = SessionContext::default();
        ctx.register_csv("test", "testdata/test.csv", CsvReadOptions::default())
            .await?;
        let df = ctx.sql("select * from test").await?;
        let plan = df.logical_plan();
        let doc = from_datafusion(plan);
        let yaml = serde_yaml::to_string(&doc).unwrap();
        let expected = r"diagram:
  title: 'Projection: test.id, test.name'
  style: projection
  inputs:
  - title: 'TableScan: test'
    style: tablescan
";
        assert_eq!(expected.to_string(), yaml);
        Ok(())
    }
}
