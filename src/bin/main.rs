use qpml::{from_text_plan, import_sql, import_substrait};
use std::fs;
use std::path::PathBuf;
use structopt::StructOpt;

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
        #[structopt(long)]
        inverted: bool,
    },
    /// Generate a Mermaid diagram
    Mermaid {
        #[structopt(parse(from_os_str))]
        input: PathBuf,
        #[structopt(long)]
        inverted: bool,
    },
    /// Import a text plan and convert to QPML
    ImportText {
        #[structopt(parse(from_os_str))]
        input: PathBuf,
    },
    /// Import a Substrait plan and convert to QPML
    ImportSubstrait {
        #[structopt(parse(from_os_str))]
        input: PathBuf,
    },
    /// Generate from a SQL query file
    ImportSql {
        #[structopt(parse(from_os_str))]
        /// Path to file containing SQL query
        input: PathBuf,
        #[structopt(parse(from_os_str))]
        /// Path to data files
        tables: PathBuf,
    },
}

#[tokio::main]
async fn main() {
    match Opt::from_args() {
        Opt::Print { input } => {
            let yaml = fs::read_to_string(input).expect("Unable to read file");
            let doc: qpml::Document = serde_yaml::from_str(&yaml).unwrap();
            qpml::generate_text(&doc);
        }
        Opt::Dot { input, inverted } => {
            let yaml = fs::read_to_string(input).expect("Unable to read file");
            let doc: qpml::Document = serde_yaml::from_str(&yaml).unwrap();
            qpml::generate_dot(&doc, inverted);
        }
        Opt::Mermaid { input, inverted } => {
            let yaml = fs::read_to_string(input).expect("Unable to read file");
            let doc: qpml::Document = serde_yaml::from_str(&yaml).unwrap();
            qpml::generate_mermaid(&doc, inverted);
        }
        Opt::ImportText { input } => {
            let doc = from_text_plan(&input).unwrap();
            let str = serde_yaml::to_string(&doc).unwrap();
            println!("{}", str);
        }
        Opt::ImportSubstrait { input } => {
            let doc = import_substrait(&input).await.unwrap();
            let str = serde_yaml::to_string(&doc).unwrap();
            println!("{}", str);
        }
        Opt::ImportSql { input, tables } => {
            let doc = import_sql(&input, &tables).await.unwrap();
            let str = serde_yaml::to_string(&doc).unwrap();
            println!("{}", str);
        }
    }
}
