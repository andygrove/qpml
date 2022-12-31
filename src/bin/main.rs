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
}

fn main() {
    match Opt::from_args() {
        Opt::Print { input } => {
            let yaml = fs::read_to_string(&input).expect("Unable to read file");
            let doc: qpml::Document = serde_yaml::from_str(&yaml).unwrap();
            qpml::generate_text(&doc);
        }
        Opt::Dot { input, inverted } => {
            let yaml = fs::read_to_string(&input).expect("Unable to read file");
            let doc: qpml::Document = serde_yaml::from_str(&yaml).unwrap();
            qpml::generate_dot(&doc, inverted);
        }
        Opt::Mermaid { input, inverted } => {
            let yaml = fs::read_to_string(&input).expect("Unable to read file");
            let doc: qpml::Document = serde_yaml::from_str(&yaml).unwrap();
            qpml::generate_mermaid(&doc, inverted);
        }
    }
}
