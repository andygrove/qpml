# QPML Documentation

# Converting Existing Query Plans to QPML

## Apache Spark

Add a dependency on `jackson-dataformat-yaml`:
```xml
<dependency>
    <groupId>com.fasterxml.jackson.dataformat</groupId>
    <artifactId>jackson-dataformat-yaml</artifactId>
    <version>2.12.3</version>
</dependency>
```

Sample code for generating QPML text from a Spark logical plan:

```scala
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Join, LogicalPlan, Project}
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}

case class Node(@JsonProperty("title") title: String,
                @JsonProperty("operator") operator: String,
                @JsonProperty("inputs") inputs: java.util.List[Node])

object Qpml {

  def fromLogicalPlan(plan: LogicalPlan): String = {

    def _fromLogicalPlan(plan: LogicalPlan): Node = {
      import collection.JavaConverters._
      val children = plan.children.map(_fromLogicalPlan).asJava
      plan match {
        case f: LogicalRelation =>
          val title = f.relation.asInstanceOf[HadoopFsRelation].location.rootPaths.head.getName
          Node(title, "scan", children)
        case j: Join =>
          val title = s"${j.joinType} Join: ${j.condition}"
          Node(title, "join", children)
        case p: Project =>
          val title = s"Projection: ${p.projectList.mkString(", ")}"
          Node(title, "projection", children)
        case f: Filter =>
          val title = s"Filter: ${f.condition}"
          Node(title, "filter", children)
        case _ =>
          val title = plan.simpleStringWithNodeId()
          Node(title, plan.getClass.getSimpleName, children)
      }
    }

    val mapper = new ObjectMapper(new YAMLFactory())
    mapper.registerModule(DefaultScalaModule)
    mapper.writeValueAsString(_fromLogicalPlan(plan))
  }

}
```

## Apache Arrow DataFusion & Ballista

The qpml crate includes a utility function `from_datafusion` for converting DataFusion logical plans into QPML
format.

The following dependencies are required:

```toml
datafusion = "15.0"
qpml = "0.5"
serde = { version = "1.0", features = ["derive"] }
serde_yaml = "0.9"
```

Here is a code sample for writing a DataFusion logical plan to disk in QPML format.

```rust
use qpml::from_datafusion;
use std::fs::File;
use std::io::BufWriter;

let qpml = from_datafusion(&plan);
let filename = format!("q{}.qpml", query_no);
let file = File::create(&filename)?;
let mut file = BufWriter::new(file);
serde_yaml::to_writer(&mut file, &qpml).unwrap();
```