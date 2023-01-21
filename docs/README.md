# QPML Documentation

# Converting Existing Query Plans to QPML

## Text Plans

The easiest way to import a plan into QPML is from a text representation of a plan. Here is an example of an 
Apache Spark query plan but QPML supports any text plan that uses indentation. Any leading non-alphabetic characters 
are ignored. 

```text
- LocalLimit 10
   +- Sort [revenue#289 DESC NULLS LAST, o_orderdate#60 ASC NULLS FIRST], true
      +- Aggregate [l_orderkey#16L, o_orderdate#60, o_shippriority#63], [l_orderkey#16L, sum(CheckOverflow((promote_precision(cast(l_extendedprice#21 as decimal(12,2))) * promote_precision(CheckOverflow((1.00 - promote_precision(cast(l_discount#22 as decimal(12,2)))), DecimalType(12,2), true))), DecimalType(24,4), true)) AS revenue#289, o_orderdate#60, o_shippriority#63]
         +- Project [o_orderdate#60, o_shippriority#63, l_orderkey#16L, l_extendedprice#21, l_discount#22]
            +- Join Inner, (l_orderkey#16L = o_orderkey#56L)
               :- Project [o_orderkey#56L, o_orderdate#60, o_shippriority#63]
               :  +- Join Inner, (c_custkey#0L = o_custkey#57L)
               :     :- Project [c_custkey#0L]
               :     :  +- Filter ((isnotnull(c_mktsegment#6) AND (c_mktsegment#6 = HOUSEHOLD)) AND isnotnull(c_custkey#0L))
               :     :     +- Relation [c_custkey#0L,c_name#1,c_address#2,c_nationkey#3L,c_phone#4,c_acctbal#5,c_mktsegment#6,c_comment#7] parquet
               :     +- Project [o_orderkey#56L, o_custkey#57L, o_orderdate#60, o_shippriority#63]
               :        +- Filter ((isnotnull(o_orderdate#60) AND (o_orderdate#60 < 1995-03-21)) AND (isnotnull(o_custkey#57L) AND isnotnull(o_orderkey#56L)))
               :           +- Relation [o_orderkey#56L,o_custkey#57L,o_orderstatus#58,o_totalprice#59,o_orderdate#60,o_orderpriority#61,o_clerk#62,o_shippriority#63,o_comment#64] parquet
               +- Project [l_orderkey#16L, l_extendedprice#21, l_discount#22]
                  +- Filter ((isnotnull(l_shipdate#26) AND (l_shipdate#26 > 1995-03-21)) AND isnotnull(l_orderkey#16L))
                     +- Relation [l_orderkey#16L,l_partkey#17L,l_suppkey#18L,l_linenumber#19,l_quantity#20,l_extendedprice#21,l_discount#22,l_tax#23,l_returnflag#24,l_linestatus#25,l_shipdate#26,l_commitdate#27,l_receiptdate#28,l_shipinstruct#29,l_shipmode#30,l_comment#31] parquet```
```

This plan can be imported into QPML format with the following command.

```bash
qpml import-text spark-plan.txt > spark-plan.qpml
```

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