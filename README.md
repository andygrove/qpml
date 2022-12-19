# Query Plan Markup Language (QPML)

QPML is a YAML-based DSL for describing query plans for the purposes of producing diagrams and textual representations
of query plans for use in documentation and presentations.

## Example

Here is a minimal example of a qpml file. See [examples/example1.yaml](examples/example1.yaml) for a fuller example.

```yaml
title: 'Inner Join: w_warehouse_sk = inv_warehouse_sk'
operator: join
inputs:
  - title: 'Inner Join: cs_item_sk = inv_item_sk'
    operator: join
    inputs:
      - title: catalog_sales
        operator: scan
      - title: inventory
        operator: scan
  - title: warehouse
    operator: scan
```

## Tools

### Generate Query Plan Diagram

```shell
qpml dot example1.yaml > example1.dot
dot -Tpng example1.dot > example1.png
```

![Example Diagram](examples/example1.png)

### Generate Text Plan

```shell
$ qpml print example1.yaml
```

```
Inner Join: cs_ship_date_sk = d3.d_date_sk
  Inner Join: inv_date_sk = d2.d_date_sk
    Inner Join: cs_sold_date_sk = d1.d_date_sk
      Inner Join: cs_bill_hdemo_sk = hd_demo_sk
        Inner Join: cs_bill_cdemo_sk = cd_demo_sk
          Inner Join: i_item_sk = cs_item_sk
            Inner Join: w_warehouse_sk = inv_warehouse_sk
              Inner Join: cs_item_sk = inv_item_sk
                catalog_sales
                inventory
              warehouse
            item
          customer_demographics
        household_demographics
      d1
    d2
  d3
```

# Generating QPML from Query Engines

## Apache Spark

Add a dependency on `jackson-dataformat-yaml`:
```xml
<dependency>
    <groupId>com.fasterxml.jackson.dataformat</groupId>
    <artifactId>jackson-dataformat-yaml</artifactId>
    <version>${scala.binary.version}.3</version>
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
