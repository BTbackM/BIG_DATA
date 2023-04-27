package optimizer;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.externalize.RelWriterImpl;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;
import org.junit.Test;

import java.io.PrintWriter;
import java.io.StringWriter;

public class OptimizerAlumnoTest {
    @Test
    public void test_tpch_q6() throws Exception {
        SimpleTable alumnos = SimpleTable.newBuilder("alumnos")
            .addField("id", SqlTypeName.INTEGER)
            .addField("nombre", SqlTypeName.VARCHAR)
            .addField("apellido", SqlTypeName.VARCHAR)
            .addField("edad", SqlTypeName.INTEGER)
            .addField("promedio", SqlTypeName.INTEGER)
            .withRowCount(50_000L)
            .build();

        SimpleTable NotasAlumnos = SimpleTable.newBuilder("NotasAlumnos")
            .addField("id", SqlTypeName.INTEGER)
            .addField("nota", SqlTypeName.INTEGER)
            .withRowCount(50_000L)
            .build();

        SimpleSchema schema = SimpleSchema.newBuilder("tpch").addTable(alumnos).addTable(NotasAlumnos).build();

        Optimizer optimizer = Optimizer.create(schema);

        String sql =
          "SELECT A.nombre as nombre, A.apellido as apellido, A.promedio as promedio\n" +
          "FROM alumnos A\n" +
          "INNER JOIN NotasAlumnos NA\n" +
          "ON A.id = NA.id\n" +
          "WHERE A.promedio < ?" +
          "AND A.nombre LIKE ?" +
          "AND A.apellido LIKE ?" +
          "AND NA.nota > ?";

        SqlNode sqlTree = optimizer.parse(sql);
        SqlNode validatedSqlTree = optimizer.validate(sqlTree);
        RelNode relTree = optimizer.convert(validatedSqlTree);

        print("AFTER CONVERSION", relTree);

        RuleSet rules = RuleSets.ofList(
            CoreRules.FILTER_INTO_JOIN,
            CoreRules.FILTER_TO_CALC,
            CoreRules.PROJECT_TO_CALC,
            CoreRules.FILTER_CALC_MERGE,
            CoreRules.PROJECT_CALC_MERGE,
            EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE,
            EnumerableRules.ENUMERABLE_PROJECT_RULE,
            EnumerableRules.ENUMERABLE_FILTER_RULE,
            EnumerableRules.ENUMERABLE_JOIN_RULE,
            EnumerableRules.ENUMERABLE_CALC_RULE,
            EnumerableRules.ENUMERABLE_AGGREGATE_RULE
        );

        RelNode optimizerRelTree = optimizer.optimize(
            relTree,
            relTree.getTraitSet().plus(EnumerableConvention.INSTANCE),
            rules
        );

        print("AFTER OPTIMIZATION", optimizerRelTree);
    }

    private void print(String header, RelNode relTree) {
        StringWriter sw = new StringWriter();

        sw.append(header).append(":").append("\n");

        RelWriterImpl relWriter = new RelWriterImpl(new PrintWriter(sw), SqlExplainLevel.ALL_ATTRIBUTES, true);

        relTree.explain(relWriter);

        System.out.println(sw.toString());
    }
}
