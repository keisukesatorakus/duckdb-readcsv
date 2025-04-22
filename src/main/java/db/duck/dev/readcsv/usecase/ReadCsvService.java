package db.duck.dev.readcsv.usecase;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import javax.sql.DataSource;

import org.jooq.impl.DSL;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import db.duck.dev.readcsv.domain.CsvImportFormat;
import db.duck.dev.readcsv.domain.Header;
import db.duck.dev.readcsv.domain.ImportField;
import db.duck.dev.readcsv.domain.ImportFieldType;
import db.duck.dev.readcsv.domain.ImportMethod;
import lombok.AllArgsConstructor;

@Service
@AllArgsConstructor
public class ReadCsvService {

  //  private final DSLContext dsl;
  private final DataSource dataSource;

  public record Data(
      long size,
      List<Header> headers,
      List<String[]> rows
  ) {

  }

  @Transactional
  public Data read(String s3Url, boolean header, int skip) {

    // format
    var format = new CsvImportFormat(
        1, "",
        Map.of(
            ImportField.CUST_CD, new ImportMethod.FromCsvColumn(4),
            ImportField.AMOUNT, new ImportMethod.FromCsvColumn(6),
            ImportField.DATE, new ImportMethod.FromCsvColumn(1)
        )
    );

    var con = DataSourceUtils.getConnection(dataSource);
    var dsl = DSL.using(con);

    // 1. 対象S3 CSVのカラム一覧を取得
    var pragmaResult = dsl.fetch(String.format("DESCRIBE SELECT * FROM read_csv_auto('%s')", s3Url));
    List<String> columnNames = pragmaResult.getValues("column_name", String.class);

    // columnNames を index をキーに map に変換
    var columnNameMap = IntStream.range(0, columnNames.size())
        .boxed()
        .collect(Collectors.toMap(
            i -> i,
            columnNames::get
        ));

    var selectFields = format.getMappings().entrySet().stream()
        .filter(e -> e.getValue() instanceof ImportMethod.FromCsvColumn)
        .map(e -> {
          var field = e.getKey();
          var index = ((ImportMethod.FromCsvColumn) e.getValue()).columnIndex();
          var columnName = columnNameMap.get(index);
          return String.format("%s as %s", columnName, field);
        })
        .collect(Collectors.joining(","));

    // <order, カラム> formatで設定されているカラムを取得
    var overrideColumns = format.getMappings().entrySet().stream()
        .filter(entry -> entry.getValue() instanceof ImportMethod.FromCsvColumn)
        .collect(Collectors.toMap(
            entry -> ((ImportMethod.FromCsvColumn) entry.getValue()).columnIndex(),
            Map.Entry::getKey
        ));

    // <カラム名, 型名> CSVヘッダ一覧
    var columns = IntStream.range(0, columnNames.size())
        .boxed()
        .collect(Collectors.toMap(
            columnNames::get,
            i -> Optional.ofNullable(overrideColumns.get(i)).map(ImportField::getType).orElse(ImportFieldType.VARCHAR),
            (v1, v2) -> v1,
            LinkedHashMap::new
        ));

    // read_csv_auto のカラム設定
    String columnsScan = columns.entrySet().stream()
        .map(entry -> String.format("'%s': '%s'", entry.getKey(), entry.getValue()))
        .collect(Collectors.joining(", "));

    var count = dsl.fetch(
        String.format(
            """
                  SELECT
                    count(*)
                  FROM
                  read_csv_auto(
                    '%s',
                    skip = %d,
                    header = %s
                  )
                """,
            s3Url,
            skip,
            header
        )
    ).getFirst().get(0, Long.class);

    var res = dsl.fetch(
        String.format(
            """
                  SELECT
                    %s
                  FROM
                  read_csv_auto(
                    '%s',
                    columns = {{ %s }},
                    store_rejects = true,
                    rejects_scan = 'reject_scans',
                    rejects_table = 'reject_errors',
                    rejects_limit = 1000,
                    skip = %d,
                    header = %s
                  )
                """,
            selectFields,
            s3Url,
            columnsScan,
            skip,
            header
        )
    );

//    // s3 join
//    var query = dsl.fetch(
//        String.format(
//            """
//                SELECT t1.*
//                FROM read_csv_auto(
//                  '%s',
//                  columns = {{ %s }},
//                  store_rejects = true,
//                  rejects_scan = 'reject_scans',
//                  rejects_table = 'reject_errors',
//                  rejects_limit = 1000
//                ) t1
//                JOIN read_csv_auto('%s') t2
//                ON t1.%s = t2.%s
//                """,
//            s3Url,
//            columnsScan,
//            s3Url,
//            columnNames.get(4),
//            columnNames.get(4)
//        )
//    );

    // store_rejects
    var rejectErrors = dsl.select()
        .from("reject_errors")
        .fetch();
    printResult(rejectErrors);

    var rejectScans = dsl.select()
        .from("reject_scans")
        .fetch();
    printResult(rejectScans);

    return new Data(
        count,
        // header
        format.getMappings().keySet().stream().map(
            importMethod -> new Header(importMethod.name(), importMethod.getType().name())).toList(),
        // rows
        res.stream()
            .map(record -> Arrays.stream(record.intoArray())
                .map(val -> val != null ? val.toString() : "")
                .toArray(String[]::new)
            )
            .toList()
    );
  }

  private void printResult(org.jooq.Result<?> result) {
    for (var record : result) {
      for (var field : result.fields()) {
        var columnName = field.getName();
        var value = record.get(field);
        System.out.printf("%s = %s%n", columnName, value);
      }
      System.out.println("-----");
    }
  }
}
