package db.duck.dev.readcsv.controller;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import db.duck.dev.readcsv.usecase.ReadCsvService;
import db.duck.dev.readcsv.usecase.ReadCsvStmtService;
import lombok.AllArgsConstructor;

@RestController
@AllArgsConstructor
@RequestMapping("/s3")
public class ReadCsvController {

  private final ReadCsvService readCsvService;
  private final ReadCsvStmtService readCsvStmtService;

  private final static String bucketName = "duck-db-dev";

  // http://localhost:8080/s3/duck-read/jooq?key=dev/&header=true&skip=0
  @GetMapping("/duck-read/jooq")
  public ReadCsvService.Data processCsv(
      @RequestParam String key,
      @RequestParam(defaultValue = "true") boolean header,
      @RequestParam(defaultValue = "0") int skip
  ) {
    String s3Url = "s3://" + bucketName + "/" + key;
    return readCsvService.read(s3Url, header, skip);
  }

  @GetMapping("/duck-read/stmt")
  public ReadCsvStmtService.Data processCsvStmt(
      @RequestParam String key,
      @RequestParam(defaultValue = "true") boolean header,
      @RequestParam(defaultValue = "0") int skip
  ) {
    String s3Url = "s3://" + bucketName + "/" + key;
    return readCsvStmtService.read(s3Url, header, skip);
  }
}
