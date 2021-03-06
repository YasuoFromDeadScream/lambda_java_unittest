package example;

import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBIndexHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBIndexRangeKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperFieldModel.DynamoDBAttributeType;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBRangeKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTyped;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteTableRequest;
import com.amazonaws.services.dynamodbv2.model.Projection;
import com.amazonaws.services.dynamodbv2.model.ProjectionType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ResourceInUseException;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;

public abstract class DynamoDBTestBase {

  // todo ???????????????????????????????????????
  private String PACKAGE = "example.pojo.";
  private String REGITON_NAME = "us-east-1";
  private String LOCALHOST = "http://localhost:4566";

  // DynamoDB
  protected AmazonDynamoDB amazonDynamoDB;
  protected DynamoDBMapper dynamoDBMapper;
  protected DynamoDB dynamoDB;
  protected List<Class> pojoList = new ArrayList<>();

  protected void init() {
    this.amazonDynamoDB =
        AmazonDynamoDBClientBuilder.standard()
            // ???????????????
            .withEndpointConfiguration(
                new EndpointConfiguration(LOCALHOST, REGITON_NAME))
            .build();
    this.dynamoDB = new DynamoDB(amazonDynamoDB);
    this.dynamoDBMapper = new DynamoDBMapper(amazonDynamoDB);
  }

  /**
   * @throws Exception
   */
  protected void tearDown() throws Exception {
    removeTable();
  }

  /**
   * Excel??????????????????????????????????????????.
   * @param excelFileName Excel???????????????
   */
  protected void insertTestData(String excelFileName) throws ClassNotFoundException, IOException {
    InputStream in = null;
    Workbook wb = null;

    String path = "/data/" + excelFileName;
    try {
      in = getClass().getResourceAsStream(path);
      wb = WorkbookFactory.create(in);
    } catch (IOException e) {
      System.out.println(e.toString());
    } catch (InvalidFormatException e) {
      System.out.println(e.toString());
    } finally {
      try {
        in.close();
      } catch (IOException e) {
        System.out.println(e.toString());
      }
    }

    for (int i = 0; i < wb.getNumberOfSheets(); i++) {
      Sheet sheet = wb.getSheetAt(i);
      // POJO?????????
      String pojoName = sheet.getSheetName();
      Class<?> pojoClass = null;
      try {
        pojoClass = Class.forName(PACKAGE + pojoName);
      } catch (ClassNotFoundException e) {
        e.printStackTrace();
        throw e;
      }
      pojoList.add(pojoClass);

      // ??????????????????
      createTable(pojoClass);

      // ??????????????????
      insertRecord(pojoClass, sheet);

    }
  }

  /**
   * ??????????????????
   */
  protected void removeTable() {
    pojoList.forEach(
        model -> {
          DeleteTableRequest deleteTableRequest = dynamoDBMapper.generateDeleteTableRequest(model);
          try {
            amazonDynamoDB.deleteTable(deleteTableRequest);
          } catch (Exception e) {
            e.printStackTrace();
          }
        });
  }

  private void createTable(Class<?> pojoClass) {
    // GSI?????????????????????
    CreateTableRequest createTableRequest = dynamoDBMapper.generateCreateTableRequest(pojoClass);
    createTableRequest.setProvisionedThroughput(new ProvisionedThroughput(5L, 1L));
    // GSI?????????
    if (CollectionUtils.isNotEmpty(createTableRequest.getGlobalSecondaryIndexes())) {
      createTableRequest
          .getGlobalSecondaryIndexes()
          .forEach(
              globalSecondaryIndex -> {
                globalSecondaryIndex.setProvisionedThroughput(new ProvisionedThroughput(1L, 1L));
                globalSecondaryIndex.setProjection(
                    new Projection().withProjectionType(ProjectionType.KEYS_ONLY));
              });
    }
    // ??????????????????
    try {
      amazonDynamoDB.createTable(createTableRequest);
    } catch (ResourceInUseException e) {
      e.printStackTrace();
    }
  }

  private void insertRecord(Class<?> pojoClass, Sheet sheet) {
    List<Map<String, String>> mapArrayList = new ArrayList<>();
    for (Iterator<Row> it = sheet.iterator(); it.hasNext(); ) {
      Row row = it.next();
      if (row.getRowNum() == 0) {
        continue;
      }
      System.out.println(row.getRowNum() + "????????????????????????");

      Map<String, String> map = new HashMap<>();
      for (Iterator<Cell> cell = row.iterator(); cell.hasNext(); ) {
        Cell c = cell.next();
        map.put(sheet.getRow(0).getCell(c.getColumnIndex()).toString(), c.toString());
      }
      mapArrayList.add(map);
    }

    for (Map<String, String> map : mapArrayList) {
      Item item = new Item();
      String partitionKey = null;
      String sortKey = null;
      for (Field field : pojoClass.getDeclaredFields()) {
        DynamoDBTyped type = field.getAnnotation(DynamoDBTyped.class);
        DynamoDBHashKey hashKey = field.getAnnotation(DynamoDBHashKey.class);
        if (hashKey != null) {
          partitionKey = hashKey.attributeName();
          continue;
        }
        DynamoDBRangeKey rangeKey = field.getAnnotation(DynamoDBRangeKey.class);
        if (rangeKey != null) {
          sortKey = rangeKey.attributeName();
          continue;
        }
        DynamoDBAttribute attribute = field.getAnnotation(DynamoDBAttribute.class);
        DynamoDBIndexHashKey indexHashKey = field.getAnnotation(DynamoDBIndexHashKey.class);
        DynamoDBIndexRangeKey indexRangeKey = field.getAnnotation(DynamoDBIndexRangeKey.class);
        if (attribute != null) {
          String columnName = attribute.attributeName();
          // ???????????????????????????
          if (StringUtils.isEmpty(map.get(columnName))) {
            if (indexHashKey == null && indexRangeKey == null) {
              // NULL?????????????????????
              item.withNull(columnName);
            } else {
              // GSI???????????????????????????GSI???????????????????????????????????????????????????????????????????????????
              if (type == null || type.value().equals(DynamoDBAttributeType.NULL)) {
                item.withString(columnName, "-");
              } else if (type.value().equals(DynamoDBAttributeType.N)) {
                item.withLong(columnName, 0);
              }
            }
          } else if (type == null || type.value().equals(DynamoDBAttributeType.NULL)) {
            item.withString(columnName, map.get(columnName));
          } else if (type.value().equals(DynamoDBAttributeType.BOOL)) {
            item.withBoolean(columnName, BooleanUtils.toBoolean(map.get(columnName)));
          } else if (type.value().equals(DynamoDBAttributeType.N)) {
            item.withLong(columnName, (long)Double.parseDouble(map.get(columnName)));
          }
        }
      }
      // ??????????????????????????? + ????????????????????????
      if (StringUtils.isNotEmpty(sortKey)) {
        item.withPrimaryKey(partitionKey, map.get(partitionKey), sortKey, map.get(sortKey));
      } else { // ????????????????????????????????????
        item.withPrimaryKey(partitionKey, map.get(partitionKey));
      }
      Table table = dynamoDB.getTable(sheet.getSheetName());
      table.putItem(item);
    }
  }
}
