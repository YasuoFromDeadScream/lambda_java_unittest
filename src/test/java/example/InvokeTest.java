package example;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBScanExpression;
import com.amazonaws.services.dynamodbv2.datamodeling.ScanResultPage;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.lambda.runtime.Context;
import example.pojo.UserInfo;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class InvokeTest extends DynamoDBTestBase{
  private static final Logger logger = LoggerFactory.getLogger(InvokeTest.class);

  @BeforeEach
  protected void init() {
    super.init();
  }

  @AfterEach
  protected void tearDown()  {
    try {
      super.tearDown();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  void invokeTest() throws IOException, ClassNotFoundException {
    logger.info("Invoke TEST");

    insertTestData("sample.xlsx");

    HashMap<String,String> event = new HashMap<String,String>();
    Context context = new TestContext();
    Handler handler = new Handler();
    String result = handler.handleRequest(event, context);

    List<UserInfo> UserInfoList = new ArrayList<>();
    Map<String, AttributeValue> lastKeyEvaluated = null;
    do {
      DynamoDBScanExpression scanExpression = new DynamoDBScanExpression()
              .withExclusiveStartKey(lastKeyEvaluated);
      ScanResultPage<UserInfo> resultResult = dynamoDBMapper.scanPage(UserInfo.class,scanExpression);
      if(resultResult.getCount() > 0){
        UserInfoList.addAll(resultResult.getResults());
      }
      lastKeyEvaluated = resultResult.getLastEvaluatedKey();
    } while (lastKeyEvaluated != null);

   logger.info(UserInfoList.stream().findFirst().toString());
   assertEquals(UserInfoList.get(0).getAge(),22);

    assertTrue(result.contains("200 OK"));
  }

}
