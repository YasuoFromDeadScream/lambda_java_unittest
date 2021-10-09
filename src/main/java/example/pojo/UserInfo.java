package example.pojo;

import com.amazonaws.services.dynamodbv2.datamodeling.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@DynamoDBTable(tableName = "UserInfo")
public class UserInfo {

    @DynamoDBHashKey(attributeName = "UserId")
    @DynamoDBIndexRangeKey(
            globalSecondaryIndexName = "UserInfoIndex1")
    private String userId;

    @DynamoDBRangeKey(attributeName = "UserName")
    private String userName;

    @DynamoDBIndexHashKey(
            globalSecondaryIndexName = "UserInfoIndex1")
    @DynamoDBAttribute(
            attributeName = "Address")
    private String address;

    @DynamoDBAttribute(
            attributeName = "Age")
    @DynamoDBTyped(DynamoDBMapperFieldModel.DynamoDBAttributeType.N)
    private Integer age;

    @DynamoDBAttribute(
            attributeName = "IsDelete")
    @DynamoDBTyped(DynamoDBMapperFieldModel.DynamoDBAttributeType.BOOL)
    private Boolean isDelete;
}
