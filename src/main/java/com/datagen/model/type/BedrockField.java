/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datagen.model.type;

import com.datagen.model.Row;
import com.datagen.utils.ParsingUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hive.jdbc.HivePreparedStatement;
import org.apache.kudu.Type;
import org.apache.kudu.client.PartialRow;
import org.apache.orc.TypeDescription;
import org.json.JSONException;
import org.json.JSONObject;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.bedrockruntime.BedrockRuntimeClient;

import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;

@Slf4j
public class BedrockField extends Field<String> {

  private final String url;
  private final String user;
  private final String password;
  private final Double temperature;
  private final Integer maxTokens;
  private final Region region;
  private final LinkedList<ParsingUtils.StringFragment> requestToInject;
  private final BedrockRuntimeClient bedrockRuntimeClient;
  private final String modelId;
  private final BedrockModelType bedrockmodeltype;
  private JSONObject preparedRequest = null;

  public BedrockField(String name, String url, String user, String password,
                      String request, String modelType, Float temperature, String region, Integer maxTokens) {
    this.name = name;
    this.url = url;
    this.user = user;
    this.password = password;
    this.temperature = temperature == null ? 0.5 : temperature;
    this.maxTokens = maxTokens == null ? 256 : maxTokens;
    this.requestToInject = ParsingUtils.parseStringWithVars(request);
    this.region = region!=null?Region.of(region):Region.US_EAST_1;

    AwsCredentialsProvider awsCredentialsProvider =
        StaticCredentialsProvider.create(
            AwsBasicCredentials.create(this.user, this.password));

    this.bedrockRuntimeClient = BedrockRuntimeClient.builder()
        .credentialsProvider(awsCredentialsProvider)
        .region(this.region)
        .build();

    // See model Ids available at: https://docs.aws.amazon.com/bedrock/latest/userguide/model-ids.html
    this.modelId = modelType == null ? "amazon.titan-text-lite-v1" : modelType;
    /*
    Tested with
      MISTRAL: mistral.mistral-small-2402-v1:0
      TITAN: amazon.titan-text-lite-v1
      LLAMA: meta.llama3-8b-instruct-v1:0
     */

    this.bedrockmodeltype = switch (modelId.split("\\.")[0]) {
      case "anthropic":
        yield BedrockModelType.ANTHROPIC;
      case "mistral":
        yield BedrockModelType.MISTRAL;
      case "amazon":
        yield BedrockModelType.TITAN;
      case "meta":
        yield BedrockModelType.LLAMA;
      default:
        yield BedrockModelType.TITAN;
    };

    // JSON prepared request for model
    try {
      this.preparedRequest = switch (bedrockmodeltype) {
        case TITAN:
          yield new JSONObject();
        case ANTHROPIC:
          yield new JSONObject()
              .put("temperature", this.temperature)
              .put("stop_sequences", List.of("\n\nHuman:"))
              .put("max_tokens_to_sample", this.maxTokens);
        case MISTRAL:
          yield new JSONObject()
              .put("temperature", this.temperature)
              .put("max_tokens", this.maxTokens);
        case LLAMA:
          yield new JSONObject()
              .put("temperature", this.temperature);
      };
    } catch (JSONException e) {
      log.warn("Could not prepare request to Bedrock due to error: ", e);
    }

  }

  @Override
  public String generateComputedValue(Row row) {
    String stringToEvaluate =
        ParsingUtils.injectRowValuesToAString(row, requestToInject);
    log.debug("Asking to Bedrock: {}", stringToEvaluate);
    var responseText = "";

    try {
      switch (this.bedrockmodeltype) {
      case ANTHROPIC -> preparedRequest.put("prompt",
          "Human: " + stringToEvaluate + "\\n\\nAssistant:");
      case MISTRAL -> preparedRequest.put("prompt",
          "<s>[INST] " + stringToEvaluate + "[/INST]");
      case TITAN -> preparedRequest.put("inputText", stringToEvaluate);
      default -> preparedRequest.put("prompt", stringToEvaluate);
      }

      // Encode and send the request.
      var response = bedrockRuntimeClient.invokeModel(req -> req
          .accept("application/json")
          .contentType("application/json")
          .body(SdkBytes.fromUtf8String(preparedRequest.toString()))
          .modelId(modelId));

      // Extract response
      var responseBody = new JSONObject(response.body().asUtf8String());

      log.debug("Response body from Bedrock: {}", responseBody);

      responseText = switch (this.bedrockmodeltype) {
        case TITAN:
          yield responseBody.getJSONArray("results").getJSONObject(0)
              .getString("outputText");
        case LLAMA:
          yield responseBody.getString("generation");
        case MISTRAL:
          yield responseBody.getJSONArray("outputs").getJSONObject(0)
              .getString("text");
        case ANTHROPIC:
          yield responseBody.getString("completion");
      };

    } catch (JSONException e) {
      log.warn("Cannot insert or decode JSON from/to Bedrock due to error: ",
          e);
    }

    return responseText;
  }

  @Override
  public String generateRandomValue() {
    return "";
  }

  @Override
  public Put toHbasePut(String value, Put hbasePut) {
    hbasePut.addColumn(Bytes.toBytes(hbaseColumnQualifier), Bytes.toBytes(name),
        Bytes.toBytes(value));
    return hbasePut;
  }

    /*
     Override if needed Field function to insert into special connectors
     */

  @Override
  public PartialRow toKudu(String value, PartialRow partialRow) {
    partialRow.addString(name, value);
    return partialRow;
  }

  @Override
  public Type getKuduType() {
    return Type.STRING;
  }

  @Override
  public HivePreparedStatement toHive(String value, int index,
                                      HivePreparedStatement hivePreparedStatement) {
    try {
      hivePreparedStatement.setString(index, value);
    } catch (SQLException e) {
      log.warn("Could not set value : " + value.toString() +
          " into hive statement due to error :", e);
    }
    return hivePreparedStatement;
  }

  @Override
  public String getHiveType() {
    return "STRING";
  }

  @Override
  public String getGenericRecordType() {
    return "string";
  }

  @Override
  public ColumnVector getOrcColumnVector(VectorizedRowBatch batch, int cols) {
    return batch.cols[cols];
  }

  @Override
  public TypeDescription getTypeDescriptionOrc() {
    return TypeDescription.createString();
  }

  private enum BedrockModelType {
    ANTHROPIC,
    TITAN,
    MISTRAL,
    LLAMA
  }
}
