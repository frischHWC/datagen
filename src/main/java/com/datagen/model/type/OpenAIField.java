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
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hive.jdbc.HivePreparedStatement;
import org.apache.kudu.Type;
import org.apache.kudu.client.PartialRow;
import org.apache.orc.TypeDescription;
import org.springframework.ai.chat.messages.SystemMessage;
import org.springframework.ai.chat.messages.UserMessage;
import org.springframework.ai.chat.prompt.Prompt;
import org.springframework.ai.openai.OpenAiChatClient;
import org.springframework.ai.openai.OpenAiChatOptions;
import org.springframework.ai.openai.api.OpenAiApi;

import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;

@Slf4j
@Getter
public class OpenAIField extends Field<String> {

  private final String rawRequest;
  private final String context;
  private final String url;
  private final String user;
  private final String password;
  private final Float temperature;
  private final Float frequencyPenalty;
  private final Float presencePenalty;
  private final Float topP;
  private final Integer maxTokens;

  private final LinkedList<ParsingUtils.StringFragment> requestToInject;
  private final OpenAiApi openAiApi;
  private final OpenAiChatClient openAiChatClient;
  private final OpenAiChatOptions openAiChatOptions;
  private final String modelType;
  private final SystemMessage systemMessage;

  public OpenAIField(String name, String url, String user, String password,
                     String request, String modelType, Float temperature, Float frequencyPenalty,
                     Float presencePenalty, Integer maxTokens, Float topP, String context) {
    this.name = name;
    this.url = url;
    this.user = user;
    this.password = password;
    this.rawRequest = request;
    this.context = context;
    this.requestToInject = ParsingUtils.parseStringWithVars(request);

    // See model Ids available at:
    this.modelType = modelType == null ? "gpt-4-32k" : modelType;

    this.temperature = temperature == null ? 1.0f : temperature;
    this.frequencyPenalty = frequencyPenalty == null ? 1.0f : frequencyPenalty;
    this.presencePenalty = presencePenalty == null ? 1.0f : presencePenalty;
    this.topP = topP == null ? 1.0f : topP;
    this.maxTokens = maxTokens == null ? 256 : maxTokens;

    this.openAiApi = new OpenAiApi(this.password);
    this.openAiChatOptions = OpenAiChatOptions.builder()
        .withModel(this.modelType)
        .withTemperature(this.temperature)
        .withFrequencyPenalty(this.frequencyPenalty)
        .withPresencePenalty(this.presencePenalty)
        .withMaxTokens(this.maxTokens)
        .withTopP(this.topP)
        .build();
    this.openAiChatClient = new OpenAiChatClient(openAiApi, openAiChatOptions);

    var contextAsMessage = context!=null?"Use the following information to answer the question:"+System.lineSeparator()+context:"";
    this.systemMessage = new SystemMessage("Generate only the answer."+System.lineSeparator()+contextAsMessage);
    log.debug("Will provide following System information to the model: {}", systemMessage.getContent());

  }

  @Override
  public String generateComputedValue(Row row) {
    String stringToEvaluate =
        ParsingUtils.injectRowValuesToAString(row, requestToInject);
    log.debug("Asking to OpenAI: {}", stringToEvaluate);
    UserMessage userMessage = new UserMessage(stringToEvaluate);

    return openAiChatClient.call(new Prompt(
        List.of(userMessage, this.systemMessage),
        this.openAiChatOptions
    )).getResult().getOutput().getContent()
        .trim().replaceAll("\\n[ \\t]*\\n","");
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
