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
import de.kherud.llama.InferenceParameters;
import de.kherud.llama.LlamaModel;
import de.kherud.llama.LlamaOutput;
import de.kherud.llama.ModelParameters;
import de.kherud.llama.args.LogFormat;
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

import java.io.FileOutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.SQLException;
import java.util.LinkedList;

@Slf4j
@Getter
public class LocalLLMField extends Field<String> {

  private LlamaModel llamaModel;
  private final String rawRequest;
  private final String context;
  private final Float temperature;
  private final Float frequencyPenalty;
  private final Float presencePenalty;
  private final Float topP;
  private final Integer maxTokens;
  private final LinkedList<ParsingUtils.StringFragment> requestToInject;
  private final ModelParameters modelParams;

  public LocalLLMField(String name, String modelPath, String request,
                       Float temperature, Float frequencyPenalty,
                       Float presencePenalty, Float topP, Integer maxTokens, String context) {
    this.name = name;
    this.file = modelPath;
    this.rawRequest = request;
    this.context = context;
    this.requestToInject = ParsingUtils.parseStringWithVars(request);
    this.temperature = temperature == null ? 1.0f : temperature;
    this.frequencyPenalty = frequencyPenalty == null ? 1.0f : frequencyPenalty;
    this.presencePenalty = presencePenalty == null ? 1.0f : presencePenalty;
    this.topP = topP == null ? 1.0f : topP;
    this.maxTokens = maxTokens == null ? 256 : maxTokens;

    // Log llama logs in debug mode only
    LlamaModel.setLogger(LogFormat.TEXT, (level, message)-> log.debug(message));

    if(modelPath.startsWith("http")) {
      var newModelPath = "/tmp/" + modelPath.substring(modelPath.lastIndexOf("/")+1);
      try {
      URL url = new URL(modelPath);
      HttpURLConnection conn = (HttpURLConnection) url.openConnection();
      if(conn.getResponseCode() != HttpURLConnection.HTTP_OK) {
        log.warn("Cannot connect to: {} due to error: {}", modelPath, conn.getResponseMessage());
      } else {
        log.info("Starting to download model file from: {}.", modelPath);
        var inputStream = conn.getInputStream();
        var outputStream = new FileOutputStream(newModelPath);
        int bytesRead = -1;
        byte[] buffer = new byte[4096];
        while ((bytesRead = inputStream.read(buffer)) != -1) {
          outputStream.write(buffer, 0, bytesRead);
        }
        outputStream.close();
        inputStream.close();
        log.info("Finished to download model, saved locally in: {}", newModelPath);
      }
      } catch (Exception e) {
        log.warn("Could not download model file from: {} ", modelPath, e);
      }

      this.modelParams = new ModelParameters()
          .setModelFilePath(newModelPath)
          .setNThreads(2);
    } else {
      this.modelParams = new ModelParameters()
          .setModelFilePath(modelPath)
          .setNThreads(2);
    }

  }

  @Override
  public void initField() {
    // We should only open model once generation has been launched
    this.llamaModel = new LlamaModel(modelParams);
  }

  @Override
  public void closeField() {
    // We should also close model once done
    this.llamaModel.close();
  }

  @Override
  public String generateComputedValue(Row row) {
    var prompt = "<|system|>\n";
    prompt += this.context;
    prompt += "\n<|user|>";
    prompt += ParsingUtils.injectRowValuesToAString(row, requestToInject);
    prompt += "\n<|assistant|>";
    InferenceParameters inferParams = new InferenceParameters(prompt)
        .setTemperature(this.temperature)
        .setPenalizeNl(true)
        .setCachePrompt(false)
        .setNKeep(0)
        .setFrequencyPenalty(this.frequencyPenalty)
        .setPresencePenalty(this.presencePenalty)
        .setTopP(this.topP);
    var sb = new StringBuilder();
    for(LlamaOutput l: this.llamaModel.generate(inferParams)) {
      if(!l.toString().equalsIgnoreCase("\n")) {
        sb.append(l);
      }
    }
    return sb.toString();
  }

  @Override
  public String generateRandomValue() {
    return "";
  }

    /*
     Override if needed Field function to insert into special connectors
     */

  @Override
  public Put toHbasePut(String value, Put hbasePut) {
    hbasePut.addColumn(Bytes.toBytes(hbaseColumnQualifier), Bytes.toBytes(name),
        Bytes.toBytes(value));
    return hbasePut;
  }

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
}
