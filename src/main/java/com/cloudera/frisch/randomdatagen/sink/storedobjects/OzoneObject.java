package com.cloudera.frisch.randomdatagen.sink.storedobjects;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@AllArgsConstructor
@Getter
@Setter
public class OzoneObject {
  String bucket;
  String key;
  String value;
}