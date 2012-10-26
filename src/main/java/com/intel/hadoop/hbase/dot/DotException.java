/**
 * Copyright 2007 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.intel.hadoop.hbase.dot;

/**
 * Standard exception prompts for DOT
 *
 */
public interface DotException {
  public static final String INVALID_DOT_TYPE = "Invalid DOT Type";
  public static final String ILLEGAL_DOT_OPRATION = "Illegal DOT Operation";
  public static final String DOC_ELEMENTS_EMPTY = "No DOC element";
  public static final String DOC_SCHEMA_EMPTY = "No schema definition";
  public static final String MISSING_FILTER_CANDIDATE = "No filter candiate";
}
