/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.mllib.pmml.export

import scala.{Array => SArray}

import org.dmg.pmml._

import org.apache.spark.mllib.regression.GeneralizedLinearModel

/**
 * PMML Model Export for GeneralizedLinearModel class with binary ClassificationModel
 */
private[mllib] class BinaryClassificationPMMLModelExport(
                                                          model: GeneralizedLinearModel,
                                                          description: String,
                                                          normalizationMethod: RegressionNormalizationMethodType,
                                                          threshold: Double)
  extends PMMLModelExport {

  populateBinaryClassificationPMML()

  /**
   * Export the input LogisticRegressionModel or SVMModel to PMML format.
   */
  private def populateBinaryClassificationPMML(): Unit = {
    pmml.getHeader.setDescription(description)
    println("PMMLModelExport Error")
  }
}

case class RegressionNormalizationMethodType

