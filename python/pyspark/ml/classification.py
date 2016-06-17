#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import warnings

from pyspark import since
from pyspark.ml.util import *
from pyspark.ml.wrapper import JavaEstimator, JavaModel
from pyspark.ml.param import TypeConverters
from pyspark.ml.param.shared import *
from pyspark.ml.regression import (
    RandomForestParams, TreeEnsembleParams, DecisionTreeModel, TreeEnsembleModels)
from pyspark.mllib.common import inherit_doc


__all__ = ['LogisticRegression', 'LogisticRegressionModel',
           'DecisionTreeClassifier', 'DecisionTreeClassificationModel',
           'GBTClassifier', 'GBTClassificationModel',
           'RandomForestClassifier', 'RandomForestClassificationModel',
           'NaiveBayes', 'NaiveBayesModel',
           'MultilayerPerceptronClassifier', 'MultilayerPerceptronClassificationModel']


@inherit_doc
class LogisticRegression(JavaEstimator, HasFeaturesCol, HasLabelCol, HasPredictionCol, HasMaxIter,
                         HasRegParam, HasTol, HasProbabilityCol, HasRawPredictionCol,
                         HasElasticNetParam, HasFitIntercept, HasStandardization, HasThresholds,
                         HasWeightCol, JavaMLWritable, JavaMLReadable):
    """
    Logistic regression.
    Currently, this class only supports binary classification.

    >>> from pyspark.sql import Row
    >>> from pyspark.mllib.linalg import Vectors
    >>> df = sc.parallelize([
    ...     Row(label=1.0, weight=2.0, features=Vectors.dense(1.0)),
    ...     Row(label=0.0, weight=2.0, features=Vectors.sparse(1, [], []))]).toDF()
    >>> lr = LogisticRegression(maxIter=5, regParam=0.01, weightCol="weight")
    >>> model = lr.fit(df)
    >>> model.coefficients
    DenseVector([5.5...])
    >>> model.intercept
    -2.68...
    >>> test0 = sc.parallelize([Row(features=Vectors.dense(-1.0))]).toDF()
    >>> result = model.transform(test0).head()
    >>> result.prediction
    0.0
    >>> result.probability
    DenseVector([0.99..., 0.00...])
    >>> result.rawPrediction
    DenseVector([8.22..., -8.22...])
    >>> test1 = sc.parallelize([Row(features=Vectors.sparse(1, [0], [1.0]))]).toDF()
    >>> model.transform(test1).head().prediction
    1.0
    >>> lr.setParams("vector")
    Traceback (most recent call last):
        ...
    TypeError: Method setParams forces keyword arguments.
    >>> lr_path = temp_path + "/lr"
    >>> lr.save(lr_path)
    >>> lr2 = LogisticRegression.load(lr_path)
    >>> lr2.getMaxIter()
    5
    >>> model_path = temp_path + "/lr_model"
    >>> model.save(model_path)
    >>> model2 = LogisticRegressionModel.load(model_path)
    >>> model.coefficients[0] == model2.coefficients[0]
    True
    >>> model.intercept == model2.intercept
    True

    .. versionadded:: 1.3.0
    """

    threshold = Param(Params._dummy(), "threshold",
                      "Threshold in binary classification prediction, in range [0, 1]." +
                      " If threshold and thresholds are both set, they must match.",
                      typeConverter=TypeConverters.toFloat)

    @keyword_only
    def __init__(self, featuresCol="features", labelCol="label", predictionCol="prediction",
                 maxIter=100, regParam=0.0, elasticNetParam=0.0, tol=1e-6, fitIntercept=True,
                 threshold=0.5, thresholds=None, probabilityCol="probability",
                 rawPredictionCol="rawPrediction", standardization=True, weightCol=None):
        """
        __init__(self, featuresCol="features", labelCol="label", predictionCol="prediction", \
                 maxIter=100, regParam=0.0, elasticNetParam=0.0, tol=1e-6, fitIntercept=True, \
                 threshold=0.5, thresholds=None, probabilityCol="probability", \
                 rawPredictionCol="rawPrediction", standardization=True, weightCol=None)
        If the threshold and thresholds Params are both set, they must be equivalent.
        """
        super(LogisticRegression, self).__init__()
        self._java_obj = self._new_java_obj(
            "org.apache.spark.ml.classification.LogisticRegression", self.uid)
        self._setDefault(maxIter=100, regParam=0.0, tol=1E-6, threshold=0.5)
        kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)
        self._checkThresholdConsistency()

    @keyword_only
    @since("1.3.0")
    def setParams(self, featuresCol="features", labelCol="label", predictionCol="prediction",
                  maxIter=100, regParam=0.0, elasticNetParam=0.0, tol=1e-6, fitIntercept=True,
                  threshold=0.5, thresholds=None, probabilityCol="probability",
                  rawPredictionCol="rawPrediction", standardization=True, weightCol=None):
        """
        setParams(self, featuresCol="features", labelCol="label", predictionCol="prediction", \
                  maxIter=100, regParam=0.0, elasticNetParam=0.0, tol=1e-6, fitIntercept=True, \
                  threshold=0.5, thresholds=None, probabilityCol="probability", \
                  rawPredictionCol="rawPrediction", standardization=True, weightCol=None)
        Sets params for logistic regression.
        If the threshold and thresholds Params are both set, they must be equivalent.
        """
        kwargs = self.setParams._input_kwargs
        self._set(**kwargs)
        self._checkThresholdConsistency()
        return self

    def _create_model(self, java_model):
        return LogisticRegressionModel(java_model)

    @since("1.4.0")
    def setThreshold(self, value):
        """
        Sets the value of :py:attr:`threshold`.
        Clears value of :py:attr:`thresholds` if it has been set.
        """
        self._paramMap[self.threshold] = value
        if self.isSet(self.thresholds):
            del self._paramMap[self.thresholds]
        return self

    @since("1.4.0")
    def getThreshold(self):
        """
        Gets the value of threshold or its default value.
        """
        self._checkThresholdConsistency()
        if self.isSet(self.thresholds):
            ts = self.getOrDefault(self.thresholds)
            if len(ts) != 2:
                raise ValueError("Logistic Regression getThreshold only applies to" +
                                 " binary classification, but thresholds has length != 2." +
                                 "  thresholds: " + ",".join(ts))
            return 1.0/(1.0 + ts[0]/ts[1])
        else:
            return self.getOrDefault(self.threshold)

    @since("1.5.0")
    def setThresholds(self, value):
        """
        Sets the value of :py:attr:`thresholds`.
        Clears value of :py:attr:`threshold` if it has been set.
        """
        self._paramMap[self.thresholds] = value
        if self.isSet(self.threshold):
            del self._paramMap[self.threshold]
        return self

    @since("1.5.0")
    def getThresholds(self):
        """
        If :py:attr:`thresholds` is set, return its value.
        Otherwise, if :py:attr:`threshold` is set, return the equivalent thresholds for binary
        classification: (1-threshold, threshold).
        If neither are set, throw an error.
        """
        self._checkThresholdConsistency()
        if not self.isSet(self.thresholds) and self.isSet(self.threshold):
            t = self.getOrDefault(self.threshold)
            return [1.0-t, t]
        else:
            return self.getOrDefault(self.thresholds)

    def _checkThresholdConsistency(self):
        if self.isSet(self.threshold) and self.isSet(self.thresholds):
            ts = self.getParam(self.thresholds)
            if len(ts) != 2:
                raise ValueError("Logistic Regression getThreshold only applies to" +
                                 " binary classification, but thresholds has length != 2." +
                                 " thresholds: " + ",".join(ts))
            t = 1.0/(1.0 + ts[0]/ts[1])
            t2 = self.getParam(self.threshold)
            if abs(t2 - t) >= 1E-5:
                raise ValueError("Logistic Regression getThreshold found inconsistent values for" +
                                 " threshold (%g) and thresholds (equivalent to %g)" % (t2, t))


class LogisticRegressionModel(JavaModel, JavaMLWritable, JavaMLReadable):
    """
    Model fitted by LogisticRegression.

    .. versionadded:: 1.3.0
    """

    @property
    @since("1.4.0")
    def weights(self):
        """
        Model weights.
        """

        warnings.warn("weights is deprecated. Use coefficients instead.")
        return self._call_java("weights")

    @property
    @since("1.6.0")
    def coefficients(self):
        """
        Model coefficients.
        """
        return self._call_java("coefficients")

    @property
    @since("1.4.0")
    def intercept(self):
        """
        Model intercept.
        """
        return self._call_java("intercept")


class TreeClassifierParams(object):
    """
    Private class to track supported impurity measures.

    .. versionadded:: 1.4.0
    """
    supportedImpurities = ["entropy", "gini"]

    impurity = Param(Params._dummy(), "impurity",
                     "Criterion used for information gain calculation (case-insensitive). " +
                     "Supported options: " +
                     ", ".join(supportedImpurities), typeConverter=TypeConverters.toString)

    def __init__(self):
        super(TreeClassifierParams, self).__init__()

    @since("1.6.0")
    def setImpurity(self, value):
        """
        Sets the value of :py:attr:`impurity`.
        """
        self._paramMap[self.impurity] = value
        return self

    @since("1.6.0")
    def getImpurity(self):
        """
        Gets the value of impurity or its default value.
        """
        return self.getOrDefault(self.impurity)


class GBTParams(TreeEnsembleParams):
    """
    Private class to track supported GBT params.

    .. versionadded:: 1.4.0
    """
    supportedLossTypes = ["logistic"]


@inherit_doc
class DecisionTreeClassifier(JavaEstimator, HasFeaturesCol, HasLabelCol, HasPredictionCol,
                             HasProbabilityCol, HasRawPredictionCol, DecisionTreeParams,
                             TreeClassifierParams, HasCheckpointInterval, HasSeed, JavaMLWritable,
                             JavaMLReadable):
    """
    `http://en.wikipedia.org/wiki/Decision_tree_learning Decision tree`
    learning algorithm for classification.
    It supports both binary and multiclass labels, as well as both continuous and categorical
    features.

    >>> from pyspark.mllib.linalg import Vectors
    >>> from pyspark.ml.feature import StringIndexer
    >>> df = sqlContext.createDataFrame([
    ...     (1.0, Vectors.dense(1.0)),
    ...     (0.0, Vectors.sparse(1, [], []))], ["label", "features"])
    >>> stringIndexer = StringIndexer(inputCol="label", outputCol="indexed")
    >>> si_model = stringIndexer.fit(df)
    >>> td = si_model.transform(df)
    >>> dt = DecisionTreeClassifier(maxDepth=2, labelCol="indexed")
    >>> model = dt.fit(td)
    >>> model.numNodes
    3
    >>> model.depth
    1
    >>> model.featureImportances
    SparseVector(1, {0: 1.0})
    >>> test0 = sqlContext.createDataFrame([(Vectors.dense(-1.0),)], ["features"])
    >>> result = model.transform(test0).head()
    >>> result.prediction
    0.0
    >>> result.probability
    DenseVector([1.0, 0.0])
    >>> result.rawPrediction
    DenseVector([1.0, 0.0])
    >>> test1 = sqlContext.createDataFrame([(Vectors.sparse(1, [0], [1.0]),)], ["features"])
    >>> model.transform(test1).head().prediction
    1.0

    >>> dtc_path = temp_path + "/dtc"
    >>> dt.save(dtc_path)
    >>> dt2 = DecisionTreeClassifier.load(dtc_path)
    >>> dt2.getMaxDepth()
    2
    >>> model_path = temp_path + "/dtc_model"
    >>> model.save(model_path)
    >>> model2 = DecisionTreeClassificationModel.load(model_path)
    >>> model.featureImportances == model2.featureImportances
    True

    .. versionadded:: 1.4.0
    """

    @keyword_only
    def __init__(self, featuresCol="features", labelCol="label", predictionCol="prediction",
                 probabilityCol="probability", rawPredictionCol="rawPrediction",
                 maxDepth=5, maxBins=32, minInstancesPerNode=1, minInfoGain=0.0,
                 maxMemoryInMB=256, cacheNodeIds=False, checkpointInterval=10, impurity="gini",
                 seed=None):
        """
        __init__(self, featuresCol="features", labelCol="label", predictionCol="prediction", \
                 probabilityCol="probability", rawPredictionCol="rawPrediction", \
                 maxDepth=5, maxBins=32, minInstancesPerNode=1, minInfoGain=0.0, \
                 maxMemoryInMB=256, cacheNodeIds=False, checkpointInterval=10, impurity="gini", \
                 seed=None)
        """
        super(DecisionTreeClassifier, self).__init__()
        self._java_obj = self._new_java_obj(
            "org.apache.spark.ml.classification.DecisionTreeClassifier", self.uid)
        self._setDefault(maxDepth=5, maxBins=32, minInstancesPerNode=1, minInfoGain=0.0,
                         maxMemoryInMB=256, cacheNodeIds=False, checkpointInterval=10,
                         impurity="gini")
        kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    @since("1.4.0")
    def setParams(self, featuresCol="features", labelCol="label", predictionCol="prediction",
                  probabilityCol="probability", rawPredictionCol="rawPrediction",
                  maxDepth=5, maxBins=32, minInstancesPerNode=1, minInfoGain=0.0,
                  maxMemoryInMB=256, cacheNodeIds=False, checkpointInterval=10,
                  impurity="gini", seed=None):
        """
        setParams(self, featuresCol="features", labelCol="label", predictionCol="prediction", \
                  probabilityCol="probability", rawPredictionCol="rawPrediction", \
                  maxDepth=5, maxBins=32, minInstancesPerNode=1, minInfoGain=0.0, \
                  maxMemoryInMB=256, cacheNodeIds=False, checkpointInterval=10, impurity="gini", \
                  seed=None)
        Sets params for the DecisionTreeClassifier.
        """
        kwargs = self.setParams._input_kwargs
        return self._set(**kwargs)

    def _create_model(self, java_model):
        return DecisionTreeClassificationModel(java_model)


@inherit_doc
class DecisionTreeClassificationModel(DecisionTreeModel, JavaMLWritable, JavaMLReadable):
    """
    Model fitted by DecisionTreeClassifier.

    .. versionadded:: 1.4.0
    """

    @property
    @since("2.0.0")
    def featureImportances(self):
        """
        Estimate of the importance of each feature.

        This generalizes the idea of "Gini" importance to other losses,
        following the explanation of Gini importance from "Random Forests" documentation
        by Leo Breiman and Adele Cutler, and following the implementation from scikit-learn.

        This feature importance is calculated as follows:
          - importance(feature j) = sum (over nodes which split on feature j) of the gain,
            where gain is scaled by the number of instances passing through node
          - Normalize importances for tree to sum to 1.

        Note: Feature importance for single decision trees can have high variance due to
              correlated predictor variables. Consider using a :py:class:`RandomForestClassifier`
              to determine feature importance instead.
        """
        return self._call_java("featureImportances")


@inherit_doc
class RandomForestClassifier(JavaEstimator, HasFeaturesCol, HasLabelCol, HasPredictionCol, HasSeed,
                             HasRawPredictionCol, HasProbabilityCol,
                             RandomForestParams, TreeClassifierParams, HasCheckpointInterval):
    """
    `http://en.wikipedia.org/wiki/Random_forest  Random Forest`
    learning algorithm for classification.
    It supports both binary and multiclass labels, as well as both continuous and categorical
    features.

    >>> import numpy
    >>> from numpy import allclose
    >>> from pyspark.mllib.linalg import Vectors
    >>> from pyspark.ml.feature import StringIndexer
    >>> df = sqlContext.createDataFrame([
    ...     (1.0, Vectors.dense(1.0)),
    ...     (0.0, Vectors.sparse(1, [], []))], ["label", "features"])
    >>> stringIndexer = StringIndexer(inputCol="label", outputCol="indexed")
    >>> si_model = stringIndexer.fit(df)
    >>> td = si_model.transform(df)
    >>> rf = RandomForestClassifier(numTrees=3, maxDepth=2, labelCol="indexed", seed=42)
    >>> model = rf.fit(td)
    >>> model.featureImportances
    SparseVector(1, {0: 1.0})
    >>> allclose(model.treeWeights, [1.0, 1.0, 1.0])
    True
    >>> test0 = sqlContext.createDataFrame([(Vectors.dense(-1.0),)], ["features"])
    >>> result = model.transform(test0).head()
    >>> result.prediction
    0.0
    >>> numpy.argmax(result.probability)
    0
    >>> numpy.argmax(result.rawPrediction)
    0
    >>> test1 = sqlContext.createDataFrame([(Vectors.sparse(1, [0], [1.0]),)], ["features"])
    >>> model.transform(test1).head().prediction
    1.0

    .. versionadded:: 1.4.0
    """

    @keyword_only
    def __init__(self, featuresCol="features", labelCol="label", predictionCol="prediction",
                 probabilityCol="probability", rawPredictionCol="rawPrediction",
                 maxDepth=5, maxBins=32, minInstancesPerNode=1, minInfoGain=0.0,
                 maxMemoryInMB=256, cacheNodeIds=False, checkpointInterval=10, impurity="gini",
                 numTrees=20, featureSubsetStrategy="auto", seed=None):
        """
        __init__(self, featuresCol="features", labelCol="label", predictionCol="prediction", \
                 probabilityCol="probability", rawPredictionCol="rawPrediction", \
                 maxDepth=5, maxBins=32, minInstancesPerNode=1, minInfoGain=0.0, \
                 maxMemoryInMB=256, cacheNodeIds=False, checkpointInterval=10, impurity="gini", \
                 numTrees=20, featureSubsetStrategy="auto", seed=None)
        """
        super(RandomForestClassifier, self).__init__()
        self._java_obj = self._new_java_obj(
            "org.apache.spark.ml.classification.RandomForestClassifier", self.uid)
        self._setDefault(maxDepth=5, maxBins=32, minInstancesPerNode=1, minInfoGain=0.0,
                         maxMemoryInMB=256, cacheNodeIds=False, checkpointInterval=10, seed=None,
                         impurity="gini", numTrees=20, featureSubsetStrategy="auto")
        kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    @since("1.4.0")
    def setParams(self, featuresCol="features", labelCol="label", predictionCol="prediction",
                  probabilityCol="probability", rawPredictionCol="rawPrediction",
                  maxDepth=5, maxBins=32, minInstancesPerNode=1, minInfoGain=0.0,
                  maxMemoryInMB=256, cacheNodeIds=False, checkpointInterval=10, seed=None,
                  impurity="gini", numTrees=20, featureSubsetStrategy="auto"):
        """
        setParams(self, featuresCol="features", labelCol="label", predictionCol="prediction", \
                 probabilityCol="probability", rawPredictionCol="rawPrediction", \
                  maxDepth=5, maxBins=32, minInstancesPerNode=1, minInfoGain=0.0, \
                  maxMemoryInMB=256, cacheNodeIds=False, checkpointInterval=10, seed=None, \
                  impurity="gini", numTrees=20, featureSubsetStrategy="auto")
        Sets params for linear classification.
        """
        kwargs = self.setParams._input_kwargs
        return self._set(**kwargs)

    def _create_model(self, java_model):
        return RandomForestClassificationModel(java_model)


class RandomForestClassificationModel(TreeEnsembleModels):
    """
    Model fitted by RandomForestClassifier.

    .. versionadded:: 1.4.0
    """

    @property
    @since("2.0.0")
    def featureImportances(self):
        """
        Estimate of the importance of each feature.

        Each feature's importance is the average of its importance across all trees in the ensemble
        The importance vector is normalized to sum to 1. This method is suggested by Hastie et al.
        (Hastie, Tibshirani, Friedman. "The Elements of Statistical Learning, 2nd Edition." 2001.)
        and follows the implementation from scikit-learn.

        .. seealso:: :py:attr:`DecisionTreeClassificationModel.featureImportances`
        """
        return self._call_java("featureImportances")


@inherit_doc
class GBTClassifier(JavaEstimator, HasFeaturesCol, HasLabelCol, HasPredictionCol, HasMaxIter,
                    GBTParams, HasCheckpointInterval, HasStepSize, HasSeed):
    """
    `http://en.wikipedia.org/wiki/Gradient_boosting Gradient-Boosted Trees (GBTs)`
    learning algorithm for classification.
    It supports binary labels, as well as both continuous and categorical features.
    Note: Multiclass labels are not currently supported.

    >>> from numpy import allclose
    >>> from pyspark.mllib.linalg import Vectors
    >>> from pyspark.ml.feature import StringIndexer
    >>> df = sqlContext.createDataFrame([
    ...     (1.0, Vectors.dense(1.0)),
    ...     (0.0, Vectors.sparse(1, [], []))], ["label", "features"])
    >>> stringIndexer = StringIndexer(inputCol="label", outputCol="indexed")
    >>> si_model = stringIndexer.fit(df)
    >>> td = si_model.transform(df)
    >>> gbt = GBTClassifier(maxIter=5, maxDepth=2, labelCol="indexed", seed=42)
    >>> model = gbt.fit(td)
    >>> model.featureImportances
    SparseVector(1, {0: 1.0})
    >>> allclose(model.treeWeights, [1.0, 0.1, 0.1, 0.1, 0.1])
    True
    >>> test0 = sqlContext.createDataFrame([(Vectors.dense(-1.0),)], ["features"])
    >>> model.transform(test0).head().prediction
    0.0
    >>> test1 = sqlContext.createDataFrame([(Vectors.sparse(1, [0], [1.0]),)], ["features"])
    >>> model.transform(test1).head().prediction
    1.0

    .. versionadded:: 1.4.0
    """

    lossType = Param(Params._dummy(), "lossType",
                     "Loss function which GBT tries to minimize (case-insensitive). " +
                     "Supported options: " + ", ".join(GBTParams.supportedLossTypes),
                     typeConverter=TypeConverters.toString)

    @keyword_only
    def __init__(self, featuresCol="features", labelCol="label", predictionCol="prediction",
                 maxDepth=5, maxBins=32, minInstancesPerNode=1, minInfoGain=0.0,
                 maxMemoryInMB=256, cacheNodeIds=False, checkpointInterval=10, lossType="logistic",
                 maxIter=20, stepSize=0.1, seed=None):
        """
        __init__(self, featuresCol="features", labelCol="label", predictionCol="prediction", \
                 maxDepth=5, maxBins=32, minInstancesPerNode=1, minInfoGain=0.0, \
                 maxMemoryInMB=256, cacheNodeIds=False, checkpointInterval=10, \
                 lossType="logistic", maxIter=20, stepSize=0.1, seed=None)
        """
        super(GBTClassifier, self).__init__()
        self._java_obj = self._new_java_obj(
            "org.apache.spark.ml.classification.GBTClassifier", self.uid)
        self._setDefault(maxDepth=5, maxBins=32, minInstancesPerNode=1, minInfoGain=0.0,
                         maxMemoryInMB=256, cacheNodeIds=False, checkpointInterval=10,
                         lossType="logistic", maxIter=20, stepSize=0.1, seed=None)
        kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    @since("1.4.0")
    def setParams(self, featuresCol="features", labelCol="label", predictionCol="prediction",
                  maxDepth=5, maxBins=32, minInstancesPerNode=1, minInfoGain=0.0,
                  maxMemoryInMB=256, cacheNodeIds=False, checkpointInterval=10,
                  lossType="logistic", maxIter=20, stepSize=0.1, seed=None):
        """
        setParams(self, featuresCol="features", labelCol="label", predictionCol="prediction", \
                  maxDepth=5, maxBins=32, minInstancesPerNode=1, minInfoGain=0.0, \
                  maxMemoryInMB=256, cacheNodeIds=False, checkpointInterval=10, \
                  lossType="logistic", maxIter=20, stepSize=0.1, seed=None)
        Sets params for Gradient Boosted Tree Classification.
        """
        kwargs = self.setParams._input_kwargs
        return self._set(**kwargs)

    def _create_model(self, java_model):
        return GBTClassificationModel(java_model)

    @since("1.4.0")
    def setLossType(self, value):
        """
        Sets the value of :py:attr:`lossType`.
        """
        self._paramMap[self.lossType] = value
        return self

    @since("1.4.0")
    def getLossType(self):
        """
        Gets the value of lossType or its default value.
        """
        return self.getOrDefault(self.lossType)


class GBTClassificationModel(TreeEnsembleModels):
    """
    Model fitted by GBTClassifier.

    .. versionadded:: 1.4.0
    """

    @property
    @since("2.0.0")
    def featureImportances(self):
        """
        Estimate of the importance of each feature.

        Each feature's importance is the average of its importance across all trees in the ensemble
        The importance vector is normalized to sum to 1. This method is suggested by Hastie et al.
        (Hastie, Tibshirani, Friedman. "The Elements of Statistical Learning, 2nd Edition." 2001.)
        and follows the implementation from scikit-learn.

        .. seealso:: :py:attr:`DecisionTreeClassificationModel.featureImportances`
        """
        return self._call_java("featureImportances")


@inherit_doc
class NaiveBayes(JavaEstimator, HasFeaturesCol, HasLabelCol, HasPredictionCol, HasProbabilityCol,
                 HasRawPredictionCol, JavaMLWritable, JavaMLReadable):
    """
    Naive Bayes Classifiers.
    It supports both Multinomial and Bernoulli NB. Multinomial NB
    (`http://nlp.stanford.edu/IR-book/html/htmledition/naive-bayes-text-classification-1.html`)
    can handle finitely supported discrete data. For example, by converting documents into
    TF-IDF vectors, it can be used for document classification. By making every vector a
    binary (0/1) data, it can also be used as Bernoulli NB
    (`http://nlp.stanford.edu/IR-book/html/htmledition/the-bernoulli-model-1.html`).
    The input feature values must be nonnegative.

    >>> from pyspark.sql import Row
    >>> from pyspark.mllib.linalg import Vectors
    >>> df = sqlContext.createDataFrame([
    ...     Row(label=0.0, features=Vectors.dense([0.0, 0.0])),
    ...     Row(label=0.0, features=Vectors.dense([0.0, 1.0])),
    ...     Row(label=1.0, features=Vectors.dense([1.0, 0.0]))])
    >>> nb = NaiveBayes(smoothing=1.0, modelType="multinomial")
    >>> model = nb.fit(df)
    >>> model.pi
    DenseVector([-0.51..., -0.91...])
    >>> model.theta
    DenseMatrix(2, 2, [-1.09..., -0.40..., -0.40..., -1.09...], 1)
    >>> test0 = sc.parallelize([Row(features=Vectors.dense([1.0, 0.0]))]).toDF()
    >>> result = model.transform(test0).head()
    >>> result.prediction
    1.0
    >>> result.probability
    DenseVector([0.42..., 0.57...])
    >>> result.rawPrediction
    DenseVector([-1.60..., -1.32...])
    >>> test1 = sc.parallelize([Row(features=Vectors.sparse(2, [0], [1.0]))]).toDF()
    >>> model.transform(test1).head().prediction
    1.0
    >>> nb_path = temp_path + "/nb"
    >>> nb.save(nb_path)
    >>> nb2 = NaiveBayes.load(nb_path)
    >>> nb2.getSmoothing()
    1.0
    >>> model_path = temp_path + "/nb_model"
    >>> model.save(model_path)
    >>> model2 = NaiveBayesModel.load(model_path)
    >>> model.pi == model2.pi
    True
    >>> model.theta == model2.theta
    True

    .. versionadded:: 1.5.0
    """

    smoothing = Param(Params._dummy(), "smoothing", "The smoothing parameter, should be >= 0, " +
                      "default is 1.0", typeConverter=TypeConverters.toFloat)
    modelType = Param(Params._dummy(), "modelType", "The model type which is a string " +
                      "(case-sensitive). Supported options: multinomial (default) and bernoulli.",
                      typeConverter=TypeConverters.toString)

    @keyword_only
    def __init__(self, featuresCol="features", labelCol="label", predictionCol="prediction",
                 probabilityCol="probability", rawPredictionCol="rawPrediction", smoothing=1.0,
                 modelType="multinomial"):
        """
        __init__(self, featuresCol="features", labelCol="label", predictionCol="prediction", \
                 probabilityCol="probability", rawPredictionCol="rawPrediction", smoothing=1.0, \
                 modelType="multinomial")
        """
        super(NaiveBayes, self).__init__()
        self._java_obj = self._new_java_obj(
            "org.apache.spark.ml.classification.NaiveBayes", self.uid)
        self._setDefault(smoothing=1.0, modelType="multinomial")
        kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    @since("1.5.0")
    def setParams(self, featuresCol="features", labelCol="label", predictionCol="prediction",
                  probabilityCol="probability", rawPredictionCol="rawPrediction", smoothing=1.0,
                  modelType="multinomial"):
        """
        setParams(self, featuresCol="features", labelCol="label", predictionCol="prediction", \
                  probabilityCol="probability", rawPredictionCol="rawPrediction", smoothing=1.0, \
                  modelType="multinomial")
        Sets params for Naive Bayes.
        """
        kwargs = self.setParams._input_kwargs
        return self._set(**kwargs)

    def _create_model(self, java_model):
        return NaiveBayesModel(java_model)

    @since("1.5.0")
    def setSmoothing(self, value):
        """
        Sets the value of :py:attr:`smoothing`.
        """
        self._paramMap[self.smoothing] = value
        return self

    @since("1.5.0")
    def getSmoothing(self):
        """
        Gets the value of smoothing or its default value.
        """
        return self.getOrDefault(self.smoothing)

    @since("1.5.0")
    def setModelType(self, value):
        """
        Sets the value of :py:attr:`modelType`.
        """
        self._paramMap[self.modelType] = value
        return self

    @since("1.5.0")
    def getModelType(self):
        """
        Gets the value of modelType or its default value.
        """
        return self.getOrDefault(self.modelType)


class NaiveBayesModel(JavaModel, JavaMLWritable, JavaMLReadable):
    """
    Model fitted by NaiveBayes.

    .. versionadded:: 1.5.0
    """

    @property
    @since("1.5.0")
    def pi(self):
        """
        log of class priors.
        """
        return self._call_java("pi")

    @property
    @since("1.5.0")
    def theta(self):
        """
        log of class conditional probabilities.
        """
        return self._call_java("theta")


@inherit_doc
class MultilayerPerceptronClassifier(JavaEstimator, HasFeaturesCol, HasLabelCol, HasPredictionCol,
                                     HasMaxIter, HasTol, HasSeed, JavaMLWritable, JavaMLReadable):
    """
    Classifier trainer based on the Multilayer Perceptron.
    Each layer has sigmoid activation function, output layer has softmax.
    Number of inputs has to be equal to the size of feature vectors.
    Number of outputs has to be equal to the total number of labels.

    >>> from pyspark.mllib.linalg import Vectors
    >>> df = sqlContext.createDataFrame([
    ...     (0.0, Vectors.dense([0.0, 0.0])),
    ...     (1.0, Vectors.dense([0.0, 1.0])),
    ...     (1.0, Vectors.dense([1.0, 0.0])),
    ...     (0.0, Vectors.dense([1.0, 1.0]))], ["label", "features"])
    >>> mlp = MultilayerPerceptronClassifier(maxIter=100, layers=[2, 5, 2], blockSize=1, seed=123)
    >>> model = mlp.fit(df)
    >>> model.layers
    [2, 5, 2]
    >>> model.weights.size
    27
    >>> testDF = sqlContext.createDataFrame([
    ...     (Vectors.dense([1.0, 0.0]),),
    ...     (Vectors.dense([0.0, 0.0]),)], ["features"])
    >>> model.transform(testDF).show()
    +---------+----------+
    | features|prediction|
    +---------+----------+
    |[1.0,0.0]|       1.0|
    |[0.0,0.0]|       0.0|
    +---------+----------+
    ...
    >>> mlp_path = temp_path + "/mlp"
    >>> mlp.save(mlp_path)
    >>> mlp2 = MultilayerPerceptronClassifier.load(mlp_path)
    >>> mlp2.getBlockSize()
    1
    >>> model_path = temp_path + "/mlp_model"
    >>> model.save(model_path)
    >>> model2 = MultilayerPerceptronClassificationModel.load(model_path)
    >>> model.layers == model2.layers
    True
    >>> model.weights == model2.weights
    True

    .. versionadded:: 1.6.0
    """

    layers = Param(Params._dummy(), "layers", "Sizes of layers from input layer to output layer " +
                   "E.g., Array(780, 100, 10) means 780 inputs, one hidden layer with 100 " +
                   "neurons and output layer of 10 neurons, default is [1, 1].",
                   typeConverter=TypeConverters.toListInt)
    blockSize = Param(Params._dummy(), "blockSize", "Block size for stacking input data in " +
                      "matrices. Data is stacked within partitions. If block size is more than " +
                      "remaining data in a partition then it is adjusted to the size of this " +
                      "data. Recommended size is between 10 and 1000, default is 128.",
                      typeConverter=TypeConverters.toInt)

    @keyword_only
    def __init__(self, featuresCol="features", labelCol="label", predictionCol="prediction",
                 maxIter=100, tol=1e-4, seed=None, layers=None, blockSize=128):
        """
        __init__(self, featuresCol="features", labelCol="label", predictionCol="prediction", \
                 maxIter=100, tol=1e-4, seed=None, layers=[1, 1], blockSize=128)
        """
        super(MultilayerPerceptronClassifier, self).__init__()
        self._java_obj = self._new_java_obj(
            "org.apache.spark.ml.classification.MultilayerPerceptronClassifier", self.uid)
        self._setDefault(maxIter=100, tol=1E-4, layers=[1, 1], blockSize=128)
        kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    @since("1.6.0")
    def setParams(self, featuresCol="features", labelCol="label", predictionCol="prediction",
                  maxIter=100, tol=1e-4, seed=None, layers=None, blockSize=128):
        """
        setParams(self, featuresCol="features", labelCol="label", predictionCol="prediction", \
                  maxIter=100, tol=1e-4, seed=None, layers=[1, 1], blockSize=128)
        Sets params for MultilayerPerceptronClassifier.
        """
        kwargs = self.setParams._input_kwargs
        if layers is None:
            return self._set(**kwargs).setLayers([1, 1])
        else:
            return self._set(**kwargs)

    def _create_model(self, java_model):
        return MultilayerPerceptronClassificationModel(java_model)

    @since("1.6.0")
    def setLayers(self, value):
        """
        Sets the value of :py:attr:`layers`.
        """
        self._paramMap[self.layers] = value
        return self

    @since("1.6.0")
    def getLayers(self):
        """
        Gets the value of layers or its default value.
        """
        return self.getOrDefault(self.layers)

    @since("1.6.0")
    def setBlockSize(self, value):
        """
        Sets the value of :py:attr:`blockSize`.
        """
        self._paramMap[self.blockSize] = value
        return self

    @since("1.6.0")
    def getBlockSize(self):
        """
        Gets the value of blockSize or its default value.
        """
        return self.getOrDefault(self.blockSize)


class MultilayerPerceptronClassificationModel(JavaModel, JavaMLWritable, JavaMLReadable):
    """
    Model fitted by MultilayerPerceptronClassifier.

    .. versionadded:: 1.6.0
    """

    @property
    @since("1.6.0")
    def layers(self):
        """
        array of layer sizes including input and output layers.
        """
        return self._call_java("javaLayers")

    @property
    @since("1.6.0")
    def weights(self):
        """
        vector of initial weights for the model that consists of the weights of layers.
        """
        return self._call_java("weights")


if __name__ == "__main__":
    import doctest
    import pyspark.ml.classification
    from pyspark.context import SparkContext
    from pyspark.sql import SQLContext
    globs = pyspark.ml.classification.__dict__.copy()
    # The small batch size here ensures that we see multiple batches,
    # even in these small test examples:
    sc = SparkContext("local[2]", "ml.classification tests")
    sqlContext = SQLContext(sc)
    globs['sc'] = sc
    globs['sqlContext'] = sqlContext
    import tempfile
    temp_path = tempfile.mkdtemp()
    globs['temp_path'] = temp_path
    try:
        (failure_count, test_count) = doctest.testmod(globs=globs, optionflags=doctest.ELLIPSIS)
        sc.stop()
    finally:
        from shutil import rmtree
        try:
            rmtree(temp_path)
        except OSError:
            pass
    if failure_count:
        exit(-1)
