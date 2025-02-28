#!/bin/bash

echo "初始化"

# hdfs dfs -rm -r /LAB2/NaiveBayesModel
hdfs dfs -rm -r /LAB2/NaiveBayesValidationResult
# hdfs dfs -rm -r /LAB2/NaiveBayesTestResult

# echo "开始训练NaiveBayes模型"

# hadoop jar ~/LAB2/BigDataAnalysis.LAB2-4.0-FinalVersion.jar org.NaiveBayes.NaiveBayesJob hdfs:///LAB2/训练数据.txt hdfs:///LAB2/NaiveBayesModel train

# echo "NaiveBayes模型训练完成"

echo "开始验证NavieBayes模型"

hadoop jar ~/LAB2/BigDataAnalysis.LAB2-4.0-FinalVersion.jar org.NaiveBayes.NaiveBayesJob hdfs:///LAB2/验证数据.txt hdfs:///LAB2/NaiveBayesValidationResult validate

echo "NaiveBayes模型验证完成"

# echo "开始测试NaiveBayes模型"

# hadoop jar ~/LAB2/BigDataAnalysis.LAB2-4.0-FinalVersion.jar org.NaiveBayes.NaiveBayesJob hdfs:///LAB2/测试数据.txt hdfs:///LAB2/NaiveBayesTestResult test

# echo "NaiveBayes模型测试完成"

echo "正在获取结果"

hdfs dfs -ls /LAB2

# hdfs dfs -get /LAB2/NaiveBayesModel ~/LAB2/NaiveBayes
hdfs dfs -get /LAB2/NaiveBayesValidationResult ~/LAB2/NaiveBayes
# hdfs dfs -get /LAB2/NaiveBayesTestResult ~/LAB2/NaiveBayes