# Databricks notebook source
# DBTITLE 1,Install MLFlow
# MAGIC %pip install mlflow

# COMMAND ----------

# DBTITLE 1,Download data
from sklearn import datasets
X, y = datasets.load_iris(return_X_y=True)

# COMMAND ----------

# DBTITLE 1,Create an SVM model
from sklearn import svm
model = svm.SVC(degree=2)
model.fit(X, y)
result = model.predict(X[0:100])

print(result)

# COMMAND ----------

# DBTITLE 1,Save model information into mlflow tracking
import mlflow

mlflow.set_experiment("/Users/nastasia.saby@gmail.com/demo/iris_experiments")

with mlflow.start_run() as last_run:
  mlflow.log_metric("accuracy", 0.7)
  mlflow.log_param("degree", 0.6)
  mlflow.sklearn.log_model(model, "Iris")

# COMMAND ----------


