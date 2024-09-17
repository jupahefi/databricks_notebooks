# Databricks notebook source
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import StandardScaler
from pyspark.sql import DataFrame, Row
from pyspark.sql.functions import col
import matplotlib.pyplot as plt
import numpy as np
from pyspark.ml.evaluation import ClusteringEvaluator
from typing import List, Tuple

def load_data(file_path: str) -> DataFrame:
    """
    Load dataset from a CSV file.
    
    Args:
    - file_path (str): Path to the CSV file.
    
    Returns:
    - DataFrame: Loaded dataset.
    """
    try:
        data: DataFrame = spark.read.csv(file_path, header=True, inferSchema=True, sep=';')
        return data
    except Exception as e:
        print(f"Error loading data: {str(e)}")
        return None

def calculate_metrics(data_scaled: DataFrame, k_values: List[int]) -> Tuple[List[float], List[float]]:
    """
    Calculate WCSS and silhouette score for different values of k.
    
    Args:
    - data_scaled (DataFrame): DataFrame with scaled features.
    - k_values (List[int]): List of k values for clustering.
    
    Returns:
    - Tuple[List[float], List[float]]: WCSS and silhouette scores.
    """
    wcss: List[float] = []
    silhouette_scores: List[float] = []
    try:
        for i in k_values:
            kmeans: KMeans = KMeans(k=i, seed=0)
            model = kmeans.fit(data_scaled)
            summary = model.summary
            wcss.append(summary.trainingCost)

            # Calculate silhouette score
            predictions = model.transform(data_scaled)
            evaluator = ClusteringEvaluator()
            silhouette_score: float = evaluator.evaluate(predictions)
            silhouette_scores.append(silhouette_score)

            print(f"Para k={i}, WCSS={summary.trainingCost}, Puntuación de la silueta={silhouette_score}")

        return wcss, silhouette_scores
    except Exception as e:
        print(f"Error calculating metrics: {str(e)}")
        return [], []

def find_elbow_point(wcss: List[float]) -> int:
    """
    Find the elbow point in the WCSS curve.
    
    Args:
    - wcss (List[float]): List of WCSS values.
    
    Returns:
    - int: Optimal number of clusters.
    """
    try:
        x1, y1 = 2, wcss[0]
        x2, y2 = 16, wcss[-1]
        distances: List[float] = []
        for i in range(len(wcss)):
            x0 = i + 2
            y0 = wcss[i]
            numerator = abs((y2 - y1) * x0 - (x2 - x1) * y0 + x2*y1 - y2*x1)
            denominator = np.sqrt((y2 - y1)**2 + (x2 - x1)**2)
            distances.append(numerator / denominator)
        return distances.index(max(distances)) + 2
    except Exception as e:
        print(f"Error finding elbow point: {str(e)}")
        return -1

def plot_metrics(k_values: List[int], wcss: List[float], silhouette_scores: List[float]) -> None:
    """
    Plot WCSS and silhouette score.
    
    Args:
    - k_values (List[int]): List of k values.
    - wcss (List[float]): List of WCSS values.
    - silhouette_scores (List[float]): List of silhouette scores.
    """
    try:
        wcss_rounded: List[float] = [round(value, 1) for value in wcss]
        silhouette_scores_rounded: List[float] = [round(value, 2) for value in silhouette_scores]

        plt.figure(figsize=(12, 12))

        # Plot WCSS
        plt.subplot(2, 1, 1)
        plt.plot(k_values, wcss_rounded, marker='o')
        plt.xlabel('Número de clusters (k)')
        plt.ylabel('WCSS')
        plt.title('WCSS para diferentes valores de k')
        plt.xticks(np.arange(2, 17, step=1))
        plt.yticks(np.round(np.linspace(min(wcss_rounded), max(wcss_rounded), 5), 1))

        for i, txt in enumerate(wcss_rounded):
            plt.text(k_values[i], wcss_rounded[i], str(txt), ha='center', va='bottom')

        # Plot silhouette score
        plt.subplot(2, 1, 2)
        plt.plot(k_values, silhouette_scores_rounded, marker='o')
        plt.xlabel('Número de clusters (k)')
        plt.ylabel('Puntuación de la silueta')
        plt.title('Puntuación de la silueta para diferentes valores de k')
        plt.xticks(np.arange(2, 17, step=1))
        plt.yticks(np.round(np.linspace(min(silhouette_scores_rounded), max(silhouette_scores_rounded), 5), 1))

        for i, txt in enumerate(silhouette_scores_rounded):
            plt.text(k_values[i], silhouette_scores_rounded[i], str(txt), ha='center', va='bottom')

        plt.tight_layout()
        plt.show()
    except Exception as e:
        print(f"Error plotting metrics: {str(e)}")

def main() -> None:
    try:
        # Load dataset
        file_path: str = "abfss://raw@cs2100320032141b0ad.dfs.core.windows.net/data_science/Libro1.csv"
        data: DataFrame = load_data(file_path)
        if data is None:
            return

        # Assemble features
        assembler: VectorAssembler = VectorAssembler(inputCols=data.columns[1:], outputCol="features")
        data_assembled: DataFrame = assembler.transform(data)

        # Scale features
        scaler: StandardScaler = StandardScaler(inputCol="features", outputCol="scaled_features")
        scaler_model = scaler.fit(data_assembled)
        data_scaled: DataFrame = scaler_model.transform(data_assembled)

        # Calculate metrics
        k_values: List[int] = list(range(2, 17))
        wcss, silhouette_scores = calculate_metrics(data_scaled, k_values)

        # Find elbow point
        optimal_k: int = find_elbow_point(wcss)
        print(f'El número óptimo de clusters es: {optimal_k}')

        # Plot metrics
        plot_metrics(k_values, wcss, silhouette_scores)

        # Perform clustering with optimal number of clusters
        if optimal_k != -1:
            kmeans: KMeans = KMeans(k=optimal_k, seed=0)
            model = kmeans.fit(data_scaled)
            clusters: DataFrame = model.transform(data_scaled)

            # Show clustering results
            display(clusters)
            return clusters
    except Exception as e:
        print(f"Error in main function: {str(e)}")
        return spark.createDataFrame([], "string")

clusters = main()

# COMMAND ----------

visualization_data = clusters.drop("features", "scaled_features").withColumnRenamed("prediction", "cluster")

display(visualization_data)


# COMMAND ----------

# Evaluate clustering by computing Silhouette score
evaluator = ClusteringEvaluator()

silhouette_score = evaluator.evaluate(clusters)
print(f"Silhouette Score: {silhouette_score}")

# Crear un DataFrame de Spark con el Silhouette Score
silhouette_row = Row("metric", "value")
silhouette_df = spark.createDataFrame([silhouette_row("Silhouette Score", round(silhouette_score, 2))])

# Mostrar el DataFrame con el Silhouette Score
display(silhouette_df)
