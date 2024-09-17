# Databricks notebook source
#install.packages(c("factoextra", "tidyverse", "clue", "kableExtra", "igraph"))
#install.packages(c("knitr"))
install.packages("factoextra")

# COMMAND ----------

# Install and load necessary packages
install.packages(c("factoextra", "tidyverse", "clue", "kableExtra", "igraph"))
library(factoextra)
library(tidyverse)
library(clue)
library(kableExtra)
require("ggsci")
require(igraph)

# Load dataset
data("longley")

# Method to determine optimal number of clusters using elbow method
wcss <- vector()
for(i in 1:15) { 
  wcss[i] <- sum(kmeans(longley, i)$withinss) 
}

# Plot elbow method results
ggplot() + 
  geom_point(aes(x = 1:15, y = wcss), color = 'blue') +
  geom_line(aes(x = 1:15, y = wcss), color = 'blue') +
  ggtitle("Elbow Method") +
  xlab('Number of clusters (k)') +
  ylab('WCSS')

# Scale data
scaled_data <- scale(x = longley, center = TRUE, scale = TRUE)

# Calculate distance matrix
distance <- dist(x = scaled_data, method = "euclidean")

# Perform hierarchical clustering
clustering_hierarchical <- hclust(d = distance, method = "complete")

# Plot dendrogram
plot(x = clustering_hierarchical, main = "Hierarchical Clustering Dendrogram")

# Visualize dendrogram in different layouts
fviz_dend(clustering_hierarchical, cex = 0.8, lwd = 0.8, k = 3,
          rect = TRUE,
          k_colors = "jco",
          rect_border = "jco",
          rect_fill = TRUE)

fviz_dend(clustering_hierarchical, cex = 0.8, lwd = 0.8, k = 3,
          rect = TRUE,
          k_colors = "jco",
          rect_border = "jco",
          rect_fill = TRUE,
          horiz = TRUE)

fviz_dend(clustering_hierarchical, cex = 0.8, lwd = 0.8, k = 3,
          k_colors = "jco",
          type = "phylogenic")

fviz_dend(clustering_hierarchical, cex = 0.8, lwd = 0.8, k = 3,
          k_colors = "jco",
          type = "circular")

# Cut dendrogram to form clusters
clusters <- cutree(clustering_hierarchical, k = 3)

# Combine clusters with original data
clustered_data <- cbind(longley, clusters)

# Perform k-means clustering on original data
kmeans_clusters <- kmeans(longley, centers = 3, iter.max = 100, nstart = 16)

# Scale original data for k-means clustering
scaled_data_kmeans <- as.data.frame(apply(longley, MARGIN = 2, FUN = scale))

# Perform k-means clustering on scaled data
kmeans_clusters_scaled <- kmeans(scaled_data_kmeans, centers = 3, nstart = 16, iter.max = 500)

# Add cluster information to original and scaled data
scaled_data_kmeans$Cluster <- kmeans_clusters_scaled$cluster
longley$Cluster <- kmeans_clusters_scaled$cluster

# Convert cluster column to factor with descriptive labels
scaled_data_kmeans$Cluster <- factor(scaled_data_kmeans$Cluster, levels = c(1, 2, 3), labels = c('Group 1', 'Group 2', 'Group 3'))

# Plot GNP vs Employed
ggplot(scaled_data_kmeans, aes(y = GNP, x = Employed, shape = Cluster)) +
    geom_point() +
    stat_ellipse(aes(y = GNP, x = Employed, fill = Cluster), geom = 'polygon', alpha = 0.21, level = 0.95) +
    labs(title = "Relationship between GNP and Employment by Clusters",
         x = "Number of employees (Employed)",
         y = "Gross National Product (GNP)")

# Plot GNP.deflator vs Employed
ggplot(scaled_data_kmeans, aes(y = GNP.deflator, x = Employed, shape = Cluster)) +
    geom_point() +
    stat_ellipse(aes(y = GNP.deflator, x = Employed, fill = Cluster), geom = 'polygon', alpha = 0.21, level = 0.95) +
    labs(title = "Relationship between GNP Deflator and Employment by Clusters",
         x = "Number of employees (Employed)",
         y = "Gross National Product Deflator (GNP.deflator)")

# Display tables for GNP and GNP.deflator variables
table_GNP <- kable(longley[, c(2, 7)], format = 'html') %>%
    kable_styling(bootstrap_options = c('striped', 'hover'))

table_GNP_deflator <- kable(longley[, c(1, 7)], format = 'html') %>%
    kable_styling(bootstrap_options = c('striped', 'hover'))


# COMMAND ----------

library(ggplot2)
library(knitr)
library(kableExtra)

theme_set(theme_bw())

# Escalar datos
longley.scaled <- as.data.frame(apply(longley, MARGIN = 2, FUN = scale))

# Calcular clusters de los datos escalados
longley.scaled.clusters <- kmeans(longley.scaled, centers = 3, nstart = 16, iter.max = 500)

# Agregar columna "Cluster"
longley.scaled$Cluster <- longley.scaled.clusters$cluster

# Discretizar la columna "Cluster", transformar esa columna a factor
longley.scaled$Cluster <- factor(longley.scaled$Cluster, levels = c(1, 2, 3), labels = c('Grupo 1', 'Grupo 2', 'Grupo 3'))

# Graficar variables
grafico_GNP <- ggplot(longley.scaled, aes(y = GNP, x = Employed, shape = Cluster)) +
    geom_point() +
    stat_ellipse(aes(y = GNP, x = Employed, fill = Cluster), geom = 'polygon', alpha = 0.21, level = 0.95) +
    labs(title = "Relación entre el producto nacional bruto y el empleo por clusters",
         x = "Número de empleados (Employed)",
         y = "Producto nacional bruto (GNP)")

grafico_GNP

grafico_GNP_deflator <- ggplot(longley.scaled, aes(y = GNP.deflator, x = Employed, shape = Cluster)) +
    geom_point() +
    stat_ellipse(aes(y = GNP.deflator, x = Employed, fill = Cluster), geom = 'polygon', alpha = 0.21, level = 0.95) +
    labs(title = "Relación entre el deflactor del producto nacional bruto y el empleo por clusters",
         x = "Número de empleados (Employed)",
         y = "Deflactor del producto nacional bruto (GNP.deflator)")

grafico_GNP_deflator

# Creación de tablas por variable
tabla.GNP <- kable(longley[, c(2, 7)], format = 'html') %>%
    kable_styling(bootstrap_options = c('striped', 'hover'))

tabla.GNP

tabla.GNP.deflator <- kable(longley[, c(1, 7)], format = 'html') %>%
    kable_styling(bootstrap_options = c('striped', 'hover'))

tabla.GNP.deflator
