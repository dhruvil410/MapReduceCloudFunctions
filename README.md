# MapReduceCloudFunctions


## Introduction
- Project is about implementing parallel map-reduce from scratch to search books from the 
Gutenberg Project. In this, all required tasks are deployed on Google Cloud functions. 
- Project Gutenberg contains more than 70,000 documents for which U.S. copyright has expired. All 
books are freely available for reading in many different formats.
- This project has two main functionalities. The first is to calculate the TF-IDF index using a map reduce design pattern for given documents. The second is to process the user query also using a map-reduce design pattern and search the most relevant documents based on cosine similarity of 
TF-IDF scores from already processed documents.