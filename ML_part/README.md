## Machine learning part

We use a Naive bayes classifier to predict sentiment values. 
We also use the Term Frequency-Inverse Document Frequency (TF-IDF) as a text pre-processing step that is based on each word term frequency in the corpus. The pipeline is composed by the following steps:

1) We begin with a set of instagram text
2) Each instagram text is split into words using the spark tokenizer
3) For each text (list of words), we hash the sentence into a feature vector (HashingTF)
4) We apply IDF spark method to rescale the feature vectors
5) Our feature vectors are ready to be learned by the Naive bayes classifier 

## Accuracy of our model

The F1-score (which take in account the precision and the recall score ) for each labels are 0.876 for positive, 0.585 for neutral and 0.592 for negative prediction.
We have a look now at the results more in details.

## Confusion matrix

[ 293970.,   43401.,   12564.]
[  20855.,   50214.,    5337.]
[   6086.,    1484.,   18547.]

The classifier has acceptable results, but it has from far better results for positive prediction.
71 % of the time it well classifies bad instagrams and 65 % of neutral instagrams predictions are neutral. The results could be improved by having a better text preparation. Using stem words could help.