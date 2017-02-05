## Text Pre-processing

Before training sentiment analysis on the text, we need to prepare it by cleaning it.
We look only at the main text, that is the description of the instagram picture, but not at the hashtag list neither at the comment.
Our process is composed of 4 functions, that are successively called to obtain the text that is ready to be analyzed.

1) First clean

We remove @-mentions and #-hashtags that are present in the main text

2) Strip accent

3) Remove urls

4) Filter text

We remove all stop words and punctuation. We only keep words and emoticons (using regex) 
