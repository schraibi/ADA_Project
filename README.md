# ADA_Project
## AbstractFor the ADA's project we were inspired by a project called "GoodCityLife" lead by Luca Maria Aiello, Daniele Quercia, two data scientists of Nokia Bell labs and Rossano Schifanella, an assistant professor in computer science at the University of Turin. In this Project, our goal is to build a "Happy Swiss Map" based on an instagram dataset. In this picture, you can have an idea of where the instagram we had came from.![alt tag](https://github.com/schraibi/ADA_Project/blob/master/visualization/pic/city_density.png) An interactive map of sentiment scores is [here] (https://rawgit.com/schraibi/ADA_Project/master/canton_sentiments.html)Thanks to our professor Michele Catasta, we will have access to a large dataset of Instagram informations (around 10 millions). We will focus in this project on the text part of each instagram.

A visualized resume of the project can be seen [there] (http://nbviewer.jupyter.org/github/schraibi/ADA_Project/blob/master/Presentation.ipynb) You find the same content in Presentation.ipynb## What kind of data do we have ?Our first work will be to study our instagram dataset. We have in hand a dataset of very noisy datas and we have to clean them. This is the text pre-processing part, which code can be find in directory text_preprocessing

## Machine learning pipeline

We follow a standard pipeline using Naive Bayes multiclassification method that trains on 20% of the dataset and predict the 80% others. More details on the pipeline and accuracy of the method in ML_part directory.## Geo-localizationAny localization information was given in our dataset, then we decided to find it by ourselves. The algorithm is based on the tag list of every instagram. To have a look at the alto, the code can be found in localization
## VisualizationOur final step is to create a cartography of Switzerland that we can visualize. You can see all the result in the file Presentation.ipynb, and for more details about how the visualization was done, all the necessary is in the visualization directory.

## Conclusion

Finally, to see all the pipeline in one script, go to the directory one_script. All the functions used in in_one_script.ipynb are concatenated in functions.py. Before all, this project's aim was to introduce myself to big data calculus and practicing Spark for parallelize algorithms. Even if the final results are not relevant, the only fact that all the steps above mentioned have been run on almost 10-millions instagrams is a success.

Thank you to the all teaching team, Michele Catasta and especially Tiziano Piccardi who was very talkative on slack !!