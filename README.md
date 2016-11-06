ADA_Project
For the ADA?s Project we were inspired by a Project called ?Good city life? lead by Luca Maria Aiello, Daniele Quercia, two data scientists at Nokia Bell labs and Rossano Schifanella, an assistant professor in computer science at the University of Turin. The main goal of their project was the following:They built new maps of 2 cities (London and Boston) based on one human emotion: happiness. With these new maps, people are not only able to find the shortest path from A to B, but also to find a path that optimize happiness. For this purpose, they developed an algorithm that try to optimize both shortness and happiness. Following this algorithm, people can take a path a few minutes longer than the shortest path, but 10 times happier and more pleasant since there are less cars and more trees on the roadside, for instance.Our goal in this Project is to build a Happy Lausanne Map inspired by GoodCityLife work. 

Globally, there are two main lines in this project:I) Compute happinessThe core of the project is how to determine if a path is happier than an other ? For London and Boston, the GoodCityLife team divided the map in numerous cells (532 exactly), each cell having an area of 200mx200m. They created a graph in which each cell is a node linked by an edge to 8 neighbors. Those 8 neighbors correspond to the 8 adjacent cells. Moreover, each node has a specific weight evaluating it?s degree of happiness.Next comes the interesting part for the course: How to compute the degree of happiness of each node ?Thanks to our professor Michele Catasta, we will have access to a large dataset of Tweets and Instagram informations. Considering the fact that these informations are coming from social medias and therefore are not always telling the truth, we will have to make the best approximation of the degree of happiness of each node.Data descriptionOur first work will be to study our dataset. What different datas do we have in hand ? Then we will have to find a specific treatment for each different type of data. For instance, an image alone will be treated differently than a tweet without image.We will have a serious look on how the GoodCityLife team managed their Flick?r and Foursquare datas. We will separate our datas in two subparts:a)	Written datasIn order to extract the most relevant tweets we will use NLP methods. For instance, there exists a dictionary called the ?Linguistic Inquiry Word Count? (LIWC) that contain 2300 english words representing 80\% of the words used in our everyday life. Based on linguistics and psychological process, they associate each word to one category among 72 (positive emotions, negative emotions, swear words, anxiety, sadness, anger, etc.). This dictionary will probably be useful for us, but is there an equivalent in French and Italian ? Indeed, at this moment we do not know in which languages our data will be and if there are more than one. If it?s the case we will need to use different dictionaries according to the languages.
Ideally, we would like to treat English and French datas following the same guideline.b)	Images:- For Flick?r images without tag, the GoodCityLife team created computer-generated tags using computer vision. Is this a confident method ? We will discuss about it shortcuts. Flick?r has already a deep learning algorithm that recognize images using neural network techniques. Roughly, the image pass through series of layers, to make more and more calculus and finally determine in which tag category it belongs. 
II) Find the good pathAlgorithms have already been written, even before the GoodCityLife Project. In the case of the Happy maps, the complexity is to find a path that optimize both shortness and happiness. The main question will be : ?What is the best trade-off between shortness and happiness ? ?

III) Visualization

Our final step is to create a cartography of Lausanne with the paths founded thanks to our algorithm.

Deliverable:

1st Deliverable: Cleaned dataset and final computation of happiness degrees (6th December)

2nd Deliverable: Different paths computed (20th December)

3rd Deliverable: Final report with visualization (last January)

Timetable:

now - 6th December: Data gathering, mining and analysis 

6th December - checkpoint (mid-December) : Discussion about the results and « study » of the available algorithms 

mid-December - mid-January: Implement the most appropriate algorithm and visualization of the results  mid-January - last January: Final report and mini-symposium preparation