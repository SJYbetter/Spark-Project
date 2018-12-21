# CS6240 Project README

#### By HSIANGWEI CHAO, JIEYU SHENG



##### Data Sourse

#####  <https://aminer.org/citation>, <https://dblp.uni-trier.de/>



##### Data Description

This dataset is consisted of metadata of 1,511,035 papers, each record contains at most 7 attributes: paper title, authors, year, publication venue, index id, id of references, abstract. In this project, we will perform link analysis on the citation network, hence only index id and references will be used. The format of the original dataset is a text file of blocks. An example of a block is shown in the next bullet point. The year of papers ranges from 1936 to 2011. According to the aminer.org, the number of citation relationship is 2,084,019.

An Example of Raw Data:

> \#* --- paperTitle
>
> \#@ --- Authors
>
> \#t ---- Year
>
> \#c  --- publication venue
>
> \#index 00---- index id of this paper
>
> \#% ---- the id of references of this paper (there are multiple lines, with each indicating a reference)
>
> \#! --- Abstract



##### Goal

We would like to estimate, for each paper, what is the estimated prior probability that a directly cited paper is also directly cited by other directly cited paper. In other words, we would like to explore for each paper A, and a citation C, what is the probability that there exists a paper B that creates the following special triplet relationship:

![image-20181130210643304](/Users/jieyusheng/Library/Application Support/typora-user-images/image-20181130210643304.png)

For example, from the graph above, we know paper A cites paper B, C, D, E. There only exits one special triplet ABC, then we could conclude that for paper A, the probability is 0.25. In this dataset, we are aimed to calculate the overall prior probability for each paper. Through analysis the relationship between year and cited prior probability to mining the value.