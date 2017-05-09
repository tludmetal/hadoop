# hadoop

Author: Jose Luis Fernandez

*****************************************************************
WORDCOUNT
*****************************************************************


In the wordcount problem, were asked to give the ten more common words that appear after the word "for" in the entire corpus of Jane Austen's works. This words must be listed in descending order. 

That program analyzes the text and considers that one word is 
followed by another just if there is a whitespace or a line break 
between them. 

Valid examples of words occurring after "for" are:

... What a fine thing for our girls! ...

... "for Mrs. Long has just been here...	("for" preceded by a symbol)

"I see no occasion for that.        (the next word followed by symbol)


But not:

"It is more than I engage for, I assure you.". ("for" followed by symbol)

... and into it for
				(empty line)
half-an-hour--was ...


This analysis is case-insensitive.

The program consists of a MR job where the Mapper does the above
mentioned analysis and emits a key-value result being the key
the word that is preceded by "for" and the value an integer that
contains the number of repetitions of that occurrence. Because the default input record for a Mapper splits the input in lines, I wrote a paragraph record reader to be able to handle ccurrences were the word "for" appears at the end of a line.

The job defines a one only Reducer that retrieves all the above
mentioned pairs. Once all of them have being retrieved it emits the words that have the more occurrences after the word "for".

Because this problem is for a particular word, there is no need to follow neither the pairs nor the stripes approach, and there is also no need for normalizing the result.

To make the program a little bit more reusable the target word "for" can be passed as an argument. 

For this first exercise, I was new developing with Hadoop API. So I implemented all using  "mapred" API instead "mapreduce". I found this document very helpful: http://www.slideshare.net/sh1mmer/upgrading-to-the-new-map-reduce-api
Second exercise was developed with the new API, but mrunit does not work properly, so I needed to test/watch "on the fly" intermediate results using HUE in cloudera.

The result of this exersice is:

the
her
a
his
it
him
you
i
me
she


*****************************************************************
PAGERANK
*****************************************************************


For the pagerank problem were asked to provide a programs that calculates the ten users with the highest PageRank(descending order) with damping factor 0,85 in the Epinions Network. 

To do so, I designed a  program that runs a set of different MR jobs.

In the first the list of adjacency and the initial PageRank of each node -1- is produced.

The second task, to calculate the PageRank over 50 iterations, is done by 50 consecutive jobs. This jobs run the same Mapper and Reducer, being the input of one job the output of its predecessor. 

One last job is run to obtain the users with the highest PageRank. Because of the simplicity of this task, the identity Mapper can be used for it.

*The PageRank convergence can be determined in multiples ways.
For this problem, I decided establish my target convergence in the one that is obtained by applying the algorithm 50 times based on the adquired acknowledge in other exercise that I developed for SEWN: https://github.com/tludmetal/PageRank

The execution result for my program is:

 18
 118
 1719
 136
 790
 143
 40
 1619
 725
 849
