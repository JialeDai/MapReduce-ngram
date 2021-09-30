## running script
`hadoop jar big-data-assignment1-ngram-1.0-SNAPSHOT.jar edu.nyu.app.NGram -D mapred.textoutputformat.separator="," <input_path>  <output_path>` </br>
Notice: must specify the program split the key and value by "," using </br>-D mapred.textoutputformat.separator=","
## approach
- TokenizeMapper take the lines of the input, convert the whole line into list of unigrams, bigrams and trigrams and write to the WordCountReducer.
- WordCountReducer just do the simple word count job for the TokenizeMapper output. Then write the result to the tempDir temp1.
- NGramCountMapper read the file in temp1 as input file and output the line based on the word's type. For example: unigram, the-102
- NGramCountReducer receives the output of the NGramCountMapper and do the word count based on ngram type. For example, unigram-count,word1-count~word2-count...The result is output to tmep2.
- NGramComputerMapper receives the file in temp2 and compute the possibility: 
  - [bigram-10,the hat-1~in the-1~cat book-1~the best-1~hat is-1~cat in-1~best cat-1~the cat-1~is the-1~book ever-1] will be convert to the [the hat,1/10...]
- NGramCounterReducer receives the output of mapper and write to the output direction specified by the user.