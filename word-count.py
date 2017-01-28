"""Basic Word Count
Robert Crowe

This is a basic word counting exercise using Apache Spark.  It 
loads a text file containing the complete works of William 
Shakespeare and does some counting, along with tests.

Written in Python 2

Complete works of William Shakespeare:
http://www.gutenberg.org/ebooks/100
"""
import sys
import os.path
from pyspark import SparkContext
from test_helper import Test

import re
def removePunctuation(text):
    """Removes punctuation, changes to lower case, and strips leading and trailing spaces.

    Note:
        Only spaces, letters, and numbers should be retained.  Other characters should should be
        eliminated (e.g. it's becomes its).  Leading and trailing spaces should be removed after
        punctuation is removed.

    Args:
        text (str): A string.

    Returns:
        str: The cleaned up string.
    """
    return re.sub('[^a-zA-Z 0-9]', '', text.lower().strip())

if __name__ == "__main__":
    master = "local"
    if len(sys.argv) == 2:
        master = sys.argv[1]
    
    sc = SparkContext(master, "WordCount")
    shakespeareRDD = (sc.textFile('./shakespeare.txt', 8)
                .map(removePunctuation))

    print '\n'.join(shakespeareRDD
                .zipWithIndex()  # to (line, lineNum)
                .map(lambda (l, num): '{0}: {1}'.format(num, l))  # to 'lineNum: line'
                .take(15))
    
    shakespeareWordsRDD = shakespeareRDD.flatMap(lambda s: s.split(' '))
    shakespeareWordCount = shakespeareWordsRDD.count()
    print shakespeareWordsRDD.top(5)
    print shakespeareWordCount

    Test.assertTrue(shakespeareWordCount == 927631 or shakespeareWordCount == 928908,
                'incorrect value for shakespeareWordCount')
    Test.assertEquals(shakespeareWordsRDD.top(5),
                  [u'zwaggerd', u'zounds', u'zounds', u'zounds', u'zounds'],
                  'incorrect value for shakespeareWordsRDD')
    
    shakeWordsRDD = shakespeareWordsRDD.filter(lambda s: s != '')
    shakeWordCount = shakeWordsRDD.count()
    print shakeWordCount

    Test.assertEquals(shakeWordCount, 882996, 'incorrect value for shakeWordCount')

    top15WordsAndCounts = wordCount(shakeWordsRDD).takeOrdered(15, lambda (w,c): -c)
    print '\n'.join(map(lambda (w, c): '{0}: {1}'.format(w, c), top15WordsAndCounts))

    Test.assertEquals(top15WordsAndCounts,
                  [(u'the', 27361), (u'and', 26028), (u'i', 20681), (u'to', 19150), (u'of', 17463),
                   (u'a', 14593), (u'you', 13615), (u'my', 12481), (u'in', 10956), (u'that', 10890),
                   (u'is', 9134), (u'not', 8497), (u'with', 7771), (u'me', 7769), (u'it', 7678)],
                  'incorrect value for top15WordsAndCounts')