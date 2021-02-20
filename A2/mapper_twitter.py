#!/usr/bin/env python3

import sys
import json
import re
import sys

output_dict = {} 
pronouns = ['han', 'hon', 'den', 'det', 'denna', 'denne','hen']

for line in sys.stdin:  #loop through each line from STDIN  
    line = line.strip()  #strip empty space
    if line:    #check that line is not empty
        j = json.loads(line)    #parse the incoming json to a dictionary 
        json_text = j['text']  #grab twitter-text

        if 'retweeted_status' not in j:  #check that tweet is not a retweet

            for pronoun in pronouns:  #loop through each pronom

                x = re.search('\\b'+pronoun+'\\b',json_text,flags = re.IGNORECASE) #search for pronom in tweet (case insensitive)
                if x != None:     #if pronom is found add to dictionary with value 1
                    output_dict[pronoun] = 1

            for i in output_dict:   #loop through dictionary
                print("%s\t%s" % (i, output_dict[i])) #write result to STDOUT


            output_dict = {} #reset dictionary for next tweet 